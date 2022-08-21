package service

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"e_wallet/api"
	"e_wallet/model"
	types "e_wallet/type"
	"e_wallet/utils"
	csv "e_wallet/utils"

	"gorm.io/gorm"
)

var (
	hourFormat    = "2006-01-02 15:04:05"
	numOfWorker   = 1
	intDateFormat = "20060102"
)

type ReportService struct {
	DB *gorm.DB
	api.ReportAPI
	FilePath       string
	PrefixFileName string
}

func NewReportService(db *gorm.DB, filePath, prefixFileName string) *ReportService {
	return &ReportService{
		DB:             db,
		FilePath:       filePath,
		PrefixFileName: prefixFileName,
	}
}

func (rs *ReportService) ExportTransaction(w http.ResponseWriter, r *http.Request) {

	// set path for destination file
	var filePath = rs.FilePath

	// init postgres connection
	client := rs.DB

	startProcess := time.Now()

	fromStr := r.URL.Query().Get("from")

	toStr := r.URL.Query().Get("to")

	// 2022-01-01 00:00:00 is marked as the first time of system
	var fromTimeQuery = time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local)
	var toTimeQuery = time.Now()
	var err error

	// if user pass a date -> use it
	if fromStr != "" {
		from := formatTimeString(fromStr)
		fromTimeQuery, err = FormatDate(from)
		if err != nil {
			utils.Response(w, types.ErrorResponse{
				Code:    500,
				Message: "from's format is invalid !",
				Error:   err.Error(),
			})
			return
		}
	}

	// if user pass a date -> use it
	if toStr != "" {
		to := formatTimeString(toStr)
		toTimeQuery, err = FormatDate(to)
		if err != nil {
			utils.Response(w, types.ErrorResponse{
				Code:    500,
				Message: "to's format is invalid !",
				Error:   err.Error(),
			})
			return
		}
	}

	// fromTime must be less or equal than toTime
	if fromTimeQuery.After(toTimeQuery) {
		utils.Response(w, types.ErrorResponse{
			Code:    500,
			Message: "from must be less than to !",
			Error:   err.Error(),
		})
		return
	}

	// convert date time to format yyyy-mm-dd hh:mm:ss
	fromTimeQueryStr := fromTimeQuery.Format(hourFormat)
	// convert date time to format yyyy-mm-dd hh:mm:ss
	toTimeQueryStr := toTimeQuery.Format(hourFormat)

	// format filename
	fileName := fmt.Sprintf("%s_%s_to_%s", rs.PrefixFileName, strings.ReplaceAll(fromTimeQueryStr, " ", "_"), strings.ReplaceAll(toTimeQueryStr, " ", "_"))
	filePath += fileName

	// Init writer for writing csv file
	writer, err := csv.NewCsvWriter(filePath)
	if err != nil {
		utils.Response(w, types.ErrorResponse{
			Code:    500,
			Message: "Can't init file !",
			Error:   err.Error(),
		})
		return
	}

	// Query transactions from db then write to file
	totalTransactions, _, err := ProcessTransactions(client, fromTimeQuery, toTimeQuery, writer)
	if err != nil {
		utils.Response(w, types.ErrorResponse{
			Code:    500,
			Message: "Error when process transactions !",
			Error:   err.Error(),
		})
		return
	}

	writer.Flush()

	log.Printf("Job done : %v\n", time.Since(startProcess))

	var response types.SucessResponse
	if totalTransactions == 0 {
		response = types.SucessResponse{
			Code:    200,
			Message: "There're no transactions during this period !",
		}
	} else {
		response = types.SucessResponse{
			Code:    200,
			Message: fmt.Sprintf("Exported %d transactions with into %s", totalTransactions, filePath),
		}
	}

	utils.Response(w, response)

}

func formatTimeString(t string) string {

	year := t[0:4]
	month := t[4:6]
	day := t[6:8]
	hour := t[8:10]
	minute := t[10:12]
	second := t[12:]

	return fmt.Sprintf("%s-%s-%s %s:%s:%s", year, month, day, hour, minute, second)
}

// Round to day unit : 2022-01-02 13:42:31 -> 2022-01-02 00:00:00
func Round(t time.Time) time.Time {
	return t.Truncate(time.Hour * 24)
}

func FormatDate(processTime string) (time.Time, error) {
	return time.Parse(hourFormat, processTime)
}

// Format time to int : 2022-06-20 -> 20220620
func FormatIntTime(t time.Time) (int, error) {
	intDateTime, err := strconv.Atoi(t.Format(intDateFormat))
	if err != nil {
		return 0, fmt.Errorf("Can't parse time to int with format yyyymmdd !")
	}
	return intDateTime, nil
}

func ProcessTransactions(client *gorm.DB, fromTimeQuery, toTimeQuery time.Time, writer *csv.CsvWriter) (int, int, error) {

	// total amount of all transactions
	var totalAmount model.Counter

	// total transactions
	var totalTransactions model.Counter

	// channel which receive transactions between goroutines
	var txsChan = make(chan []model.TransactionFileFormat)
	var errorChan = make(chan error)

	// Content of the first line
	writer.Write([]string{"HD", "TRACE", "TXN_HASH", "FROM", "TO", "TRANSTYPE", "AMOUNT", "STATUS", "TXN_TIME"})

	var wg sync.WaitGroup

	// init session using Prepare Statement
	tx := client.Session(&gorm.Session{PrepareStmt: true})

	// Init worker for concurrency processing
	for i := 0; i < numOfWorker; i++ {
		go func(i int) {
			// get transactions from channel to process
			for transactions := range txsChan {
				fmt.Printf("Worker %d is processing !\n", i)
				err := ExportToFile(transactions, writer, &totalAmount)
				if err != nil {
					errorChan <- err
				}

				wg.Done()
			}
		}(i)
	}

	var timeQuery = Round(fromTimeQuery)
	var timeQueryCondition string

	// Query each date in range [fromTimeQuery, toTimeQuery]
	for timeQuery.Before(Round(toTimeQuery)) || timeQuery.Equal(Round(toTimeQuery)) {
		var withCondition = false

		// if timeQuery and fromTimeQuery are both the same date -> need pass a detail condition
		if timeQuery.Equal(Round(fromTimeQuery)) {
			withCondition = true
			timeQueryCondition = "system_date >= ?"
			timeQuery = fromTimeQuery
			// if timeQuery and toTimeQuery are both the same date -> need pass a detail condition
		} else if timeQuery.Equal(Round(toTimeQuery)) {
			withCondition = true
			timeQueryCondition = "system_date < ?"
			timeQuery = toTimeQuery
		}

		intTimeQuery, err := FormatIntTime(timeQuery)
		if err != nil {
			return 0, 0, err
		}

		tableName := fmt.Sprintf("transaction_%d", intTimeQuery)

		// Query by shard table (sharding by date : transaction_20220420,  transaction_20220421, ...)
		err = QueryTransactions(&wg, tx, tableName, txsChan, &totalTransactions, timeQuery, timeQueryCondition, withCondition)
		if err != nil {
			errorChan <- err
		}

		timeQuery = Round(timeQuery).AddDate(0, 0, 1)

	}

	// log.Printf("Queried %d transactions from %s to %s : %v \n", totalTransactions.total, fromTime, toTime, time.Since(start))

	// wait for all goroutines to complete
	wg.Wait()

	close(txsChan)
	close(errorChan)

	// Content of the last line
	writer.Write([]string{"FT", fmt.Sprint(totalTransactions.Total), fmt.Sprint(totalTransactions.Total), "0", fmt.Sprint(totalAmount.Total)})

	err := <-errorChan
	if err != nil {
		return totalTransactions.Total, totalAmount.Total, err
	}

	return totalTransactions.Total, totalAmount.Total, nil

}

func QueryTransactions(wg *sync.WaitGroup, client *gorm.DB, tableName string, txsChan chan []model.TransactionFileFormat,
	totalTransactions *model.Counter, timeQuery time.Time, timeQueryCondition string, withCondition bool) error {
	// start := time.Now()
	var transactions []model.TransactionFileFormat

	if withCondition {
		err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Where(timeQueryCondition, timeQuery).Find(&transactions).Error
		if err != nil && !strings.Contains(err.Error(), "does not exist") {
			log.Println("Warning :", err)
			return fmt.Errorf("There's at least 1 error when querying from DB : %v", err)
		}

		// fmt.Println(timeQueryCondition, timeQuery)
	} else {
		err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Find(&transactions).Error
		if err != nil && !strings.Contains(err.Error(), "does not exist") {
			log.Println("Warning :", err)
			return fmt.Errorf("There's at least 1 error when querying from DB : %v", err)
		}
	}

	if len(transactions) > 0 {
		wg.Add(1)
		// send queried transactions to channel
		txsChan <- transactions

		// Count total transactions
		totalTransactions.Lock()
		totalTransactions.Total += len(transactions)
		totalTransactions.Unlock()
	}
	return nil

	// fmt.Println(time.Since(start))

}

func ExportToFile(transactions []model.TransactionFileFormat, writer *csv.CsvWriter, totalAmount *model.Counter) error {

	// var rows = [][]string{}
	for _, transaction := range transactions {

		row := []string{"CT", transaction.TraceNo, transaction.Txhash, transaction.SenderId, transaction.ReceiverId,
			transaction.Action, fmt.Sprint(transaction.Amount), "00", transaction.SystemDate.String()}

		// calculate total amount of all transactions
		totalAmount.Lock()
		totalAmount.Total += transaction.Amount
		totalAmount.Unlock()

		// rows = append(rows, row)
		err := writer.Write(row)
		if err != nil {
			return fmt.Errorf("There's at least 1 error when writing to file : %v", err)
		}

	}

	return nil

}
