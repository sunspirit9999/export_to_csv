package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"flag"

	"e_wallet/db"
	types "e_wallet/type"
	csv "e_wallet/utils"

	"gorm.io/gorm"
)

var (
	hourFormat     = "2006-01-02 15:04:05"
	prefixFileName = "Transaction"
	numOfWorker    = 10
	intDateFormat  = "20060102"
)

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
		return 0, err
	}
	return intDateTime, nil
}
func main() {
	from := flag.String("from", "", "date with format yyyy-dd-mm")
	to := flag.String("to", "", "date with format yyyy-dd-mm")
	path := flag.String("file_path", "/home/sunspirit/Documents/", "relative path with format string")

	flag.Parse()

	// set path for destination file
	var filePath = *path

	// init postgres connection
	client := db.InitDB()

	startProcess := time.Now()

	// 2022-01-01 00:00:00 is marked as the first time of system
	var fromTimeQuery = time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local)
	var toTimeQuery = time.Now()
	var err error

	// if user pass a date -> use it
	if *from != "" {
		fromTimeQuery, err = FormatDate(*from)
		if err != nil {
			panic(err)
		}
	}

	// if user pass a date -> use it
	if *to != "" {
		toTimeQuery, err = FormatDate(*to)
		if err != nil {
			panic(err)
		}
	}

	// fromTime must be less or equal than toTime
	if fromTimeQuery.After(toTimeQuery) {
		panic("from must be less than to !")
	}

	// convert date time to format yyyy-mm-dd hh:mm:ss
	fromTimeQueryStr := fromTimeQuery.Format(hourFormat)
	// convert date time to format yyyy-mm-dd hh:mm:ss
	toTimeQueryStr := toTimeQuery.Format(hourFormat)

	// format filename
	fileName := fmt.Sprintf("%s_%s_to_%s", prefixFileName, strings.ReplaceAll(fromTimeQueryStr, " ", "_"), strings.ReplaceAll(toTimeQueryStr, " ", "_"))
	filePath += fileName

	// Init writer for writing csv file
	writer, err := csv.NewCsvWriter(filePath)
	if err != nil {
		panic(err)
	}

	// Query transactions from db then write to file
	ProcessTransactions(client, fromTimeQuery, toTimeQuery, writer)

	writer.Flush()

	log.Printf("Job done : %v\n", time.Since(startProcess))

}

func ProcessTransactions(client *gorm.DB, fromTimeQuery, toTimeQuery time.Time, writer *csv.CsvWriter) {

	// total amount of all transactions
	var totalAmount types.Counter

	// total transactions
	var totalTransactions types.Counter

	// channel which receive transactions between goroutines
	var txsChan = make(chan []types.Transaction)

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
				ExportToFile(transactions, writer, &totalAmount)
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
			panic(err)
		}

		tableName := fmt.Sprintf("transaction_%d", intTimeQuery)

		// Query by shard table (sharding by date : transaction_20220420,  transaction_20220421, ...)
		QueryTransactions(&wg, tx, tableName, txsChan, &totalTransactions, timeQuery, timeQueryCondition, withCondition)

		timeQuery = Round(timeQuery).AddDate(0, 0, 1)

	}

	// log.Printf("Queried %d transactions from %s to %s : %v \n", totalTransactions.total, fromTime, toTime, time.Since(start))

	// wait for all goroutines to complete
	wg.Wait()

	close(txsChan)

	// Content of the last line
	writer.Write([]string{"FT", fmt.Sprint(totalTransactions.Total), fmt.Sprint(totalTransactions.Total), "0", fmt.Sprint(totalAmount.Total)})

}

func QueryTransactions(wg *sync.WaitGroup, client *gorm.DB, tableName string, txsChan chan []types.Transaction,
	totalTransactions *types.Counter, timeQuery time.Time, timeQueryCondition string, withCondition bool) {
	// start := time.Now()
	var transactions []types.Transaction

	if withCondition {
		err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Where(timeQueryCondition, timeQuery).Find(&transactions).Error
		if err != nil && !strings.Contains(err.Error(), "does not exist") {
			log.Println("Warning :", err)
		}

		// fmt.Println(timeQueryCondition, timeQuery)
	} else {
		err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Find(&transactions).Error
		if err != nil && !strings.Contains(err.Error(), "does not exist") {
			log.Println("Warning :", err)
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

	// fmt.Println(time.Since(start))

}

func ExportToFile(transactions []types.Transaction, writer *csv.CsvWriter, totalAmount *types.Counter) {

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
			log.Println("Error when writing to file :", err)
		}

	}

}
