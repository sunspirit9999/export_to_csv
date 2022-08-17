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

type count struct {
	sync.Mutex
	total int
}

func FormatDate(processTime string) (time.Time, error) {
	return time.Parse(hourFormat, processTime)
}

func main() {
	from := flag.String("from", "", "date with format yyyy-dd-mm")
	to := flag.String("to", "", "date with format yyyy-dd-mm")
	path := flag.String("file_path", "/home/sunspirit/Documents", "relative path with format string")

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

func ProcessTransactions(client *gorm.DB, fromTime, toTime time.Time, writer *csv.CsvWriter) {

	// total amount of all transactions
	var totalAmount count

	// total transactions
	var totalTransactions count

	// channel which receive transactions between goroutines
	var txsChan = make(chan []types.Transaction)

	// Content of the first line
	writer.Write([]string{"HD", "TRACE", "TXN_HASH", "FROM", "TO", "TRANSTYPE", "AMOUNT", "STATUS", "TXN_TIME"})

	var wg sync.WaitGroup

	// Init worker for concurrency processing
	for i := 0; i < numOfWorker; i++ {
		go func() {
			// get transactions from channel to process
			for transactions := range txsChan {
				// fmt.Printf("Worker %d is processing !\n", i)
				ExportToFile(transactions, writer, &totalAmount)
				wg.Done()
			}
		}()
	}

	// Query by shard db (sharding by date)
	for fromTime.Before(toTime) {
		intDateTime, err := strconv.Atoi(fromTime.Format(intDateFormat))
		if err != nil {
			panic(err)
		}

		tableName := fmt.Sprintf("transaction_%d", intDateTime)

		wg.Add(1)
		QueryTransactions(client, tableName, txsChan, &totalTransactions)

		fromTime = fromTime.AddDate(0, 0, 1)
	}

	// log.Printf("Queried %d transactions from %s to %s : %v \n", totalTransactions.total, fromTime, toTime, time.Since(start))

	// wait for all goroutines to complete
	wg.Wait()

	close(txsChan)

	// Content of the last line
	writer.Write([]string{"FT", fmt.Sprint(totalTransactions.total), fmt.Sprint(totalTransactions.total), "0", fmt.Sprint(totalAmount.total)})

}

func QueryTransactions(client *gorm.DB, tableName string, txsChan chan []types.Transaction, totalTransactions *count) {

	var transactions []types.Transaction

	err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Find(&transactions).Error
	if err != nil {
		log.Println("Warning :", err)
	}

	// send queried transactions to channel
	txsChan <- transactions

	// Count total transactions
	totalTransactions.Lock()
	totalTransactions.total += len(transactions)
	totalTransactions.Unlock()

}

func ExportToFile(transactions []types.Transaction, writer *csv.CsvWriter, totalAmount *count) {

	for _, transaction := range transactions {

		row := []string{"CT", transaction.TraceNo, transaction.Txhash, transaction.SenderId, transaction.ReceiverId,
			transaction.Action, fmt.Sprint(transaction.Amount), "00", transaction.SystemDate.String()}

		// calculate total amount of all transactions
		totalAmount.Lock()
		totalAmount.total += transaction.Amount
		totalAmount.Unlock()

		err := writer.Write(row)
		if err != nil {
			log.Println("Error when writing to file :", err)
		}
	}
}