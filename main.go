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

	"e_wallet/config"

	"gorm.io/gorm"
)

var (
	hourFormat         = "2006-01-02 15:04:05"
	intDateFormat      = "20060102"
	validLengthOfInput = 14 // len("yyyymmddhhmmss")
	totalDailyBlocks   = 86400
	dateFormat         = "2006-01-02"
)

func main() {
	startProcess := time.Now()

	// from := flag.String("from", "", "date with format yyyy-dd-mm")
	// to := flag.String("to", "", "date with format yyyy-dd-mm")

	date := flag.String("date", "", "date with format yyyy-dd-mm")

	fromBegin := flag.Bool("from_begin", false, "process transactions all time")

	flag.Parse()

	config, err := config.Load()
	if err != nil {
		log.Fatal("Failed to load config !")
	}

	pgConfig := config.GetStringMapString("postgres")

	schemaName := config.GetString("table.schemaName")

	// init postgres connection
	client := db.InitDB(pgConfig)

	// set path for destination file
	filePath := config.GetString("file.path")

	// set path for destination file
	partNumber := config.GetInt("file.partNumber")

	extension := config.GetString("file.extension")

	// default
	var dateTimeQuery = time.Now()

	// if user pass a date -> use it
	if *date != "" {
		dateTimeQuery, err = FormatDate2(*date)

		if err != nil {
			log.Println("date's format is invalid !")
			return
		}
	}

	// Query transactions from db then write to file
	totalTransactions, totalAmounts, err := ProcessTransactions(client, dateTimeQuery, filePath, schemaName, partNumber, extension, *fromBegin)
	if err != nil {
		log.Println(err)
	}

	log.Printf("Wrote %d transactions with total amount = %d to file !", totalTransactions, totalAmounts)

	log.Printf("Job done : %v\n", time.Since(startProcess))

}

func ProcessTransactions(client *gorm.DB, dateTimeQuery time.Time, filePath string, schemaName string, partNumber int,
	extension string, fromBegin bool) (int64, int64, error) {

	// total amount of all transactions
	var totalAmount types.Counter

	// total transactions
	var totalTransactions types.Counter

	var timeQuery = Round(dateTimeQuery)

	intTimeQuery, err := FormatIntTime(timeQuery.AddDate(0, 0, -1))
	if err != nil {
		return 0, 0, err
	}

	fileSubFix, err := FormatIntTime(timeQuery)
	if err != nil {
		return 0, 0, err
	}

	tableName := fmt.Sprintf("transaction_%d", intTimeQuery)
	blockTableName := fmt.Sprintf("blocks_%d", intTimeQuery)

	var tableList []string

	if fromBegin {
		err := client.Raw(
			`SELECT tablename FROM pg_tables
				WHERE  schemaname = ?
				AND    tablename like 'transaction%'`,
			schemaName).Scan(&tableList).Error

		if err != nil {
			log.Println("There's no tables found in DB")
			return 0, 0, err
		}
		log.Printf("Export transactions from begin to %s", dateTimeQuery)

	} else {
		tableList = append(tableList, tableName)
		log.Printf("Export transactions on %s", dateTimeQuery)
	}

	for _, tableName := range tableList {
		var lastBlockNum int

		err := client.Raw("select MAX(blocknum) from " + blockTableName).Scan(&lastBlockNum).Error
		if err != nil {
			log.Printf("Cant query the last block in table %s : %+v\n", tableName, err)
			return 0, 0, err
		}

		// the first blocknum in table
		var firstBlockNum = (lastBlockNum - totalDailyBlocks) - 1

		// total files will be created, each file has a number of transactions in 1000 blocks
		var totalParts = (totalDailyBlocks / partNumber) + 1

		var wg sync.WaitGroup
		var processLastBlock = false

		for partId := 1; partId <= totalParts; partId++ {

			fromBlock := firstBlockNum + (partId-1)*partNumber
			toBlock := firstBlockNum + partId*partNumber

			// the last file
			if partId == totalParts {
				toBlock = lastBlockNum
				processLastBlock = true
			}

			wg.Add(1)

			fmt.Println(fromBlock, toBlock)

			go func(partId int) {
				// Query by shard table (sharding by date : transaction_20220420,  transaction_20220421, ...)
				transactions := QueryTransactions(&wg, client, tableName, fromBlock, toBlock, processLastBlock)

				if len(transactions) > 0 {
					fileName := fmt.Sprintf("%d_%d.%s", fileSubFix, partId, extension)
					url := filePath + fileName

					total, err := ExportToFile(transactions, url)
					if err != nil {
						log.Printf("Error when exporting to file : %+v\n", err)
					}

					totalAmount.Lock()
					totalAmount.Total += total
					totalAmount.Unlock()

					totalTransactions.Lock()
					totalTransactions.Total += int64(len(transactions))
					totalTransactions.Unlock()

					log.Printf("wrote %d transactions in %s to file !\n", len(transactions), tableName)
				}

				wg.Done()

			}(partId)

		}

		wg.Wait()

	}

	return totalTransactions.Total, totalAmount.Total, nil

}

func QueryTransactions(wg *sync.WaitGroup, client *gorm.DB, tableName string, fromBlock, toBlock int, processLastBlock bool) []types.TransactionFileFormat {
	// start := time.Now()
	var transactions []types.TransactionFileFormat

	if processLastBlock {
		err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Where("blocknum >= ? and blocknum <= ? ", fromBlock, toBlock).Order("blocknum, system_date ASC").Find(&transactions).Error
		if err != nil {
			log.Println("Warning :", err)
		}
	} else {
		err := client.Table(tableName).Select("trace_no", "txhash", "sender_id", "receiver_id", "action", "amount", "system_date").Where("blocknum >= ? and blocknum < ?", fromBlock, toBlock).Order("blocknum, system_date ASC").Find(&transactions).Error
		if err != nil {
			log.Println("Warning :", err)
		}
	}

	return transactions

}

func ExportToFile(transactions []types.TransactionFileFormat, filePath string) (int64, error) {

	var totalAmount int64

	// Init writer for writing csv file
	writer, err := csv.NewCsvWriter(filePath)
	if err != nil {
		log.Fatal(err)
	}

	// Content of the first line
	writer.Write([]string{"HD", "TRACE", "TXN_HASH", "FROM", "TO", "TRANSTYPE", "AMOUNT", "STATUS", "TXN_TIME"})

	for _, transaction := range transactions {
		switch transaction.Action {
		case "Credit":
			transaction.Action = "mint"
		case "Debit":
			transaction.Action = "burn"
		case "Transfer":
			transaction.Action = "transfer"
		}
		row := []string{"CT", transaction.TraceNo, transaction.Txhash, strings.ToLower(transaction.SenderId), strings.ToLower(transaction.ReceiverId),
			transaction.Action, fmt.Sprint(transaction.Amount / 100), "00", transaction.SystemDate.Format(hourFormat)}

		// calculate total amount of all transactions

		totalAmount += int64(transaction.Amount / 100)

		// rows = append(rows, row)
		err := writer.Write(row)
		if err != nil {
			// return 0, fmt.Errorf("There's at least 1 error when writing to file : %v", err)
			fmt.Println(err)
		}

	}

	// Content of the last line
	writer.Write([]string{"FT", fmt.Sprint(len(transactions)), fmt.Sprint(len(transactions)), "0", fmt.Sprint(totalAmount)})

	writer.Flush()

	return totalAmount, nil

}

// Round to day unit : 2022-01-02 13:42:31 -> 2022-01-02 00:00:00
func Round(t time.Time) time.Time {
	return t.Truncate(time.Hour * 24)
}

// convert string to time
func FormatDate(processTime string) (time.Time, error) {
	return time.Parse(hourFormat, processTime)
}

func FormatDate2(processTime string) (time.Time, error) {
	return time.Parse(dateFormat, processTime)
}

// Format time to int : 2022-06-20 -> 20220620
func FormatIntTime(t time.Time) (int, error) {
	intDateTime, err := strconv.Atoi(t.Format(intDateFormat))
	if err != nil {
		return 0, fmt.Errorf("Can't parse time to int with format yyyymmdd !")
	}
	return intDateTime, nil
}

// func validateTimeFormat(t string) bool {
// 	r, _ := regexp.Compile("([0-9]{4})(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])(2[0-3]|[01][0-9])([0-5][0-9])([0-5][0-9])")
// 	return r.MatchString(t) && len(t) == validLengthOfInput
// }

// // convert yyyyddmmddhhmmss to yyyy-mm-dd hh:mm:ss
// func formatTimeString(t string) string {

// 	year := t[0:4]
// 	month := t[4:6]
// 	day := t[6:8]
// 	hour := t[8:10]
// 	minute := t[10:12]
// 	second := t[12:]

// 	return fmt.Sprintf("%s-%s-%s %s:%s:%s", year, month, day, hour, minute, second)
// }
