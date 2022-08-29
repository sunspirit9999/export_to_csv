package main

import (
	"e_wallet/config"
	"e_wallet/db"
	"e_wallet/handler"
	"e_wallet/service"
	"fmt"
	"log"
	"time"

	cron "github.com/robfig/cron/v3"
)

func main() {
	config, err := config.Load()
	if err != nil {
		log.Fatal("Failed to load config !")
	}

	pgConfig := config.GetStringMapString("postgres")
	client := db.InitDB(pgConfig)

	filePath := config.GetString("file.path")

	prefixFileName := config.GetString("file.prefixName")

	port := config.GetInt("http.port")

	reportService := service.NewReportService(client, filePath, prefixFileName)

	c := cron.New()

	c.AddFunc("28 17 * * *", func() {
		fmt.Println("Every hour on the half hour sss4")
		now := time.Date(2022, 7, 2, 1, 0, 0, 0, time.Local)
		dayBefore := now.AddDate(0, 0, -1)
		fmt.Printf("Subtract 1 Day: %s and %s ", now, dayBefore)
		reportService.AutoExport(dayBefore, now)
	})
	c.Start()

	handler.Serve(reportService, port)
}
