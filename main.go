package main

import (
	"e_wallet/config"
	"e_wallet/db"
	"e_wallet/handler"
	"e_wallet/service"
	"log"
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
	handler.Serve(reportService, port)
}
