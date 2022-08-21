package db

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func InitDB(pgConfig map[string]string) *gorm.DB {
	host := pgConfig["host"]
	port := pgConfig["port"]
	username := pgConfig["username"]
	password := pgConfig["password"]
	dbname := pgConfig["database"]

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s", host, username, password, dbname, port)
	fmt.Printf("PgService.NewPgService: dsn = %s\n", dsn)
	postgresDB, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true, // disables implicit prepared statement usage
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic(err)
	}
	return postgresDB
}
