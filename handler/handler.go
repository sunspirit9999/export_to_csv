package handler

import (
	"e_wallet/service"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func Serve(reportService *service.ReportService, port int) {

	r := mux.NewRouter()

	p := fmt.Sprintf(":%d", port)

	r.HandleFunc("/transactions", reportService.ExportTransaction).Methods(http.MethodGet)

	log.Println("Server is listening !!!")

	log.Fatal(http.ListenAndServe(p, r))
}
