package api

type ReportAPI interface {
	ExportTransaction(from string, to string)
}
