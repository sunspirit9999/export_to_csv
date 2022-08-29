package types

import (
	"sync"
	"time"
)

type Counter struct {
	sync.Mutex
	Total int64
}

type Transaction struct {
	Id            int
	BlockNum      int    `gorm:"index:idx_transaction,unique"`
	Txhash        string `gorm:"index:idx_transaction,unique"`
	TraceNo       string
	Action        string
	Amount        int
	SenderPre     int
	SenderAfter   int
	ReceiverPre   int
	ReceiverAfter int
	ReceiverId    string
	SenderId      string
	SystemDate    time.Time
	WorkingDate   time.Time
	MigrateRetry  int
	MigrateTime   time.Time
	Parsed        bool
	ReceiptId     int
}

type AccountBalance struct {
	Id          int
	AccountId   string
	Amount      int
	Balance     int
	BalancePre  int
	SystemDate  time.Time
	WorkingDate time.Time
	TraceNo     string
	BlockNum    int
}

type TransactionFileFormat struct {
	Txhash     string
	TraceNo    string
	Action     string
	Amount     int
	ReceiverId string
	SenderId   string
	SystemDate time.Time
}
