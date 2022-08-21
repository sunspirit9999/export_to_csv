package types

type SucessResponse struct {
	Code    int
	Message string
}

type ErrorResponse struct {
	Code    int
	Message string
	Error   string
}
