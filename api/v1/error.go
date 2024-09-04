package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("Offset out of range: %d \n", e.Offset),
	)

	msg := fmt.Sprintf(
		"Request offset is outside the log range: %d \n",
		e.Offset,
	)

	localMsg := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}

	sts, err := st.WithDetails(localMsg)
	if err != nil {
		return st
	}

	return sts
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
