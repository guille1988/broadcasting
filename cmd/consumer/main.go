package main

import (
	"broadcasting/internal/bootstrap"
	"broadcasting/internal/infrastructure/logger"
)

func main() {
	consumer, err := bootstrap.NewConsumer()

	if err != nil {
		logger.Fatal(err)
	}

	err = bootstrap.RunConsumer(consumer)

	if err != nil {
		logger.Fatal(err)
	}
}
