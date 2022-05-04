package main

import (
	"log"
	"os"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"

	_ "github.com/johannaojeling/go-data-ingestion"
)

func main() {
	port, ok := os.LookupEnv("PORT")
	if !ok {
		port = "8080"
	}

	log.Printf("listening on port %s\n", port)
	err := funcframework.Start(port)
	if err != nil {
		log.Fatalf("failed to start funcframework: %v", err)
	}
}
