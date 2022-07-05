package main

import (
	"log"
	"os"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"

	_ "github.com/johannaojeling/go-data-ingestion"
)

var port = os.Getenv("PORT")

func init() {
	if port == "" {
		port = "8080"
	}
}

func main() {
	log.Printf("listening on port %s\n", port)
	err := funcframework.Start(port)
	if err != nil {
		log.Fatalf("failed to start funcframework: %v", err)
	}
}
