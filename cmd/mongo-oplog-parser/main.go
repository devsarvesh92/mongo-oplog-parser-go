/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/devsarvesh92/mongoOplogParser/internal/adapter/reader"
	"github.com/devsarvesh92/mongoOplogParser/internal/adapter/writer"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/model"
	"github.com/devsarvesh92/mongoOplogParser/internal/domain/service/parser"
	"github.com/spf13/cobra"
)

func main() {
	Execute()
}

var rootCmd = &cobra.Command{
	Use:   "mongo-oplog-parser",
	Short: "Responsible for converting mongo oplog to SQL",
	Long:  `It provides tools to process and analyze MongoDB operation logs for various use cases such as replication, auditing, and debugging.`,
	Run:   generateSQL,
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringP("Source", "f", "", "Source mongodb oplog")
	rootCmd.Flags().StringP("Desitination", "o", "", "Destination SQL")
	rootCmd.Flags().StringP("SourceType", "t", "mongo-file", "Type of input (mongo-file, mongo-stream)")
	rootCmd.Flags().StringP("DesitinationType", "w", "file", "Type of output (file, database)")
}

func generateSQL(cmd *cobra.Command, args []string) {
	source, _ := cmd.Flags().GetString("Source")
	destination, _ := cmd.Flags().GetString("Desitination")
	sourceType, _ := cmd.Flags().GetString("SourceType")
	destinationType, _ := cmd.Flags().GetString("DesitinationType")

	oplogReader, err := reader.NewReader(reader.ReaderType(sourceType), source)

	defer oplogReader.Close()

	if err != nil {
		log.Fatalf("Unable to read file")
	}

	oplogWriter, err := writer.NewWriter(writer.WriterType(destinationType), destination)

	if err != nil {
		log.Fatalf("Unable to create output")
	}
	defer oplogWriter.Close()
	oplogParser := parser.NewMongoOplogParser(model.NewTracker())
	for {
		oplog, err := oplogReader.ReadOplog()

		if err == io.EOF {
			fmt.Printf("Done with file processing")
			os.Exit(1)
		}

		result := oplogParser.GenerateSQL([]model.Oplog{oplog})

		for _, sql := range result.SQL {
			err := oplogWriter.WriteSQL(sql)
			fmt.Println(sql)
			if err != nil {
				fmt.Printf("unable to write sql %v due to error %v", sql, err)
			}
		}
	}
}
