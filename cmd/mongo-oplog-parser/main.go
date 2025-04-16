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

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mongo-oplog-parser",
	Short: "Responsible for converting mongo oplog to SQL",
	Long:  `It provides tools to process and analyze MongoDB operation logs for various use cases such as replication, auditing, and debugging.`,
	Run:   generateSQL,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mongoOplogParser.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().StringP("Input", "f", "", "Source mongodb oplog")
	rootCmd.Flags().StringP("Output", "o", "", "Destination SQL")
}

func generateSQL(cmd *cobra.Command, args []string) {
	input, _ := cmd.Flags().GetString("Input")
	output, _ := cmd.Flags().GetString("Output")

	oplogReader, err := reader.NewMongoReader(input)

	defer oplogReader.Close()

	if err != nil {
		log.Fatalf("Unable to read file")
	}

	oplogWriter, err := writer.NewFileWriter(output)

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
