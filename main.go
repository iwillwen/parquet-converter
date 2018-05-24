package main

import (
  "flag"
  "os"
  "fmt"

  "./commands"
  datasets "./dataset"
  "github.com/xitongsys/parquet-go/ParquetFile"
)

var (
  command string
)

func main() {

  // SubCommands
  jsonToParquetCommand := flag.NewFlagSet("json2parquet", flag.ExitOnError)
  csvToParquetCommand := flag.NewFlagSet("csv2parquet", flag.ExitOnError)

  // JSON
  jsonFilePtr := jsonToParquetCommand.String("file", "", "JSON File to convert. (Required)")
  jsonOutputFilePtr := jsonToParquetCommand.String("out", "out.parquet", "Output parquet file.")

  // CSV
  columnsCSVFilePtr := csvToParquetCommand.String("columns", "", "Columns CSV File to convert. (Required)")
  rowsCSVFilePtr := csvToParquetCommand.String("rows", "", "Rows CSV File to convert. (Required)")
  csvOutputFilePtr := csvToParquetCommand.String("out", "out.parquet", "Output parquet file.")

  if len(os.Args) < 2 {
    fmt.Println("subcommand is required")
    os.Exit(1)
  }

  switch os.Args[1] {
  case "json2parquet":
    jsonToParquetCommand.Parse(os.Args[2:])
  case "csv2parquet":
    csvToParquetCommand.Parse(os.Args[2:])
  default:
    flag.PrintDefaults()
    os.Exit(1)
  }


  if jsonToParquetCommand.Parsed() {
    if *jsonFilePtr == "" {
      jsonToParquetCommand.PrintDefaults()
      os.Exit(1)
    }

    file, err := os.Open(*jsonFilePtr)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    dataset, err := commands.LoadJSONDataset(&commands.JSONFileInput{ File: file })
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    fw, err := ParquetFile.NewLocalFileWriter(*jsonOutputFilePtr)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    writer, err := datasets.NewDatasetWriter(dataset, fw)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    err = datasets.WriteParquetFile(dataset, writer)
    if err != nil {
      panic(err)
      os.Exit(1)
    }
  }

  if csvToParquetCommand.Parsed() {
    if *columnsCSVFilePtr == "" {
      csvToParquetCommand.PrintDefaults()
      os.Exit(1)
    }

    if *rowsCSVFilePtr == "" {
      csvToParquetCommand.PrintDefaults()
      os.Exit(1)
    }

    columnsFile, err := os.Open(*columnsCSVFilePtr)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    rowsFile, err := os.Open(*rowsCSVFilePtr)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    dataset, err := commands.LoadCSVDataset(&commands.CSVFileInput{
      ColumnsFile: columnsFile,
      RowsFile: rowsFile,
    })

    fw, err := ParquetFile.NewLocalFileWriter(*csvOutputFilePtr)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    writer, err := datasets.NewDatasetWriter(dataset, fw)
    if err != nil {
      panic(err)
      os.Exit(1)
    }

    err = datasets.WriteParquetFile(dataset, writer)
    if err != nil {
      panic(err)
      os.Exit(1)
    }
  }
}