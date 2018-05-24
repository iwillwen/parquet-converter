package dataset

import (
  "fmt"

  "github.com/xitongsys/parquet-go/ParquetWriter"
  "github.com/xitongsys/parquet-go/ParquetFile"
)

type Column struct {
  Name string `json:"name"`
  Encoding *string `json:"encoding"`
  ValType string `json:"val_type"`
  RepetitionType *string `json:"repetition_type"`
}

type Dataset interface {
  Columns() []Column
  Rows() [][]interface{}
}

func NewDatasetWriter(dataset Dataset, outputFile ParquetFile.ParquetFile) (*ParquetWriter.CSVWriter, error) {
  columns := dataset.Columns()
  md := make([]string, len(columns))

  for i, column := range columns {
    md[i] = fmt.Sprintf("name=%s, type=%s", column.Name, column.ValType)

    if column.RepetitionType != nil {
      md[i] += fmt.Sprintf(", repetitiontype=%s", column.RepetitionType)
    }

    if column.Encoding != nil {
      md[i] += fmt.Sprintf(", encoding=%s", *column.Encoding)
    }
  }

  writer, err := ParquetWriter.NewCSVWriter(md, outputFile, 4)
  if err != nil {
    return nil, err
  }

  return writer, nil
}

func WriteParquetFile(dataset Dataset, writer *ParquetWriter.CSVWriter) error {
  rows := dataset.Rows()

  for _, row := range rows {
    rec := make([]*string, len(row))
    for j := 0; j < len(row); j++ {
      value := fmt.Sprintf("%v", row[j])
      rec[j] = &value
    }
    writer.WriteString(rec)
  }

  if err := writer.WriteStop(); err != nil {
    return nil
  }

  return nil
}