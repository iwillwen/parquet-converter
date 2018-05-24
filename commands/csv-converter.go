package commands

import (
  "os"
  "../dataset"
  "encoding/csv"
)

type CSVFileInput struct {
  ColumnsFile *os.File
  RowsFile *os.File
}

type CSVDataset struct {
  ColumnsInter []dataset.Column
  RowsInter [][]interface{}
}

func (d CSVDataset) Columns() []dataset.Column {
  return d.ColumnsInter
}

func (d CSVDataset) Rows() [][]interface{} {
  return d.RowsInter
}

func LoadCSVDataset(input *CSVFileInput) (dataset.Dataset, error) {
  var ds CSVDataset

  columns, err := loadColumns(input.ColumnsFile)
  if err != nil {
    return ds, err
  }

  rows, err := loadRows(columns, input.RowsFile)
  if err != nil {
    return ds, err
  }

  ds.ColumnsInter = columns
  ds.RowsInter = rows

  return ds, nil
}

func loadColumns(file *os.File) ([]dataset.Column, error) {
  reader := csv.NewReader(file)

  records, err := reader.ReadAll()
  if err != nil {
    return []dataset.Column{}, err
  }

  headers := records[0]
  lenOfHeaders := len(headers)
  var (
    nameIndex = SliceIndex(lenOfHeaders, func(i int) bool { return headers[i] == "name" })
    valTypeIndex = SliceIndex(lenOfHeaders, func(i int) bool { return headers[i] == "val_type" })
    repetitionTypeIndex = SliceIndex(lenOfHeaders, func(i int) bool { return headers[i] == "repetition_type" })
    encodingIndex = SliceIndex(lenOfHeaders, func(i int) bool { return headers[i] == "encoding" })
  )

  columns := make([]dataset.Column, len(records[1:]))

  for i, columnRow := range records[1:] {
    column := dataset.Column{}

    if nameIndex >= 0 {
      column.Name = columnRow[nameIndex]
    }

    if valTypeIndex >= 0 {
      column.ValType = columnRow[valTypeIndex]
    }

    if repetitionTypeIndex >= 0 {
      column.RepetitionType = &columnRow[repetitionTypeIndex]
    }

    if encodingIndex >= 0 {
      column.Encoding = &columnRow[encodingIndex]
    }

    columns[i] = column
  }

  return columns, nil
}

func loadRows(columns []dataset.Column, file *os.File) ([][]interface{}, error) {
  reader := csv.NewReader(file)

  records, err := reader.ReadAll()
  if err != nil {
    return [][]interface{}{}, err
  }

  headers := records[0]
  records = records[1:]

  indexes := make([]int, len(columns))

  for i, column := range columns {
    index := SliceIndex(len(headers), func(j int) bool { return headers[j] == column.Name })
    indexes[i] = index
  }

  rows := make([][]interface{}, len(records))

  for i, record := range records {
    row := make([]interface{}, len(indexes))

    for j, index := range indexes {
      row[j] = record[index]
    }

    rows[i] = row
  }

  return rows, nil
}

func SliceIndex(limit int, predicate func(i int) bool) int {
  for i := 0; i < limit; i++ {
    if predicate(i) {
      return i
    }
  }
  return -1
}