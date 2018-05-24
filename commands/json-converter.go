package commands

import (
  "os"
  "io/ioutil"
  "encoding/json"
  "../dataset"
)

type JSONFileInput struct {
  File *os.File
}

type JSONDataset struct {
  ColumnsInter []dataset.Column `json:"columns"`
  RowsInter [][]interface{} `json:"rows"`
}

func (d JSONDataset) Columns() []dataset.Column {
  return d.ColumnsInter
}

func (d JSONDataset) Rows() [][]interface{} {
  return d.RowsInter
}

func LoadJSONDataset(input *JSONFileInput) (dataset.Dataset, error) {
  fileData, err := ioutil.ReadAll(input.File)
  if err != nil {
    return nil, err
  }

  var ds JSONDataset

  if err = json.Unmarshal(fileData, &ds); err != nil {
    return nil, err
  }

  return ds, nil
}