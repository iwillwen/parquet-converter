# Parquet Converter

Generating Apache Parquet file with JSON file or CSV files.

## Installation

`parquet-converter` depends on `xitongsys/parquet-go`, so you should install it first.

```bash
$ go get github.com/xitongsys/parquet-go/...
$ git clone github.com/iwillwen/parquet-converter
$ cd parquet-converter
$ go build

$ ./parquet-converter -h
```

## Usage

Now `parquet-converter` supports json and csv to generate a parquet file.

### JSON

Firstly, defining a json file includes the columns definations and the content is required.

```json
// test/test.json
{
  "columns": [
    { "name": "column_1", "val_type": "UTF8", "repetition_type": "REQUIRED"},
    { "name": "column_2", "val_type": "INT64" }
  ],
  "rows": [
    [ "foo", 1 ],
    [ "bar", 100 ]
  ]
}
```

Generating a parquet file with `parquet-converter`.

```bash
$ ./parquet-converter json2parquet -file test/test.json -out out.parquet
```

Check out the parquet file in Spark Shell.

```
scala> spark.read.format("parquet").load("file:///path/to/parquet-converter/out.parquet").show()
+--------+--------+
|column_1|column_2|
+--------+--------+
|     foo|       1|
|     bar|     100|
+--------+--------+
```

### CSV

If you want to convert the data in a csv file, you have to define a columns defination csv file first.

`test/test-columns.csv`
```csv
name,val_type,repetition_type
column_1,UTF8,REQUIRED
column_2,INT64,
column_3,BOOLEAN,
```

And the data file. `test/test-rows.csv`

```csv
column_1,column_2,column_3
foo,1,
bar,2,true
```

```bash
$ ./parquet-converter csv2parquet -columns test/test-columns.csv -rows test/test-rows.csv  -out out.parquet
```

Check it out in Spark.

```
scala> spark.read.format("parquet").load("file:///path/to/parquet-converter/out.parquet").show()
+--------+--------+--------+
|column_1|column_2|column_3|
+--------+--------+--------+
|     foo|       1|   false|
|     bar|       2|    true|
+--------+--------+--------+
```

## License

MIT Licensed.