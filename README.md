# trdsql

[![GoDoc](https://godoc.org/github.com/noborus/trdsql?status.svg)](https://godoc.org/github.com/noborus/trdsql)
[![Build Status](https://travis-ci.org/noborus/trdsql.svg?branch=master)](https://travis-ci.org/noborus/trdsql)

A tool that can execute SQL queries on [CSV](https://tools.ietf.org/html/rfc4180), [LTSV](http://ltsv.org/), [JSON](https://tools.ietf.org/html/rfc7159) and [TBLN](https://tbln.dev/).

It is a tool like [q](https://github.com/harelba/q) , [textql](https://github.com/dinedal/textql) and others.

The difference from these tools is that the syntax of PostgreSQL or MySQL can be used.

Please refer to [godoc](https://godoc.org/github.com/noborus/trdsql) and _example for usage as a library.

![trdsql.gif](doc/trdsql.gif)

## INSTALL

```
go get -d github.com/noborus/trdsql
cd $GOPATH/src/github.com/noborus/trdsql
make
make install
```

Or download binaries from the [releases](https://github.com/noborus/trdsql/releases) page(Linux/Windows/macOS).

## Usage

```
$ trdsql [options] SQL
```

### global option

* `-config` **filename**
        Configuration file location.
* `-db` **db name**
        Specify db name of the setting.
* `-dblist`
        Display db list of configure.
* `-driver` **driver name**
        database driver.  [ mysql | postgres | sqlite3 ]
* `-dsn` **dsn string**
        database connection option.
* `-debug`
        debug print.
* `-help`
        display usage information.
* `-version`
        display version information.
* `-q` **filename**
        Read query from the provided filename.

### Input format

* `-ig`
        Guess format from extension(default).
* `-icsv`
        CSV format for input.
* `-ijson`
        JSON format for input.
* `-iltsv`
        LTSV format for input.
* `-itbln`
        TBLN format for input.

#### Input option

* `-ih`
        The first line is interpreted as column names(CSV only).
* `-id` **delimiter character**(CSV only)
        Field delimiter for input. (default ",")
* `-is` **int**
        Skip header row.
* `-ir` **int**
        Number of row pre-read for column determination. (default 1)

### Output format

* `-ocsv`
        CSV format for output. (default)
* `-ojson`
        JSON format for output.
* `-oltsv`
        LTSV format for output.
* `-oat`
        ASCII Table format for output.
* `-omd`
        Mark Down format for output.
* `-oraw`
        Raw format for output.
* `-ovf`
        Vertical format for output.
* `-otbln`
        TBLN format for output.

#### Output option

* `-oh`
        Output column name as header.
* `-od` **delimiter character**
        Field delimiter for output. (default ",")

## Example

test.csv file.

```csv
1,Orange
2,Melon
3,Apple
```

Please write a file name like a table name.

```console
$ trdsql "SELECT * FROM test.csv"
```

-q filename can execute SQL from file

```console
$ trdsql -q test.sql
```

### STDIN input

"-" or "stdin" is received from standard input instead of file name.
```console
$ ps | trdsql -id " " "SELECT * FROM -"
```
or
```console
$ ps | trdsql -id " " "SELECT * FROM stdin"
```

### Multiple files

Multiple matched files can be executed as one table.

```console
$ trdsql -ih "SELECT * FROM tt*.csv"
1,test1
2,test2
3,test3
```

**Note:** It is not possible to mix different formats (ex: CSV and LTSV).

### Expand gzip

Files with the file name suffix of ".gz" are automatically expanded.

```console
$ trdsql "SELECT * FROM testdata/test.csv.gz"
```

It is possible to mix uncompressed and compressed files using wildcards.

```console
$ trdsql "SELECT * FROM testdata/test.csv*"
```

### Columns is not constant

If the number of columns is not a constant, read and decide multiple rows.

```console
$ trdsql -ir 3 -iltsv "SELECT * FROM test_indefinite.ltsv"
1,Orange,50,,
2,Melon,500,ibaraki,
3,Apple,100,aomori,red
```

### TSV (Tab Separated Value)

-id "\\t" is input from TSV (Tab Separated Value)

```
1	Orange
2	Melon
3	Apple
```

```console
$ trdsql -id "\t" "SELECT * FROM test-tab.csv"
```

-od "\\t" is TSV (Tab Separated Value) output.

```console
$ trdsql -od "\t" "SELECT * FROM test.csv"
```
```
1	Orange
2	Melon
3	Apple
```

### LTSV ([Labeled Tab-separated Values](http://ltsv.org/))

-iltsv is input from LTSV(Labeled Tab-separated Values).

sample.ltsv
```ltsv
id:1	name:Orange	price:50
id:2	name:Melon	price:500
id:3	name:Apple	price:100
```

```console
$ trdsql -iltsv "SELECT * FROM sample.ltsv"
```

```csv
1,Orange,50
2,Melon,500
3,Apple,100
```

**Note:** Only the columns in the first row are targeted.

-oltsv is LTSV(Labeled Tab-separated Values) output.

```console
$ trdsql -iltsv -oltsv "SELECT * FROM sample.ltsv"
```

```ltsv
id:1	name:Orange	price:50
id:2	name:Melon	price:500
id:3	name:Apple	price:100
```

### JSON

-ijson is input from JSON.

sample.json
```json
[
  {
    "id": "1",
    "name": "Orange",
    "price": "50"
  },
  {
    "id": "2",
    "name": "Melon",
    "price": "500"
  },
  {
    "id": "3",
    "name": "Apple",
    "price": "100"
  }
]
```

```console
$ trdsql -ijson "SELECT * FROM sample.json"
```

```csv
1,Orange,50
2,Melon,500
3,Apple,100
```

JSON can contain structured types, but trdsql is stored as it is as JSON string.

sample2.json
```json
[
    {
      "id": 1,
      "name": "Drolet",
      "attribute": { "country": "Maldives", "color": "burlywood" }
    },
    {
      "id": 2,
      "name": "Shelly",
      "attribute": { "country": "Yemen", "color": "plum" }
    },
    {
      "id": 3,
      "name": "Tuck",
      "attribute": { "country": "Mayotte", "color": "antiquewhite" }
    }
]
```

```console
trdsql -ijson "SELECT * FROM sample2.json"
```

```csv
1,Drolet,"{""color"":""burlywood"",""country"":""Maldives""}"
2,Shelly,"{""color"":""plum"",""country"":""Yemen""}"
3,Tuck,"{""color"":""antiquewhite"",""country"":""Mayotte""}"
```

Please use SQL function.

```console
trdsql -ijson "SELECT id, name, JSON_EXTRACT(attribute,'$country'), JSON_EXTRACT(attribute,'$color') FROM sample2.json"
```
```csv
1,Drolet,Maldives,burlywood
2,Shelly,Yemen,plum
3,Tuck,Mayotte,antiquewhite
```

Another json format. One record is JSON.

sample2.json
```json
{
  "id": "1",
  "name": "Orange",
  "price": "50"
}
{
  "id": "2",
  "name": "Melon",
  "price": "500"
}
{
  "id": "3",
  "name": "Apple",
  "price": "100"
}
```


-ojson is JSON Output.

```console
$ trdsql -ojson "SELECT * FROM test.csv"
```

```json
[
  {
    "c1": "1",
    "c2": "Orange"
  },
  {
    "c1": "2",
    "c2": "Melon"
  },
  {
    "c1": "3",
    "c2": "Apple"
  }
]
```

### TBLN

-itbln is input from TBLN.

sample.tbln

```TBLN
; name: | id | name |
; type: | int | text |
| 1 | Bob |
| 2 | Alice |
```

```console
$ trdsql -itbln "SELECT * FROM sample.tbln"
```

```csv
1,Bob
2,Alice
```

TBLN file reflects extras name and type.

-otbln is TBLN Output.

```console
$ trdsql -otbln "SELECT c1::int as id, c2::text as name FROM test.csv"
```

```TBLN
; created_at: 2019-03-22T13:20:31+09:00
; name: | id | name |
; type: | int | text |
| 1 | Orange |
| 2 | Melon |
| 3 | Apple |
```

TBLN can contain column names and type definitions.
Please refer to https://tbln.dev/ for details of TBLN.

### Raw output

-oraw is Raw Output.
It is used when "escape processing is unnecessary" in CSV output.
(For example, when outputting JSON in the database).

```console
$ trdsql -oraw "SELECT row_to_json(t,TRUE) FROM test.csv AS t"
```

```
{"c1":"1",
 "c2":"Orange"}
{"c1":"2",
 "c2":"Melon"}
{"c1":"3",
 "c2":"Apple"}
```

Multiple delimiter characters can be used for raw.

```
trdsql -oraw -od "\t|\t" -db pdb "SELECT * FROM test.csv"
```

```
1	|	Orange
2	|	Melon
3	|	Apple
```


### ASCII Table & MarkDown output

-oat is ASCII table output.
It uses [tablewriter](https://github.com/olekukonko/tablewriter).


```console
$ trdsql -oat "SELECT * FROM test.csv"
```

```
+----+--------+
| C1 |   C2   |
+----+--------+
|  1 | Orange |
|  2 | Melon  |
|  3 | Apple  |
+----+--------+
```

-omd is Markdown output.

```console
$ trdsql -omd "SELECT * FROM test.csv"
```

```
| C1 |   C2   |
|----|--------|
|  1 | Orange |
|  2 | Melon  |
|  3 | Apple  |
```

### Vertical format output

-ovf is Vertical format output("column name | value" vertically).

```console
$ trdsql -ovf "SELECT * FROM test.csv"
```

```
---[ 1]--------------------------------------------------------
  c1 | 1
  c2 | Orange
---[ 2]--------------------------------------------------------
  c1 | 2
  c2 | Melon
---[ 3]--------------------------------------------------------
  c1 | 3
  c2 | Apple
```

### SQL function

```console
$ trdsql "SELECT count(*) FROM test.csv"
```
```
3
```

The default column names are c1, c2,...

```console
$ trdsql "SELECT c2,c1 FROM test.csv"
```

```csv
Orange,1
Melon,2
Apple,3
```
"- ih" sets the first line to column name

```console
$ ps |trdsql -ih -oh -id " " "SELECT \"TIME\",\"TTY\",\"PID\",\"CMD\" FROM -"

TIME,TTY,PID,CMD
00:00:00,pts/20,3452,ps
00:00:00,pts/20,3453,trdsql
00:00:05,pts/20,15576,zsh
```

### JOIN

The SQL JOIN can be used.

user.csv
```csv
1,userA
2,userB
```

hist.csv
```csv
1,2017-7-10
2,2017-7-10
2,2017-7-11
```

```console
$ trdsql "SELECT u.c1,u.c2,h.c2 FROM user.csv as u LEFT JOIN hist.csv as h ON(u.c1=h.c1)"
1,userA,2017-7-10
2,userB,2017-7-10
2,userB,2017-7-11
```

### PostgreSQL

When using PostgreSQL, specify postgres for driver and connection information for dsn.

```console
$ trdsql -driver postgres -dsn "dbname=test" "SELECT count(*) FROM test.csv "
```

#### Function

The PostgreSQL driver can use the window function.

```console
$ trdsql -driver postgres -dsn "dbname=test" "SELECT row_number() OVER (ORDER BY c2),c1,c2 FROM test.csv"
1,3,Apple
2,2,Melon
3,1,Orange
```

For example, the generate_series function can be used.

```console
$ trdsql -driver postgres -dsn "dbname=test" "SELECT generate_series(1,3);"
1
2
3
```

#### Join table and CSV file is possible.

Test database has a colors table.
```console
$ psql test -c "SELECT * FROM colors"
 id |  name  
----+--------
  1 | orange
  2 | green
  3 | red
(3 rows)
```

Join table and CSV file.

```console
$ trdsql -driver postgres -dsn "dbname=test" "SELECT t.c1,t.c2,c.name FROM test.csv AS t LEFT JOIN colors AS c ON (t.c1::int = c.id)"
1,Orange,orange
2,Melon,green
3,Apple,red
```

To create a table from a file, use "CREATE TABLE ... AS SELECT...".

```console
$ trdsql -driver postgres -dns "dbname=test" "CREATE TABLE fruits (id, name) AS SELECT c1::int, c2 FROM fruits.csv "
```

```console
$ psql -c "SELECT * FROM fruits;"
 id |  name  
----+--------
  1 | Orange
  2 | Melon
  3 | Apple
(3 rows)
```

### MySQL

When using MySQL, specify mysql for driver and connection information for dsn.

```console
$ trdsql -driver mysql -dsn "user:password@/test" "SELECT GROUP_CONCAT(c2 ORDER BY c2 DESC) FROM testdata/test.csv"
```

```csv
"g,d,a"
```

```console
$ trdsql -driver mysql -dsn "user:password@/test" "SELECT c1, SHA2(c2,224) FROM test.csv"
```

```csv
1,a063876767f00792bac16d0dac57457fc88863709361a1bb33f13dfb
2,2e7906d37e9523efeefb6fd2bc3be6b3f2991678427bedc296f9ddb6
3,d0b8d1d417a45c7c58202f55cbb617865f1ef72c606f9bce54322802
```

MySQL can join tables and CSV files as well as PostgreSQL.

### configuration

You can specify driver and dsn in the configuration file.

Unix like.
```
${HOME}/.config/trdsql/config.json

```
Windows (ex).
```
C:\Users\{"User"}\AppData\Roaming\trdsql\config.json
```
Or use the -config file option.

```console
$ trdsql -config config.json "SELECT * FROM test.csv"
```

 sample: [config.json](config.json.sample)

```json
{
  "db": "pdb",
  "database": {
    "sdb": {
      "driver": "sqlite3",
      "dsn": ""
    },
    "pdb": {
      "driver": "postgres",
      "dsn": "user=test dbname=test"
    },
    "mdb": {
      "driver": "mysql",
      "dsn": "user:password@/dbname"
    }
  }
}
```

The default database is an entry of "db".

If you put the setting in you can specify the name with -db.

```console
$ trdsql -debug -db pdb "SELECT * FROM test.csv"
2017/07/18 02:27:47 driver: postgres, dsn: user=test dbname=test
2017/07/18 02:27:47 CREATE TEMPORARY TABLE "test.csv" ( c1 text,c2 text );
2017/07/18 02:27:47 INSERT INTO "test.csv" (c1,c2) VALUES ($1,$2);
2017/07/18 02:27:47 SELECT * FROM "test.csv"
1,Orange
2,Melon
3,Apple
```

## Library

Example of use as a library.

```go
package main

import (
        "log"

        "github.com/noborus/trdsql"
)

func main() {
        trd := trdsql.NewTRDSQL(
                trdsql.NewImporter(trdsql.InDelimiter(":")),
                trdsql.NewExporter(trdsql.NewWriter()),
        )
        err := trd.Exec("SELECT c1 FROM /etc/passwd")
        if err != nil {
                log.Fatal(err)
        }
}
```

Please refer to [godoc](https://godoc.org/github.com/noborus/trdsql) and _example for usage as a library.

## License

MIT

Please check each license of SQL driver.
* [SQLite](https://github.com/mattn/go-sqlite3)
* [MySQL](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://github.com/lib/pq)
