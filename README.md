# trdsql

[![GoDoc](https://godoc.org/github.com/noborus/trdsql?status.svg)](https://godoc.org/github.com/noborus/trdsql)
[![Go Report Card](https://goreportcard.com/badge/github.com/noborus/trdsql)](https://goreportcard.com/report/github.com/noborus/trdsql)
[![GoCover.io](https://gocover.io/_badge/github.com/noborus/trdsql)](https://gocover.io/github.com/noborus/trdsql)
![Go](https://github.com/noborus/trdsql/workflows/Go/badge.svg)

CLI tool that can execute SQL queries on [CSV](https://tools.ietf.org/html/rfc4180),
 [LTSV](http://ltsv.org/), [JSON](https://tools.ietf.org/html/rfc7159) and [TBLN](https://tbln.dev/).

It is a tool like [q](https://github.com/harelba/q),
 [textql](https://github.com/dinedal/textql) and others.

The difference from these tools is that the syntax of
 PostgreSQL or MySQL can be used.

Please refer to [godoc](https://godoc.org/github.com/noborus/trdsql)
 and _example for usage as a library.

![trdsql.gif](https://raw.githubusercontent.com/noborus/trdsql/master/doc/trdsql.gif)

<!-- vscode-markdown-toc -->
* 1. [INSTALL](#INSTALL)
	* 1.1. [Requirements](#Requirements)
	* 1.2. [Download](#Download)
* 2. [Docker](#Docker)
	* 2.1. [Docker pull](#Dockerpull)
	* 2.2. [image build](#imagebuild)
	* 2.3. [Docker Run](#DockerRun)
* 3. [Usage](#Usage)
	* 3.1. [global option](#globaloption)
	* 3.2. [Input format](#Inputformat)
		* 3.2.1. [Input option](#Inputoption)
	* 3.3. [Output format](#Outputformat)
		* 3.3.1. [Output option](#Outputoption)
* 4. [Example](#Example)
	* 4.1. [STDIN input](#STDINinput)
	* 4.2. [Multiple files](#Multiplefiles)
	* 4.3. [Compressed files](#Compressedfiles)
	* 4.4. [Output file](#Outputfile)
	* 4.5. [Output compression](#Outputcompression)
	* 4.6. [Guess by output file name](#Guessbyoutputfilename)
	* 4.7. [Columns is not constant](#Columnsisnotconstant)
	* 4.8. [TSV (Tab Separated Value)](#TSVTabSeparatedValue)
	* 4.9. [LTSV (Labeled Tab-separated Values)](#LTSVLabeledTab-separatedValues)
	* 4.10. [JSON](#JSON)
	* 4.11. [JSONL](#JSONL)
	* 4.12. [TBLN](#TBLN)
	* 4.13. [Raw output](#Rawoutput)
	* 4.14. [ASCII Table & MarkDown output](#ASCIITableMarkDownoutput)
	* 4.15. [Vertical format output](#Verticalformatoutput)
	* 4.16. [SQL function](#SQLfunction)
	* 4.17. [JOIN](#JOIN)
	* 4.18. [PostgreSQL](#PostgreSQL)
		* 4.18.1. [Function](#Function)
		* 4.18.2. [Join table and CSV file is possible](#JointableandCSVfileispossible)
	* 4.19. [MySQL](#MySQL)
	* 4.20. [Analyze](#Analyze)
	* 4.21. [configuration](#configuration)
* 5. [Library](#Library)
* 6. [License](#License)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

##  1. <a name='INSTALL'></a>INSTALL

```console
go get -d github.com/noborus/trdsql
cd $GOPATH/src/github.com/noborus/trdsql
make
make install
```

###  1.1. <a name='Requirements'></a>Requirements

go 1.11 or higher.

###  1.2. <a name='Download'></a>Download

Download binary from the [releases](https://github.com/noborus/trdsql/releases) page(Linux/Windows/macOS).

##  2. <a name='Docker'></a>Docker

###  2.1. <a name='Dockerpull'></a>Docker pull

Pull the latest image from the Docker hub.

```console
docker pull noborus/trdsql
```

###  2.2. <a name='imagebuild'></a>image build

Or build it yourself.

```console
docker build -t trdsql .
````

###  2.3. <a name='DockerRun'></a>Docker Run

Docker run.

```console
docker run --rm -it -v $(pwd)/tmp trdsql [options and commands]
```

##  3. <a name='Usage'></a>Usage

```console
trdsql [options] SQL
```

###  3.1. <a name='globaloption'></a>global option

* `-a` **filename**
        Analyze file and suggest SQL.
* `-A` **filename**
        Analyze but only suggest SQL.
* `-config` **filename**
        Configuration file location.
* `-db` **db name**
        Specify db name of the setting.
* `-dblist`
        Display db list of configure.
* `-driver` **string**
        database driver driver.  [ mysql | postgres | sqlite3 ]
* `-dsn` **string**
        data source name.
* `-debug`
        debug print.
* `-help`
        display usage information.
* `-version`
        display version information.
* `-q` **filename**
        Read query from the provided filename.

###  3.2. <a name='Inputformat'></a>Input format

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

####  3.2.1. <a name='Inputoption'></a>Input option

* `-ih`
        The first line is interpreted as column names(CSV only).
* `-id` **character**
        Field delimiter for input. (default ",")(CSV only)
* `-is` **int**
        Skip header row.
* `-ir` **int**
        Number of row pre-read for column determination. (default 1)

###  3.3. <a name='Outputformat'></a>Output format

* `-ocsv`
        CSV format for output. (default)
* `-ojson`
        JSON format for output.
* `-ojsonl`
        JSONL(JSON Lines) format for output.
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

Or, [guess the output format by file name](#Guessbyoutputfilename).

####  3.3.1. <a name='Outputoption'></a>Output option

* `-out` **filename**
        Output file name.
* `-out-without-guess`
        Output without guessing from file name.
* `-oh`
        Output column name as header.
* `-od` **character**
        Field delimiter for output. (default ",")(CSV and RAW only)
* `-oq` **character**
        quote character for output. (default "\"")(CSV only).
* `-oaq`
        Enclose all fields in quotes for output(CSV only).
* `-ocrlf`
        Use CRLF for output(CSV only).


##  4. <a name='Example'></a>Example

test.csv file.

```csv
1,Orange
2,Melon
3,Apple
```

Please write a file name like a table name.

```console
trdsql "SELECT * FROM test.csv"
```

-q filename can execute SQL from file

```console
trdsql -q test.sql
```

###  4.1. <a name='STDINinput'></a>STDIN input

"-" or "stdin" is received from standard input instead of file name.

```console
ps | trdsql -id " " "SELECT * FROM -"
```

or

```console
ps | trdsql -id " " "SELECT * FROM stdin"
```

###  4.2. <a name='Multiplefiles'></a>Multiple files

Multiple matched files can be executed as one table.

```console
$ trdsql -ih "SELECT * FROM tt*.csv"
1,test1
2,test2
3,test3
```

**Note:** It is not possible to mix different formats (ex: CSV and LTSV).

###  4.3. <a name='Compressedfiles'></a>Compressed files

If the file is compressed with gzip, lz4, bz2, zstd, xz, it will be automatically uncompressed.

```console
trdsql "SELECT * FROM testdata/test.csv.gz"
```

```console
trdsql "SELECT * FROM testdata/test.csv.zst"
```

It is possible to mix uncompressed and compressed files using wildcards.

```console
trdsql "SELECT * FROM testdata/test.csv*"
```

###  4.4. <a name='Outputfile'></a>Output file

`-out filename` option to output the file to a file.

```console
trdsql -out result.csv "SELECT * FROM testdata/test.csv ORDER BY c1"
```

###  4.5. <a name='Outputcompression'></a>Output compression

`-oz compression type` to compress and output.

```console
trdsql -oz gz "SELECT * FROM testdata/test.csv ORDER BY c1" > result.csv.gz
```

###  4.6. <a name='Guessbyoutputfilename'></a>Guess by output file name

The filename of `-out filename` option determines
the output format(csv, ltsv, json, tbln, raw, md, at, vf, jsonl)
 and compression format(gzip, zstd, bz2, lz4, xz) by guess.

Guess by extension output format + output compression
(eg .csv.gz, .ltsv.lz4, .md.zst ...).

The following is an LTSV file compressed in zstd.

```console
trdsql -out result.ltsv.zst "SELECT * FROM testdata/test.csv"
```

###  4.7. <a name='Columnsisnotconstant'></a>Columns is not constant

If the number of columns is not a constant, read and decide multiple rows.

```console
$ trdsql -ir 3 -iltsv "SELECT * FROM test_indefinite.ltsv"
1,Orange,50,,
2,Melon,500,ibaraki,
3,Apple,100,aomori,red
```

###  4.8. <a name='TSVTabSeparatedValue'></a>TSV (Tab Separated Value)

`-id "\\t"` is input from TSV (Tab Separated Value)

```tsv
1	Orange
2	Melon
3	Apple
```

```console
trdsql -id "\t" "SELECT * FROM test-tab.csv"
```

-od "\\t" is TSV (Tab Separated Value) output.

```console
trdsql -od "\t" "SELECT * FROM test.csv"
```

```tsv
1	Orange
2	Melon
3	Apple
```

###  4.9. <a name='LTSVLabeledTab-separatedValues'></a>LTSV (Labeled Tab-separated Values)

`-iltsv` is input from LTSV(Labeled Tab-separated Values).

sample.ltsv

```ltsv
id:1	name:Orange	price:50
id:2	name:Melon	price:500
id:3	name:Apple	price:100
```

```console
$ trdsql -iltsv "SELECT * FROM sample.ltsv"
1,Orange,50
2,Melon,500
3,Apple,100
```

**Note:** Only the columns in the first row are targeted.

-oltsv is LTSV(Labeled Tab-separated Values) output.

```console
$ trdsql -iltsv -oltsv "SELECT * FROM sample.ltsv"
id:1	name:Orange	price:50
id:2	name:Melon	price:500
id:3	name:Apple	price:100
```

###  4.10. <a name='JSON'></a>JSON

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
$ trdsql -ijson "SELECT * FROM sample2.json"
1,Drolet,"{""color"":""burlywood"",""country"":""Maldives""}"
2,Shelly,"{""color"":""plum"",""country"":""Yemen""}"
3,Tuck,"{""color"":""antiquewhite"",""country"":""Mayotte""}"
```

Please use SQL function.

```console
$ trdsql -ijson "SELECT id, name, JSON_EXTRACT(attribute,'$country'), JSON_EXTRACT(attribute,'$color') FROM sample2.json"
1,Drolet,Maldives,burlywood
2,Shelly,Yemen,plum
3,Tuck,Mayotte,antiquewhite
```

Another json format. JSONL(JSON Lines).

sample2.json

```json
{"id": "1","name": "Orange","price": "50"}
{"id": "2","name": "Melon","price": "500"}
{"id": "3","name": "Apple","price": "100"}
```

-ojson is JSON Output.

```console
trdsql -ojson "SELECT * FROM test.csv"
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

###  4.11. <a name='JSONL'></a>JSONL

To output in JSONL, specify -ojsonl.

```console
trdsql -ojsonl "SELECT * FROM test.csv"
```

```json
{"c1":"1","c2":"Orange"}
{"c1":"2","c2":"Melon"}
{"c1":"3","c2":"Apple"}
```

###  4.12. <a name='TBLN'></a>TBLN

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
1,Bob
2,Alice
```

TBLN file reflects extras name and type.

-otbln is TBLN Output.

```console
$ trdsql -otbln "SELECT c1::int as id, c2::text as name FROM test.csv"
; created_at: 2019-03-22T13:20:31+09:00
; name: | id | name |
; type: | int | text |
| 1 | Orange |
| 2 | Melon |
| 3 | Apple |
```

TBLN can contain column names and type definitions.
Please refer to <https://tbln.dev/> for details of TBLN.

###  4.13. <a name='Rawoutput'></a>Raw output

-oraw is Raw Output.
It is used when "escape processing is unnecessary" in CSV output.
(For example, when outputting JSON in the database).

```console
$ trdsql -oraw "SELECT row_to_json(t,TRUE) FROM test.csv AS t"
{"c1":"1",
 "c2":"Orange"}
{"c1":"2",
 "c2":"Melon"}
{"c1":"3",
 "c2":"Apple"}
```

Multiple delimiter characters can be used for raw.

```console
trdsql -oraw -od "\t|\t" -db pdb "SELECT * FROM test.csv"
```

```text
1	|	Orange
2	|	Melon
3	|	Apple
```

###  4.14. <a name='ASCIITableMarkDownoutput'></a>ASCII Table & MarkDown output

-oat is ASCII table output.

```console
$ trdsql -oat "SELECT * FROM test.csv"
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
| C1 |   C2   |
|----|--------|
|  1 | Orange |
|  2 | Melon  |
|  3 | Apple  |
```

###  4.15. <a name='Verticalformatoutput'></a>Vertical format output

-ovf is Vertical format output("column name | value" vertically).

```console
$ trdsql -ovf "SELECT * FROM test.csv"
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

###  4.16. <a name='SQLfunction'></a>SQL function

```console
$ trdsql "SELECT count(*) FROM test.csv"
3
```

The default column names are c1, c2,...

```console
$ trdsql "SELECT c2,c1 FROM test.csv"
Orange,1
Melon,2
Apple,3
```

"- ih" sets the first line to column name

```console
$ ps |trdsql -ih -oh -id " " "SELECT \`PID\`, \`TTY\`, \`TIME\`, \`CMD\` FROM -"
TIME,TTY,PID,CMD
00:00:00,pts/20,3452,ps
00:00:00,pts/20,3453,trdsql
00:00:05,pts/20,15576,zsh
```

###  4.17. <a name='JOIN'></a>JOIN

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

###  4.18. <a name='PostgreSQL'></a>PostgreSQL

When using PostgreSQL, specify postgres for driver
 and connection information for dsn.

```console
trdsql -driver postgres -dsn "dbname=test" "SELECT count(*) FROM test.csv "
```

####  4.18.1. <a name='Function'></a>Function

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

####  4.18.2. <a name='JointableandCSVfileispossible'></a>Join table and CSV file is possible

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
trdsql -driver postgres -dns "dbname=test" "CREATE TABLE fruits (id, name) AS SELECT c1::int, c2 FROM fruits.csv "
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

###  4.19. <a name='MySQL'></a>MySQL

When using MySQL, specify mysql for driver and connection information for dsn.

```console
$ trdsql -driver mysql -dsn "user:password@/test" "SELECT GROUP_CONCAT(c2 ORDER BY c2 DESC) FROM testdata/test.csv"
"g,d,a"
```

```console
$ trdsql -driver mysql -dsn "user:password@/test" "SELECT c1, SHA2(c2,224) FROM test.csv"
1,a063876767f00792bac16d0dac57457fc88863709361a1bb33f13dfb
2,2e7906d37e9523efeefb6fd2bc3be6b3f2991678427bedc296f9ddb6
3,d0b8d1d417a45c7c58202f55cbb617865f1ef72c606f9bce54322802
```

MySQL can join tables and CSV files as well as PostgreSQL.

###  4.20. <a name='Analyze'></a>Analyze

The ***-a filename*** option parses the file and outputs table information and SQL examples.

```console
trdsql -a testdata/test.ltsv
```

```console
The table name is testdata/header.csv.
The file type is CSV.

Data types:
+-------------+------+
| column name | type |
+-------------+------+
| id          | text |
| \`name\`    | text |
+-------------+------+

Data samples:
+----+----------+
| id | \`name\` |
+----+----------+
|  1 | Orange   |
+----+----------+

Examples:
trdsql -db sdb -ih "SELECT id, \`name\` FROM testdata/header.csv"
trdsql -db sdb -ih "SELECT id, \`name\` FROM testdata/header.csv WHERE id = '1'"
trdsql -db sdb -ih "SELECT id, count(id) FROM testdata/header.csv GROUP BY id"
trdsql -db sdb -ih "SELECT id, \`name\` FROM testdata/header.csv ORDER BY id LIMIT 10"
```

Other options(-id,-ih,-ir,-is,icsv,iltsv,-ijson,-itbln...) are available.

```console
trdsql -ih -a testdata/header.csv
```

Similarly, with  ***-A filename*** option, only Examples (SQL) is output.

```console
trdsql -ih -A testdata/header.csv
```

```console
trdsql -ih "SELECT id, \`name\` FROM testdata/header.csv"
trdsql -ih "SELECT id, \`name\` FROM testdata/header.csv WHERE id = '1'"
trdsql -ih "SELECT id, count(id) FROM testdata/header.csv GROUP BY id"
trdsql -ih "SELECT id, \`name\` FROM testdata/header.csv ORDER BY id LIMIT 10"
```

###  4.21. <a name='configuration'></a>configuration

You can specify driver and dsn in the configuration file.

Unix like.

```path
${HOME}/.config/trdsql/config.json
```

Windows (ex).

```path
C:\Users\{"User"}\AppData\Roaming\trdsql\config.json
```

Or use the -config file option.

```console
trdsql -config config.json "SELECT * FROM test.csv"
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

##  5. <a name='Library'></a>Library

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

See also [psutilsql](https://github.com/noborus/psutilsql), which uses trdsql as a library.

##  6. <a name='License'></a>License

MIT

Please check each license of SQL driver.

* [SQLite](https://github.com/mattn/go-sqlite3)
* [MySQL](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://github.com/lib/pq)
