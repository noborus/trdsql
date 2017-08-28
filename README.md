# trdsql

[![Build Status](https://travis-ci.org/noborus/trdsql.svg?branch=master)](https://travis-ci.org/noborus/trdsql)

A tool that can execute SQL queries on CSV and  [LTSV](http://ltsv.org/).

It is a tool like [q](https://github.com/harelba/q) , [textql](https://github.com/dinedal/textql) and others.

The difference from these tools is that the syntax of PostgreSQL or MySQL can be used.

You need to prepare a database server if you want to use PostgreSQL or MySQL syntax.

## INSTALL

```
go get -u github.com/noborus/trdsql
```
Or download binaries from the [releases](https://github.com/noborus/trdsql/releases) page(Linux/Windows/macOS).

## Usage

```
Options:
  -config string
      Configuration file location.
  -db string
    	Specify db name of the setting.
  -debug
    	debug print.
  -driver string
    	database driver.  [ mysql | postgres | sqlite3 ]
  -dsn string
    	database connection option.
  -help
    	display usage information.
  -id string
    	Field delimiter for input. (default ",")
  -ig
    	Guess format from extension.
  -ih
    	The first line is interpreted as column names(CSV only).
  -iltsv
    	LTSV format for input.
  -is int
    	Skip header row.
  -oat
    	ASCII Table format for output.
  -ocsv
    	CSV format for output. (default true)
  -od string
    	Field delimiter for output. (default ",")
  -oh
    	Output column name as header.
  -ojson
    	JSON format for output.
  -oltsv
    	LTSV format for output.
  -omd
    	Mark Down format for output.
  -oraw
    	Raw format for output.
  -ovf
    	Vertical format for output.
  -q string
    	Read query from the provided filename.
  -version
    	display version information.
```

## Example

test.csv file.

```csv
1,Orange
2,Melon
3,Apple
```

You can specify the CSV file in the SQL FROM clause.

```sh
$ trdsql "SELECT * FROM test.csv"
```

You can also save the SQL in a file and specify the file.
The default is CSV, so you can omit -ocsv.

```sh
$ trdsql -q test.sql
```

### TSV

For a TAB delimited CSV file(TSV), specify option -id "\\t"

```
1	Orange
2	Melon
3	Apple
```

```sh
$ trdsql -id "\t" "SELECT * FROM test-tab.csv"
```

If you want to use it for output, specify -od "\\t".

```sh
$ trdsql -od "\t" "SELECT * FROM test.csv"
```
```
1	Orange
2	Melon
3	Apple
```

### LTSV

For LTSV files, specify option -iltsv.

sample.ltsv
```
id:1	name:Orange	price:50
id:2	name:Melon	price:500
id:3	name:Apple	price:100
```

```sh
$ trdsql -iltsv "SELECT * FROM sample.ltsv"
```

```
1,Orange,50
2,Melon,500
3,Apple,100
```

**Note:** Only the columns in the first row are targeted.

If you want to use it for output, specify -oltsv.

```sh
$ trdsql -iltsv -oltsv "SELECT * FROM sample.ltsv"
```

```
id:1	name:Orange	price:50
id:2	name:Melon	price:500
name:Apple	price:100	id:3
```

**Note:** LTSV does not keep the output order of the columns

### JSON (output only)

Output JSON with option -ojson.

```sh
$ trdsql -ojson "SELECT * FROM test.csv"
```
```
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

### Raw output

Output Raw with option -oraw.
It is used when "escape processing is unnecessary" in CSV output.
(For example, when outputting JSON in the database).

```sh
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


### ASCII Table & MarkDown (output only)

You can output ASCII table using [tablewriter](https://github.com/olekukonko/tablewriter).

```sh
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

You can also output Markdown.

```sh
$ trdsql -omd "SELECT * FROM test.csv"
```
```
| C1 |   C2   |
|----|--------|
|  1 | Orange |
|  2 | Melon  |
|  3 | Apple  |
```

### Vertical format

Vertical format outputs "column name | value" vertically

```
$ trdsql -ovf "SELECT * FROM test.csv"
```
```
---[ 1]----------------------------------------------------------------
  c1 | 1
  c2 | Orange
---[ 2]----------------------------------------------------------------
  c1 | 2
  c2 | Melon
---[ 3]----------------------------------------------------------------
  c1 | 3
  c2 | Apple
```

### SQL function

```sh
$ trdsql "SELECT count(*) FROM test.csv"
```
```
3
```

The default column names are c1, c2,...
```sh
$ trdsql "SELECT c2,c1 FROM test.csv"
```
```
Orange,1
Melon,2
Apple,3
```

You can specify "-" can be specified for stdin.
```sh
$ ps | trdsql -id " " "SELECT * FROM -"
```
```
PID,TTY,TIME,CMD
3073,pts/22,00:00:00,zsh
17815,pts/22,00:00:00,ps
17816,pts/22,00:00:00,trdsql
```

### JOIN

You can also JOIN.

user.csv
```
1,userA
2,uesrB
```

hist.csv
```
1,2017-7-10
2,2017-7-10
2,2017-7-11
```

```sh
$ trdsql "SELECT u.c1,u.c2,h.c2 FROM user.csv as u LEFT JOIN hist.csv as h ON(u.c1=h.c1)"
```
```
1,userA,2017-7-10
2,uesrB,2017-7-10
2,uesrB,2017-7-11
```

### PostgreSQL

When using PostgreSQL, specify postgres for driver and connection information for dsn.

```sh
$ trdsql -driver postgres -dsn "dbname=test" "SELECT count(*) FROM test.csv "
```

#### Function
The PostgreSQL driver can use the window function.
```sh
$ trdsql -driver postgres -dsn "dbname=test" "SELECT row_number() OVER (ORDER BY c2),c1,c2 FROM test.csv"
```
```
1,3,Apple
2,2,Melon
3,1,Orange
```

You can also use the generate_series function.
```sh
$ trdsql -driver postgres -dsn "dbname=test" "SELECT generate_series(1,3);"
```
```
1
2
3
```

#### Join table and CSV file is possible.

Test database has a colors table.
```
$ psql test -c "SELECT * FROM colors"
```
```
 id |  name  
----+--------
  1 | orange
  2 | green
  3 | red
(3 rows)
```

Join table and CSV file.

```sh
$ trdsql -driver postgres -dsn "dbname=test" "SELECT t.c1,t.c2,c.name FROM test.csv AS t LEFT JOIN colors AS c ON (t.c1::int = c.id)"
```

```
1,Orange,orange
2,Melon,green
3,Apple,red
```

You can also use "CREATE TABLE ... AS SELECT...".

```sh
$ trdsql -driver postgres -dns "dbname=test" "CREATE TABLE fruits (id, name) AS SELECT c1::int, c2 FROM fruits.csv "
```

```sh
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

```sh
$ trdsql -driver mysql -dsn "user:password@/test" "SELECT GROUP_CONCAT(c2 ORDER BY c2 DESC) FROM testdata/test.csv"
```

```
"g,d,a"
```

```sh
$ trdsql -driver mysql -dsn "user:password@/test" "SELECT c1, SHA2(c2,224) FROM test.csv"
```

```
1,a063876767f00792bac16d0dac57457fc88863709361a1bb33f13dfb
2,2e7906d37e9523efeefb6fd2bc3be6b3f2991678427bedc296f9ddb6
3,d0b8d1d417a45c7c58202f55cbb617865f1ef72c606f9bce54322802
```

MySQL can join tables and CSV files as well as PostgreSQL.

### configuration

You can specify driver and dsn in the configuration file.

Unix like.
```
$HOME/.config/trdsql/config.json

```
Windows (ex).
```
C:\Users\{"User"}\AppData\Roaming\trdsql\config.json
```

Or you can specify the file with the -config option/

```
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

```sh
$ trdsql -debug -db pdb "SELECT * FROM test.csv"
```
```
2017/07/18 02:27:47 driver: postgres, dsn: user=test dbname=test
2017/07/18 02:27:47 CREATE TEMPORARY TABLE "test.csv" ( c1 text,c2 text );
2017/07/18 02:27:47 INSERT INTO "test.csv" (c1,c2) VALUES ($1,$2);
2017/07/18 02:27:47 SELECT * FROM "test.csv"
1,Orange
2,Melon
3,Apple
```

## License

MIT

Please check each license of SQL driver.
* [SQLite](https://github.com/mattn/go-sqlite3)
* [MySQL](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://github.com/lib/pq)
