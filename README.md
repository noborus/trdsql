# trdsql

[![Build Status](https://travis-ci.org/noborus/trdsql.svg?branch=master)](https://travis-ci.org/noborus/trdsql)

A tool that can execute SQL queries on csv.
It is a tool like [q](https://github.com/harelba/q) , [textql](https://github.com/dinedal/textql) and others.

The difference from these tools is that the syntax of PostgreSQL or MySQL can be used.

You need to prepare a database server, if you want to use PostgreSQL or MySQL syntax.

## INSTALL

go get -u github.com/noborus/trdsql

## Usage

```
Usage: trdsql [OPTIONS] [SQL(SELECT...)]

Options:
  -db string
    	Specify db name of the setting.
  -dbdriver string
    	database driver. default sqlite3
  -dbdsn string
    	database connection option.
  -debug
    	debug print.
  -id string
    	Field delimiter for input. (default ",")
  -ih
    	The first line is interpreted as column names.
  -od string
    	Field delimiter for output. (default ",")
  -oh
    	Output column name as header.
  -version
    	display version information.
```

## Example

There is a csv file.
```csv
1,Orange
2,Melon
3,Apple
```

You can specify the csv file in the SQL FROM clause.

```sh
$ trdsql "SELECT * FROM test.csv"
```

SQL function

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

Only LEFT JOIN is supported by default (SQLite)
```sh
$ trdsql "SELECT u.c1,u.c2,h.c2 FROM user.csv as u LEFT JOIN hist.csv as h ON(u.c1=h.c1)"
```
```
1,userA,2017-7-10
2,uesrB,2017-7-10
2,uesrB,2017-7-11
```

### PostgreSQL

When using PostgreSQL, specify postgres for dbdriver and connection information for dbdsn.

```
$ trdsql -dbdriver postgres -dbdsn "dbname=test" "SELECT count(*) FROM test.csv "
```

#### Function
The PostgreSQL driver can use the window function.
```
$ trdsql -dbdriver postgres -dbdsn "dbname=test" "SELECT row_number() OVER (ORDER BY c2),c1,c2 FROM test.csv"
```
```
1,3,Apple
2,2,Melon
3,1,Orange
```

You can also use the generate_series function.
```
$ trdsql -dbdriver postgres -dbdsn "dbname=test" "SELECT generate_series(1,3);"
```
```
1
2
3
```
**Note:** Type casting may be necessary in some cases.
```
$ trdsql -dbdriver postgres -dbdsn "dbname=test" "SELECT generate_series('2017-07-18 00:00'::timestamp,'2017-07-20 00:00'::timestamp, '24 hours')::text;"
```
```
2017-07-18 00:00:00
2017-07-19 00:00:00
2017-07-20 00:00:00
```
#### Join table and csv file is possible.

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

Join table and csv file.

```
$ trdsql -dbdriver postgres -dbdsn "dbname=test" "SELECT t.c1,t.c2,c.name FROM test.csv AS t LEFT JOIN colors AS c ON (t.c1::int = c.id)"
```

```
1,Orange,orange
2,Melon,green
3,Apple,red
```


### MySQL

When using MySQL, specify mysql for dbdriver and connection information for dbdsn.

```
$ trdsql -dbdriver mysql -dbdsn "user:password@/test" "SELECT c1, SHA2(c2,224) FROM test.csv"
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

If you put the setting in [config.json](config.json.sample)  you can specify the name with -db.

```
$ trdsql --debug -db pdb "SELECT * FROM test.csv"
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
