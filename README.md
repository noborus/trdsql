# trdsql
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
1,a,b,c
2,d,e,f
3,g,h,i
```

You can specify the csv file in the SQL FROM clause.

```sh
$ trdsql "SELECT * FROM test.csv"
```
```
1,a,b,c
2,d,e,f
3,g,h,i
```

SQL function

```sh
$ trdsql "SELECT count(*) FROM test.csv"
```
```
3
```

The default column names are c1, c2, c3 ...
```sh
$ trdsql "SELECT c2,c4 FROM test.csv"
```
```
a,c
d,f
g,i
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
$ trdsql -dbdriver postgres -dbdsn "dbname=orca" "SELECT count(*) FROM test.csv "
```

```
3
```


### MySQL

### configuration

## License

MIT

However, SQL driver has a different license.

* [SQLite](https://github.com/mattn/go-sqlite3)
* [MySQL](https://github.com/go-sql-driver/mysql)
* [PostgreSQL](https://github.com/lib/pq)
