module github.com/noborus/trdsql

require (
	github.com/go-sql-driver/mysql v1.4.1
	github.com/lib/pq v1.2.0
	github.com/logrusorgru/aurora v0.0.0-20191116043053-66b7ad493a23
	github.com/mattn/go-runewidth v0.0.6
	github.com/mattn/go-sqlite3 v1.12.0
	github.com/noborus/tbln v0.0.1
	github.com/olekukonko/tablewriter v0.0.2
	golang.org/x/crypto v0.0.0-20191112222119-e1110fd1c708
	golang.org/x/sys v0.0.0-20191115151921-52ab43148777 // indirect
	google.golang.org/appengine v1.6.5 // indirect
)

go 1.13

exclude github.com/mattn/go-sqlite3 v2.0.0+incompatible
