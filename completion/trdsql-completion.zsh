#compdef trdsql

function _trdsql {
  local context curcontext=$curcontext state line
  _arguments -C \
    '-help[display usage information.]' \
    '-version[display version information.]' \
    '-config[configuration file location.]:file:_files' \
    '-db[specify db name of the setting.]:db specify:__trdsql_db' \
    '-dblist[display db information.]' \
    '-debug[debug print.]' \
    '-driver[database driver.]:driver specify:__trdsql_driver' \
    '-dsn[database connection option.]' \
    '-id[field delimiter for input. (default ",")]::' \
    '-ih[the first line is interpreted as column names.]' \
    '(-icsv -iltsv -ijson -itbln)-ig[guess format from extension.]' \
    '(-ig -iltsv -ijson -itbln)-icsv[CSV format for input.]' \
    '(-ig -icsv -ijson -itbln)-iltsv[LTSV format for input.]' \
    '(-ig -icsv -iltsv -itbln)-ijson[JSON format for input.]' \
    '(-ig -icsv -iltsv -ijson)-itbln[TBLN format for input.]' \
    '-is[skip header row.]::' \
    '-ir[umber of row preread for column determination.(default 1)]::' \
    '-out[output file name.]' \
    '-out-without-guess[output without guessing (when using -out).]' \
    '-od[field delimiter for output. (default ",")]' \
    '-oq[quote character for output. (default "\"")]' \
    '-oaq[enclose all fields in quotes for output.]' \
    '-ocrlf[use CRLF for output. End each output line with '\\r\\n' instead of '\\n'.]' \
    '-oh[output column name as header.]' \
    '-oat[ASCII Table format for output.]' \
    '-ojson[JSON format for output.]' \
    '-ojsonl[JSONL format for output.]' \
    '-oltsv[LTSV format for output.]' \
    '-omd[Markdown format for output.]' \
    '-oraw[Raw format for output.]' \
    '-ovf[Vertical format for output.]' \
    '-oz[output compression format.]:compression:_values "" "gz" "bz2" "zst" "lz4" "xz"' \
    '-a[analyze the file and suggest SQL.]:file:_files' \
    '-A[analyze the file but only suggest SQL.]:file:_files' \
    '-q[read query from the provided filename.]:file:_files -g "*.(SQL|sql)"' \
    '1: :__trdsql_sql' \
    '*:file:_files -g "*"'
}

__trdsql_sql() {
  local -a _sql
  _sql=(
        'SELECT c1 FROM'
        'SELECT * FROM'
        'SELECT count(*) FROM'
  )
  _describe -t commands Commands _sql "$@"
}

__trdsql_db() {
  _dblist=( $(trdsql -dblist) )
  _describe -t dblist DBList _dblist
}

__trdsql_driver() {
  local -a _driver
  _driver=(
    'mysql'
    'postgres'
    'sqlite3'
  )
  _describe -t driver DBDriver _driver
}

_trdsql "$@"
