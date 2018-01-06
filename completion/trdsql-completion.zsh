#compdef trdsql

function _trdsql {
  local context curcontext=$curcontext state line
  _arguments -C \
    '-help[display usage information.]' \
    '-version[display version information.]' \
    '-config[Configuration file location.]:file:_files' \
    '-db[Specify db name of the setting.]:db specify:__trdsql_db' \
    '-dblist[display db information.]' \
    '-debug[debug print.]' \
    '-driver[database driver.]:driver specify:__trdsql_driver' \
    '-dsn[database connection option.]' \
    '-id[Field delimiter for input. (default ",")]::' \
    '-ih[The first line is interpreted as column names.]' \
    '(-icsv -iltsv -ijson)-ig[Guess format from extension.]' \
    '(-ig -iltsv -ijson)-icsv[CSV format for input.]' \
    '(-ig -icsv -ijson)-iltsv[LTSV format for input.]' \
    '(-ig -icsv -iltsv)-ijson[JSON format for input.]' \
    '-is[Skip header row.]::' \
    '-od[Field delimiter for output. (default ",")]' \
    '-oh[Output column name as header.]' \
    '-oat[ASCII Table format for output.]' \
    '-ojson[JSON format for output.]' \
    '-oltsv[LTSV format for output.]' \
    '-omd[Mark Down format for output.]' \
    '-oraw[Raw format for output.]' \
    '-ovf[Vertical format for output.]' \
    '-q[Read query from the provided filename.]:file:_files -g "*.(SQL|sql)"' \
    '1: :__trdsql_sql' \
    '*:file:_files -g "*.(csv|CSV|ltsv|LTSV|json|JSON)"'
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
