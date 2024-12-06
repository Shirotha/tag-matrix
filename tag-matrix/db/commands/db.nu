use ../naming.nu *
use ../schema.nu

# database related commands
export def main [
]: record<sql: record> -> record {
  plugin use schema
  let sql_generic = $in.sql.generic
  let sql_insert = $in.sql.insert
  let sql_update = $in.sql.update
  let sql_upsert = $in.sql.upsert
  let sql_delete = $in.sql.delete
  def sql-run [
    query: string
    --params (-p): any
    --check-empty (-e): string
    --span (-s): any
  ]: nothing -> any {
    let result = do $sql_generic.action ({
      query: $query
      params: $params
    } | normalize $sql_generic.args)
    if ($check_empty | is-not-empty) and ($result | is-empty) {
      error make {
        msg: 'invalid operation'
        label: {
          text: $check_empty
          span: ($span | default (metadata $query).span)
        }
      }
    }
    $result
  }
  def sql-insert [
    table: string
    rows: any
    --allow-conflicts (-c)
  ]: nothing -> any {
    do $sql_insert.action ({
      table: $table
      rows: $rows
      allow_conflicts: $allow_conflicts
    } | normalize $sql_insert.args)
  }
  def sql-update [
    table: string
    where: string
    values: record
    --params (-p): any
    --single (-s)
    --check-empty (-e): string
  ]: nothing -> any {
    let result = do $sql_update.action ({
      table: $table
      where: $where
      params: $params
      values: $values
      single: $single
    } | normalize $sql_update.args)
    if ($check_empty | is-not-empty) and ($result | is-empty) {
      error make {
        msg: 'invalid operation'
        label: {
          text: $check_empty
          span: (metadata $where).span
        }
      }
    }
    $result
  }
  def sql-upsert [
    table: string
    rows: any
    --conflict (-c): string
  ]: nothing -> any {
    do $sql_upsert.action ({
      table: $table
      rows: $rows
      conflict: ($conflict | default ($rows | columns | first))
    } | normalize $sql_insert.args)
  }
  def sql-delete [
    table: string
    where: string
    --params (-p): any
    --single (-s)
    --check-empty (-e): string
  ]: nothing -> any {
    let result = do $sql_delete.action ({
      table: $table
      where: $where
      params: $params
      single: $single
    } | normalize $sql_delete.args)
    if ($check_empty | is-not-empty) and ($result | is-empty) {
      error make {
        msg: 'invalid operation'
        label: {
          text: $check_empty
          span: (metadata $where).span
        }
      }
    }
    $result
  }
  def sql-exists [
    table: string
    where: string
    --params (-p): any
    --check-missing (-m): string
    --span (-s): any
  ]: nothing -> bool {
    let where = if ($where | is-not-empty) {
      $'WHERE ($where)'
    } else { '' }
    let result = sql-run -p $params $"
      SELECT EXISTS\(
        SELECT 1 FROM ($table)
        ($where)
      \) AS result
    "
    if ($check_missing | is-not-empty) and $result.0.result == 0 {
      error make {
        msg: 'invalid operation'
        label: {
          text: $'($check_missing) does not exist'
          span: ($span | default (metadata $table).span)
        }
      }
    }
    $result.0.result != 0
  }
  def 'data where' [
    column: closure
    data: list
  ]: [string -> string, nothing -> string] {
    if ($in | is-empty) { [] } else { [$in] }
    | $in ++ (0..<($data | length) | each { $'($in | do $column) == ?' })
    | str join ' AND '
  }
  def 'data merge' [
    column: closure
    data: list
  ]: [record -> record, nothing -> record] {
    mut row = $in | default {}
    for it in ($data | enumerate) {
      $row = $row | insert ($it.index | do $column) $it.item
    }
    $row
  }
  def get-source [
    info: any # assumed to match against `schema source`
  ]: nothing -> int {
    if ($info | describe -d).type == string {
      let result = sql-run -p [$info] $"
        SELECT (primary-key) AS id
        FROM (table sources)
        WHERE (column sources value) == ?
      " -e 'source not found' -s (metadata $info).span
      $result.0.id
    } else {
      (sql-exists
        (table sources)
        $'(primary-key) == ?'
        -p [$info]
        -m source -s (metadata $info).span
      )
      $info
    }
  }
  def get-entity [
    type: string
    info: any # assumed to match against `schema entity`
    --schema: any # pass cached copy of type->schema (decoded)
    --get-schema # also retrive schema (decoded)
  ]: nothing -> any {
    mut schema = $schema
    let id = if ($info | describe -d).type == int {
      if ($schema | is-empty) {
        if $get_schema {
          let result = sql-run -p [$type] $"
            SELECT (column entity-types schema) AS schema
            FROM (table entity-types)
            WHERE (column entity-types name) == ?
          " -e 'entity type does not exist' -s (metadata $type).span
          $schema = $result.0.schema | from msgpackz | schema
        } else {
          (sql-exists
            (table entity-types)
            $'(column entity-types name) == ?'
            -p [$type]
            -m 'entity type' -s (metadata $type).span
          )
        }
      }
      (sql-exists
        (table entity $type)
        $'(primary-key) == ?'
        -p [$info]
        -m entity -s (metadata $info).span
      )
      $info
    } else {
      $schema = if ($schema | is-empty) {
        let result = sql-run -p [$type] $"
          SELECT (column entity-types schema) AS schema
          FROM (table entity-types)
          WHERE (column entity-types name) == ?
        " -e 'entity type does not exist' -s (metadata $type).span
        $result.0.schema | from msgpackz | schema
      } else { $schema }
      let data = $info.data | normalize $schema
      let source = get-source $info.source
      let result = sql-run -p [$source ...$data] $"
        SELECT (primary-key) AS id
        FROM (table entity $type)
        WHERE ($'(column entity source) == ?' | data where {column entity data $in} $data)
      " -e 'entity not found' -s (metadata $info).span
      $result.0.id
    }
    if $get_schema {
      {id: $id, schema: $schema}
    } else { $id }
  }
  def get-attribute [
    info: any # assumed to match against `schema attribute`
    --get-schema # also retrieve schema (decoded)
  ]: nothing -> any {
    let key = if ($info | describe -d).type == string {
      column attributes name
    } else {
      primary-key
    }
    let schema = if $get_schema {
      $', (column attributes schema) AS schema'
    } else { '' }
    let result = sql-run -p [$info] $"
      SELECT (primary-key) AS id, (column attributes type) AS type ($schema)
      FROM (table attributes)
      WHERE ($key) == ?
    " -e 'attribute not found' -s (metadata $info).span
    if $get_schema {
      $result.0 | update schema { from msgpackz | schema }
    } else {
      $result.0
    }
  }
  # get all entity type names
  def get-entity-types [
    --for (-f): string # only types that have mappings to single attribute type
    --type (-t): string # filter to single entity type
  ] {
    if ($type | is-empty) {
      let result = if ($for | is-empty) {
        sql-run $"
          SELECT (column entity-types name) AS types
          FROM (table entity-types)
        "
      } else {
        sql-run -p [$for] $"
          SELECT (column type-map entity from) AS types
          FROM (table type-map entity)
          WHERE (column type-map entity to) == ?
        "
      }
      $result.types
    } else {
      (sql-exists
        (table entity-types)
        $'(column entity-types name) == ?'
        -p [$type]
        -m 'entity type' -s (metadata $type).span
      )
      if ($for | is-not-empty) {
        let result = (sql-exists
          (table type-map entity)
          $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
          -p [$type, $for]
        )
        if not $result { return [] }
      }
      $type
    }
  }
  # get all attribute type names
  def get-attribute-types [
    --for (-f): string # only types that have mappings to single attribute type
    --type (-t): string # filter to single attribute type
  ] {
    if ($type | is-empty) {
      let result = if ($for | is-empty) {
        sql-run $"
          SELECT (column attribute-types name) AS types
          FROM (table attribute-types)
        "
      } else {
        sql-run -p [$for] $"
          SELECT (column type-map attribute from) AS types
          FROM (table type-map attribute)
          WHERE (column type-map attribute to) == ?
        "
      }
      $result.types
    } else {
      (sql-exists
        (table attribute-types)
        $'(column attribute-types name) == ?'
        -p [$type]
        -m 'attribute type' -s (metadata $type).span
      )
      if ($for | is-not-empty) {
        let result = (sql-exists
          (table type-map attribute)
          $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
          -p [$type, $for]
        )
        if not $result { return [] }
      }
      $type
    }
  }
  # get info for all tables mapping a specific attribute type
  def get-attribute-mappings [
    type: string
  ] {
    let result = sql-run -p [$type] $"
      SELECT (column type-map entity from) AS from_
      FROM (table type-map entity)
      WHERE (column type-map entity to) == ?
    "
    mut map_data = []
    $map_data ++= $result | each {{
      type: entity
      entity: $in.from_
      table: (table map entity $in.from_ $type)
      view: (view map entity $in.from_ $type)
      from: (column map entity from)
      to: (column map entity to)
      data: {column map entity data $in}
    }}
    let result = sql-run -p [$type] $"
      SELECT (column type-map attribute from) AS from_
      FROM (table type-map attribute)
      WHERE (column type-map attribute to) == ?
    "
    $map_data ++= $result | each {{
      type: attribute
      attribute: $in.from_
      table: (table map attribute $in.from_ $type)
      view: (view map attribute $in.from_ $type)
      from: (column map attribute from)
      to: (column map attribute to)
      data: {column map atribute data $in}
    }}
    let result = (sql-exists
      (table type-map null)
      $'(column type-map null to) == ?'
      -p [$type]
    )
    if $result {
      $map_data ++= {
        type: 'null'
        table: (table map null $type)
        view: (view map null $type)
        to: (column map null to)
        data: {column map null data $in}
      }
    }
    $map_data
  }
  def update-timings [] {
    update (created-at) { into datetime }
    | update (modified-at) { into datetime }
  }
  def update-entity-type [] {
    update (column entity-types schema) { from msgpackz | schema }
  }
  def update-attribute-type [] {
    update (column attribute-types unique) { $in != 0 }
  }
  def update-source [] { update-timings }
  def update-entity [] { update-timings }
  def update-attribute [
    --view (-v)
  ] {
    let result = $in | update (column attributes schema) { from msgpackz | schema }
    | update-timings
    if $view {
      $result | update (column attribute-types unique) { $in != 0 }
    } else { $result }
  }
  def update-map [] { update-timings }
  def timing-columns [] {
    # NOTE: store date using `$date | format date "%+"` (sql query param will implicit cast)
    # NOTE: load date using `$str | into datetime`
    $"
      (created-at) TEXT NOT NULL DEFAULT\(datetime\('now'\) || 'Z'\),
      (modified-at) TEXT NOT NULL DEFAULT\(datetime\('now'\) || 'Z'\)
    "
  }
  def create-entity-table [
    name: string
    columns: int
  ] {
    mut unique = $"UNIQUE\((column entity source)"
    mut data_columns = ''
    for i in 0..<($columns) {
      $unique += $", (column entity data $i)"
      $data_columns += $"(column entity data $i) BLOB NOT NULL,\n"
    }
    $unique += ')'
    sql-run $"CREATE TABLE (table entity $name) \(
      (primary-key) INTEGER NOT NULL PRIMARY KEY,
      (column entity source) INTEGER NOT NULL,
      ($data_columns)
      (timing-columns),
      FOREIGN KEY \((column entity source)\)
        REFERENCES (table sources) \((primary-key)\)
          ON UPDATE CASCADE
          ON DELETE CASCADE,
      ($unique)
    \)"
    sql-run $"CREATE INDEX (index (table entity $name) (column entity source))
      ON (table entity $name)\((column entity source)\)
    "
    sql-run $"
      CREATE TRIGGER (trigger modified (table entity $name))
      AFTER UPDATE
      ON (table entity $name)
      BEGIN
        UPDATE (table entity $name)
        SET (modified-at) = datetime\('now'\) || 'Z'
        WHERE (primary-key) == NEW.(primary-key);
      END
    "
    let view_columns = 0..<($columns) | each { $"e.(column entity data $in),\n" } | str join
    sql-run $"CREATE VIEW (view entity $name)
      AS SELECT
        e.(primary-key),
        e.(column entity source) AS (namespaced (column entity source) (primary-key)),
        s.(column sources value) AS (column entity source),
        ($columns) AS (column entity-types columns),
        ($view_columns)
        e.(created-at),
        e.(modified-at)
      FROM (table entity $name) e
      LEFT JOIN (table sources) s ON e.(column entity source) == s.(primary-key)
    "
  }
  def create-entity-map [
    from: string
    to: string
    columns: int
    unique: bool
  ] {
    let unique_check = if $unique {
      $",\nUNIQUE\((column map entity from), (column map entity to)\)"
    } else { "" }
    mut data_columns = ""
    for i in 0..<($columns) {
      $data_columns += $"(column map entity data $i) BLOB NOT NULL,\n"
    }
    sql-run $"CREATE TABLE (table map entity $from $to) \(
      (primary-key) INTEGER NOT NULL PRIMARY KEY,
      (column map entity from) INTEGER NOT NULL,
      (column map entity to) INTEGER NOT NULL,
      ($data_columns)
      (timing-columns),
      FOREIGN KEY \((column map entity from)\)
        REFERENCES (table entity $from) \((primary-key)\)
          ON UPDATE CASCADE
          ON DELETE CASCADE,
      FOREIGN KEY \((column map entity to)\)
        REFERENCES (table attributes) \((primary-key)\)
          ON UPDATE CASCADE
          ON DELETE CASCADE
      ($unique_check)
    \)"
    if not $unique {
      sql-run $"CREATE INDEX (index map entity pair $from $to)
        ON (table map entity $from $to)\((column map entity from), (column map entity to)\)
      "
    }
    sql-run $"CREATE INDEX (index (table map entity $from $to) (column map entity from))
      ON (table map entity $from $to)\((column map entity from)\)
    "
    sql-run $"CREATE INDEX (index (table map entity $from $to) (column map entity to))
      ON (table map entity $from $to)\((column map entity to)\)
    "
    sql-run $"
      CREATE TRIGGER (trigger modified (table map entity $from $to))
      AFTER UPDATE
      ON (table map entity $from $to)
      BEGIN
        UPDATE (table map entity $from $to)
        SET (modified-at) = datetime\('now'\) || 'Z'
        WHERE (primary-key) == NEW.(primary-key);
      END
    "
    let view_columns = 0..<($columns) | each { $"m.(column map entity data $in),\n" } | str join
    let result = sql-run -p [$from] $"
      SELECT (column entity-types columns) AS columns
      FROM (table entity-types)
      WHERE (column entity-types name) == ?
    "
    let source_columns = 0..<($result.0.columns) | each { $"e.(column entity data $in),\n" } | str join
    sql-run $"CREATE VIEW (view map entity $from $to)
      AS SELECT
        m.(primary-key),
        e.(primary-key) AS (namespaced (column map entity from) (primary-key)),
        s.(primary-key) AS (namespaced (column entity source) (primary-key)),
        s.(column sources value) AS (column entity source),
        ($result.0.columns) AS (column entity-types columns),
        ($source_columns)
        a.(primary-key) AS (namespaced (column map entity to) (primary-key)),
        a.(column attributes name) AS (column map entity to),
        ($columns) AS (column attribute-types columns),
        ($view_columns)
        m.(created-at),
        m.(modified-at)
      FROM (table map entity $from $to) m
      LEFT JOIN (table entity $from) e ON m.(column map entity from) == e.(primary-key)
      LEFT JOIN (table sources) s ON e.(column entity source) == s.(primary-key)
      LEFT JOIN (table attributes) a ON m.(column map entity to) == a.(primary-key)
    "
  }
  def create-attribute-map [
    from: string
    to: string
    columns: int
    unique: bool
  ] {
    let unique_check = if $unique {
      $",\nUNIQUE\((column map attribute from), (column map attribute to)\)"
    } else { "" }
    mut data_columns = ""
    for i in 0..<($columns) {
      $data_columns += $"(column map attribute data $i) BLOB NOT NULL,\n"
    }
    sql-run $"CREATE TABLE (table map attribute $from $to) \(
      (primary-key) INTEGER NOT NULL PRIMARY KEY,
      (column map attribute from) INTEGER NOT NULL,
      (column map attribute to) INTEGER NOT NULL,
      ($data_columns)
      (timing-columns),
      FOREIGN KEY \((column map attribute from)\)
        REFERENCES (table attributes) \((primary-key)\)
          ON UPDATE CASCADE
          ON DELETE CASCADE,
      FOREIGN KEY \((column map attribute to)\)
        REFERENCES (table attributes) \((primary-key)\)
          ON UPDATE CASCADE
          ON DELETE CASCADE
      ($unique_check)
    \)"
    if not $unique {
      sql-run $"CREATE INDEX (index map attribute pair $from $to)
        ON (table map attribute $from $to)\((column map attribute from), (column map attribute to)\)
      "
    }
    sql-run $"CREATE INDEX (index (table map attribute $from $to) (column map attribute from))
      ON (table map attribute $from $to)\((column map attribute from)\)
    "
    sql-run $"CREATE INDEX (index (table map attribute $from $to) (column map attribute to))
      ON (table map attribute $from $to)\((column map attribute to)\)
    "
    sql-run $"
      CREATE TRIGGER (trigger modified (table map attribute $from $to))
      AFTER UPDATE
      ON (table map attribute $from $to)
      BEGIN
        UPDATE (table map attribute $from $to)
        SET (modified-at) = datetime\('now'\) || 'Z'
        WHERE (primary-key) == NEW.(primary-key);
      END
    "
    let view_columns = 0..<($columns) | each { $"m.(column map attribute data $in),\n" } | str join
    sql-run $"CREATE VIEW (view map attribute $from $to)
      AS SELECT
        m.(primary-key),
        f.(primary-key) AS (namespaced (column map attribute from) (primary-key)),
        f.(column attributes name) AS (column map attribute from),
        t.(primary-key) AS (namespaced (column map attribute to) (primary-key)),
        t.(column attributes name) AS (column map attribute to),
        ($columns) AS (column attribute-types columns),
        ($view_columns)
        m.(created-at),
        m.(modified-at)
      FROM (table map attribute $from $to) m
      LEFT JOIN (table attributes) f ON m.(column map attribute from) == f.(primary-key)
      LEFT JOIN (table attributes) t ON m.(column map attribute to) == t.(primary-key)
    "
  }
  def create-null-map [
    to: string
    columns: int
    unique: bool
  ] {
    let unique_check = if $unique { 'UNIQUE'  } else { '' }
    mut data_columns = ""
    for i in 0..<($columns) {
      $data_columns += $"(column map null data $i) BLOB NOT NULL,\n"
    }
    sql-run $"CREATE TABLE (table map null $to) \(
      (primary-key) INTEGER NOT NULL PRIMARY KEY,
      (column map null to) INTEGER NOT NULL ($unique_check),
      ($data_columns)
      (timing-columns),
      FOREIGN KEY \((column map null to)\)
        REFERENCES (table attributes) \((primary-key)\)
          ON UPDATE CASCADE
          ON DELETE CASCADE
    \)"
    if not $unique {
      sql-run $"CREATE INDEX (index (table map null $to) (column map null to))
        ON (table map null $to)\((column map null to)\)
      "
    }
    sql-run $"
      CREATE TRIGGER (trigger modified (table map null $to))
      AFTER UPDATE
      ON (table map null $to)
      BEGIN
        UPDATE (table map null $to)
        SET (modified-at) = datetime\('now'\) || 'Z'
        WHERE (primary-key) == NEW.(primary-key);
      END
    "
    let view_columns = 0..<($columns) | each { $"m.(column map null data $in),\n" } | str join
    sql-run $"CREATE VIEW (view map null $to)
      AS SELECT
        m.(primary-key),
        a.(primary-key) AS (namespaced (column map null to) (primary-key)),
        a.(column attributes name) AS (column map null to),
        ($columns) AS (column attribute-types columns),
        ($view_columns)
        m.(created-at),
        m.(modified-at)
      FROM (table map null $to) m
      LEFT JOIN (table attributes) a ON m.(column map null to) == a.(primary-key)
    "
  }
  $in | merge {db: {
    init: {
      args: ({} | schema struct --wrap-null)
      action: {|cmd|
        sql-run 'PRAGMA foreign_keys = ON'
        sql-run 'PRAGMA recursive_triggers = OFF'
        sql-run $"CREATE TABLE (table version) \(
          (primary-key) INTEGER PRIMARY KEY CHECK\((primary-key) == 42\),
          (column version program) TEXT NOT NULL,
          (column version data) TEXT NOT NULL,
          (column version config) TEXT NOT NULL
        \)"
        let versions = {id: 42,
          # TODO: don't hardcode versions
          (column version program): '0.1'
          (column version data): '1.0'
          (column version config): '0.1'
        }
        sql-insert (table version) $versions
        sql-run $"CREATE TABLE (table entity-types) \(
          (column entity-types name) TEXT NOT NULL PRIMARY KEY,
          (column entity-types columns) UNSIGNED_INTEGER NOT NULL CHECK\((column entity-types columns) >= 0\),
          (column entity-types schema) BLOB NOT NULL
        \)"
        sql-run $"CREATE TABLE (table attribute-types) \(
          (column attribute-types name) TEXT NOT NULL PRIMARY KEY,
          (column attribute-types unique) BOOL NOT NULL CHECK\((column attribute-types unique) IN \(0, 1\)\),
          (column attribute-types columns) UNSIGNED_INTEGER NOT NULL CHECK\((column attribute-types columns) >= 0\)
        \)"
        sql-run $"CREATE TABLE (table attributes) \(
          (primary-key) INTEGER NOT NULL PRIMARY KEY,
          (column attributes name) TEXT NOT NULL UNIQUE,
          (column attributes type) TEXT NOT NULL,
          (column attributes schema) BLOB NOT NULL,
          (timing-columns),
          FOREIGN KEY \((column attributes type)\)
            REFERENCES (table attribute-types) \((column attribute-types name)\)
              ON UPDATE CASCADE
              ON DELETE CASCADE
        \)"
        sql-run $"CREATE INDEX (index (table attributes) (column attributes type))
          ON (table attributes)\((column attributes type)\)
        "
        sql-run $"
          CREATE TRIGGER (trigger modified (table attributes))
          AFTER UPDATE
          ON (table attributes)
          BEGIN
            UPDATE (table attributes)
            SET (modified-at) = datetime\('now'\) || 'Z'
            WHERE (primary-key) == NEW.(primary-key);
          END
        "
        sql-run $"CREATE VIEW (view attributes)
          AS SELECT
            a.(primary-key),
            a.(column attributes name),
            a.(column attributes type),
            t.(column attribute-types unique),
            t.(column attribute-types columns),
            a.(column attributes schema),
            a.(created-at),
            a.(modified-at)
          FROM (table attributes) a
          LEFT JOIN (table attribute-types) t ON a.(column attributes type) == t.(column attribute-types name)
        "
        sql-run $"CREATE TABLE (table type-map entity) \(
          (column type-map entity from) TEXT NOT NULL,
          (column type-map entity to) TEXT NOT NULL,
          FOREIGN KEY \((column type-map entity from)\)
            REFERENCES (table entity-types) \((column entity-types name)\)
              ON UPDATE RESTRICT
              ON DELETE RESTRICT,
          FOREIGN KEY \((column type-map entity to)\)
            REFERENCES (table attribute-types) \((column attribute-types name)\)
              ON UPDATE RESTRICT
              ON DELETE RESTRICT,
          UNIQUE\((column type-map entity from), (column type-map entity to)\)
        \)"
        sql-run $"CREATE INDEX (index (table type-map entity) (column type-map entity from))
          ON (table type-map entity)\((column type-map entity from)\)
        "
        sql-run $"CREATE INDEX (index (table type-map entity) (column type-map entity to))
          ON (table type-map entity)\((column type-map entity to)\)
        "
        sql-run $"CREATE TABLE (table type-map attribute) \(
          (column type-map attribute from) TEXT NOT NULL,
          (column type-map attribute to) TEXT NOT NULL,
          FOREIGN KEY \((column type-map attribute from)\)
            REFERENCES (table attribute-types) \((column attribute-types name)\)
              ON UPDATE RESTRICT
              ON DELETE RESTRICT,
          FOREIGN KEY \((column type-map attribute to)\)
            REFERENCES (table attribute-types) \((column attribute-types name)\)
              ON UPDATE RESTRICT
              ON DELETE RESTRICT,
          UNIQUE\((column type-map attribute from), (column type-map attribute to)\)
        \)"
        sql-run $"CREATE INDEX (index (table type-map attribute) (column type-map attribute from))
          ON (table type-map attribute)\((column type-map attribute from)\)
        "
        sql-run $"CREATE INDEX (index (table type-map attribute) (column type-map attribute to))
          ON (table type-map attribute)\((column type-map attribute to)\)
        "
        sql-run $"CREATE TABLE (table type-map null) \(
          (column type-map null to) TEXT NOT NULL UNIQUE,
          FOREIGN KEY \((column type-map null to)\)
            REFERENCES (table attribute-types) \((column attribute-types name)\)
              ON UPDATE RESTRICT
              ON DELETE RESTRICT
        \)"
        sql-run $"CREATE TABLE (table sources) \(
          (primary-key) INTEGER NOT NULL PRIMARY KEY,
          (column sources value) TEXT NOT NULL UNIQUE,
          (timing-columns)
        \)"
        sql-run $"
          CREATE TRIGGER (trigger modified (table sources))
          AFTER UPDATE
          ON (table sources)
          BEGIN
            UPDATE (table sources)
            SET (modified-at) = datetime\('now'\) || 'Z'
            WHERE (primary-key) == NEW.(primary-key);
          END
        "
        $versions
      }
    } # init
    clean: {
      args: ({} | schema struct --wrap-null)
      action: {|cmd|
        let result = sql-run $"
          SELECT (column type-map entity from) AS from_, (column type-map entity to) AS to_, rowid
          FROM (table type-map entity)
        "
        for it in $result {
          let result = sql-exists (table map entity $it.from_ $it.to_) ''
          if not $result {
            (sql-delete
              (table type-map entity)
              'rowid == ?'
              -p [$it.rowid]
            )
            sql-run $"DROP VIEW (view map entity $it.from_ $it.to_)"
            sql-run $"DROP TABLE (table map entity $it.from_ $it.to_)"
          }
        }
        let result = sql-run $"
          SELECT (column type-map attribute from) AS from_, (column type-map attribute to) AS to_, rowid
          FROM (table type-map attribute)
        "
        for it in $result {
          let result = sql-exists (table map attribute $it.from_ $it.to_) ''
          if not $result {
            (sql-delete
              (table type-map attribute)
              'rowid == ?'
              -p [$it.rowid]
            )
            sql-run $"DROP VIEW (view map attribute $it.from_ $it.to_)"
            sql-run $"DROP TABLE (table map attribute $it.from_ $it.to_)"
          }
        }
        let result = sql-run $"
          SELECT (column type-map null to) AS to_
          FROM (table type-map null)
        "
        for it in $result {
          let result = sql-exists (table map null $it.to_) ''
          if not $result {
            (sql-delete
              (table type-map null)
              $'(column type-map null to) == ?'
              -p [$it.to_]
            )
            sql-run $"DROP VIEW (view map null $it.to_)"
            sql-run $"DROP TABLE (table map null $it.to_)"
          }
        }
      }
    } # clean
    version: {
      get: {
        args: ({} | schema struct --wrap-null)
        action: {|cmd|
          sql-run $"
            SELECT
              (column version program),
              (column version data),
              (column version config)
            FROM (table version)
            WHERE (primary-key) == 42
          " | first
        }
      }
      update: {
        args: ({
          program: [string nothing]
          data: [string nothing]
          config: [string nothing]
        } | schema struct --wrap-missing)
        action: {|cmd|
          def compare [
            rhs: int
          ]: int -> int {
            if $in < $rhs { -1 } else if $in > $rhs { 1 } else { 0}
          }
          def 'compare list' [
            rhs: list<int>
          ]: list<int> -> int {
            let zeros = {|i| if $i > 0 {{out: 0, next: ($i - 1)}} }
            let lhs_w = $in | length
            let rhs_w = $rhs | length
            let w = [$lhs_w, $rhs_w] | math max
            let lhs = $in | append (generate $zeros ($w - $lhs_w))
            let rhs = $rhs | append (generate $zeros ($w - $rhs_w))
            for it in ($lhs | zip $rhs) {
              let cmp = $it.0 | compare $it.1
              if $cmp != 0 { return $cmp }
            }
            0
          }
          def 'compare version' [
            rhs: string
          ]: string -> int {
            let lhs = $in | split column '.' | first | values | into int
            let rhs = $rhs | split column '.' | first | values | into int
            $lhs | compare list $rhs
          }
          let result = sql-run $"
            SELECT
              (column version program) AS program,
              (column version data) AS data,
              (column version config) AS config
            FROM (table version)
            WHERE (primary-key) == 42
          "
          mut row = {}
          if ($cmd.program | is-not-empty) {
            if ($cmd.program | compare version $result.0.program) != 1 {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'expected greater version than current'
                  span: (metadata $cmd.program).span
                }
              }
            }
            $row = $row | insert (column version program) $cmd.program
          }
          if ($cmd.data | is-not-empty) {
            if ($cmd.data | compare version $result.0.data) != 1 {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'expected greater version than current'
                  span: (metadata $cmd.data).span
                }
              }
            }
            $row = $row | insert (column version data) $cmd.data
          }
          if ($cmd.config | is-not-empty) {
            if ($cmd.config | compare version $result.0.config) != 1 {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'expected greater version than current'
                  span: (metadata $cmd.config).span
                }
              }
            }
            $row = $row | insert (column version config) $cmd.config
          }
          if ($row | is-empty) {
            error make {
              msg: 'invalid operation'
              label: {
                text: 'no columns to update'
                span: (metadata $cmd).span
              }
            }
          }
          sql-update (table version) $'(primary-key) == 42' $row | first
        }
      }
    } # version
    entity-type: {
      add: {
        args: ({
          name: (schema sql ident --strict)
          schema: (schema data-arg)
        } | schema struct --wrap-single --wrap-missing)
        action: {|cmd|
          let columns = $cmd.schema | length
          create-entity-table $cmd.name $columns
          sql-insert (table entity-types) {
            (column entity-types name): $cmd.name
            (column entity-types columns): $columns
            (column entity-types schema): ($cmd.schema | schema data-schema | to msgpackz)
          } | first | update-entity-type
        }
      }
      get: {
        args: ({
          name: (schema sql ident --strict)
        } | schema struct --wrap-single)
        action: {|cmd|
          sql-run -p [$cmd.name] $"
            SELECT *
            FROM (table entity-types)
            WHERE (column entity-types name) == ?
          " -e 'entity type not found' -s (metadata $cmd.name).span | first | update-entity-type
        }
      }
      list: {
        args: ({} | schema struct --wrap-null)
        action: {|cmd|
          sql-run $"
            SELECT *
            FROM (table entity-types)
          " | update-entity-type
        }
      }
      patch: {
        args: ({
          name: (schema sql ident --strict)
          schema: (schema data-arg)
          migrate: [[]] # schema to update old data to new data
        } | schema struct --wrap-missing)
        action: {|cmd|
          let result = sql-run -p [$cmd.name] $"
            SELECT (column entity-types columns) AS columns
            FROM (table entity-types)
            WHERE (column entity-types name) == ?
          " -e 'entity type not found' -s (metadata $cmd.name).span
          let old_columns = $result.0.columns
          let new_columns = $cmd.schema | length
          let schema = $cmd.schema | schema data-schema
          let migrate = if ($cmd.migrate | is-not-empty) {
            $cmd.migrate | schema
          } else { $schema }
          let data_columns = 0..<($old_columns) | each {column entity data $in} | str join ', '
          if $old_columns != $new_columns {
            let result = sql-run $"
              SELECT *
              FROM (table entity $cmd.name)
            "
            # NOTE: this fails when new columns are added to the entity tables
            let data = $result
              | par-each {|row| $row
                | select (primary-key) (column entity source) (created-at) (modified-at)
                | data merge {column entity data $in} ($row
                  | reject (primary-key) (column entity source) (created-at) (modified-at)
                  | values
                  | normalize $migrate
                )
              }
            # TODO: confirm that this does not trigger deletion of mappings
            sql-run $'DROP VIEW (view entity $cmd.name)'
            sql-run $'DROP TABLE (table entity $cmd.name)'
            create-entity-table $cmd.name $new_columns
            sql-insert (table entity $cmd.name) $data
          } else {
            let result = sql-run $"
              SELECT (primary-key) AS id, ($data_columns)
              FROM (table entity $cmd.name)
            "
            let data = $result
              | par-each {|row| $row
                | select id
                | data merge {column entity data $in} ($row
                  | reject id
                  | values
                  | normalize $migrate
                )
              }
            sql-upsert (table entity $cmd.name) $data
          }
          sql-update (table entity-types) $'(column entity-types name) == ?' -p [$cmd.name] {
            (column entity-types schema): ($schema | to msgpackz)
          }
        }
      }
      delete: {
        args: ({
          name: (schema sql ident --strict)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-single --wrap-missing)
        action: {|cmd|
          if not $cmd.force {
            let result = sql-exists (table entity $cmd.name) ''
            if $result {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'entity type in use, run with force to ignore'
                  span: (metadata $cmd.name).span
                }
              }
            }
          }
          let result = sql-delete (table type-map entity) $'(column type-map entity from) == ?' -p [$cmd.name]
          for it in $result {
            let to = $it | get (column type-map entity to)
            sql-run $"DROP VIEW (view map entity $cmd.name $to)"
            sql-run $"DROP TABLE (table map entity $cmd.name $to)"
          }
          sql-run $"DROP VIEW (view entity $cmd.name)"
          sql-run $"DROP TABLE (table entity $cmd.name)"
          sql-delete (table entity-types) $'(column entity-types name) == ?' -p [$cmd.name] | first | update-entity-type
        }
      }
    } # entity-type
    attribute-type: {
      add: {
        args: ({
          name: (schema sql ident --strict)
          unique: [[nothing {fallback: false}] bool]
          columns: [[nothing {fallback: 0}] (schema uint)]
        } | schema struct --wrap-single --wrap-missing)
        action: {|cmd|
          sql-insert (table attribute-types) {
            (column attribute-types name): $cmd.name
            (column attribute-types unique): $cmd.unique
            (column attribute-types columns): $cmd.columns
          } | first | update-attribute-type
        }
      }
      get: {
        args: ({
          name: (schema sql ident --strict)
        } | schema struct --wrap-single)
        action: {|cmd|
          sql-run -p [$cmd.name] $"
            SELECT *
            FROM (table attribute-types)
            WHERE (column attribute-types name) == ?
          " -e 'attribute type not found' -s (metadata $cmd.name).span | first | update-attribute-type
        }
      }
      list: {
        args: ({} | schema struct --wrap-null)
        action: {|cmd|
          sql-run $"
            SELECT *
            FROM (table attribute-types)
          " | update-attribute-type
        }
      }
      patch: {
        args: ({
          name: (schema sql ident --strict)
          unique: [bool nothing]
          columns: [(schema uint) nothing]
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          let result = sql-run -p [$cmd.name] $"
            SELECT (column attribute-types columns) AS columns, (column attribute-types unique) AS is_unique
            FROM (table attribute-types)
            WHERE (column attribute-types name) == ?
          " -e 'attribute type not found' -s (metadata $cmd.name).span
          let columns = $result.0.columns
          let unique = $result.0.is_unique != 0
          mut row = {}
          if ($cmd.columns | is-not-empty) and $cmd.columns != $columns {
            let result = (sql-exists
              (table attributes)
              $'(column attributes type) == ?'
              -p [$cmd.name]
            )
            if $result {
              if $cmd.force {
                sql-delete (table attributes) $'(column attributes type) == ?' -p [$cmd.name]
              } else {
                error make {
                  msg: 'invalid operation'
                  label: {
                    text: 'changing columns of attribute type with active attributes is not allowed, move attributes first, or use force to delete them'
                    span: (metadata $cmd.name).span
                  }
                }
              }
            }
            $row = $row | insert (column attribute-types columns) $cmd.columns
          }
          if ($cmd.unique | is-not-empty) and $cmd.unique != $unique {
            for it in (get-attribute-mappings $cmd.name) {
              let result = sql-run $"SELECT * FROM ($it.table)"
              sql-run $'DROP VIEW ($it.view)'
              sql-run $'DROP TABLE ($it.table)'
              match $it.type {
                'entity' => (create-entity-map $it.entity $cmd.name $cmd.columns $cmd.unique),
                'attribute' => (create-attribute-map $it.attribute $cmd.name $cmd.columns $cmd.unique),
                'null' => (create-null-map $cmd.name $cmd.columns $cmd.unique)
              }
              sql-insert $it.table $result
            }
            $row = $row | insert (column attribute-types unique) $cmd.unique
          }
          if ($row | is-empty) { return }
          sql-update (table attribute-types) $'(column attribute-types name) == ?' -p [$cmd.name] $row
        }
      }
      delete: {
        args: ({
          name: (schema sql ident --strict)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          if not $cmd.force {
            let result = sql-exists (table attributes) $'(column attributes type) == ?' -p [$cmd.name]
            if $result {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'attribute type is in use, run with force to ignore'
                  span: (metadata $cmd.name).span
                }
              }
            }
          }
          let result = sql-delete (table type-map entity) $'(column type-map entity to) == ?' -p [$cmd.name]
          for from in ($result | get (column type-map entity from)) {
            sql-run $"DROP VIEW (view map entity $from $cmd.name)"
            sql-run $"DROP TABLE (table map entity $from $cmd.name)"
          }
          let result = (sql-delete
            (table type-map attribute)
            $"? IN \((column type-map attribute from), (column type-map attribute to)\)"
            -p [$cmd.name]
          )
          for it in $result {
            let from = $it | get (column type-map attribute from | unescape)
            let to = $it | get (column type-map attribute to | unescape)
            sql-run $"DROP VIEW (view map attribute $from $to)"
            sql-run $"DROP TABLE (table map attribute $from $to)"
          }
          let result = sql-delete (table type-map null) $'(column type-map null to) == ?' -p [$cmd.name]
          if ($result | is-not-empty) {
            sql-run $"DROP VIEW (view map null $cmd.name)"
            sql-run $"DROP TABLE (table map null $cmd.name)"
          }
          sql-delete (table attributes) $'(column attributes type) == ?' -p [$cmd.name]
          (sql-delete
            (table attribute-types)
            $'(column attribute-types name) == ?'
            -p [$cmd.name]
            -e 'attribute type not found'
          ) | first | update-attribute-type
        }
      }
    } # attribute-type
    source: {
      add: {
        args: ({
          value: ('string' | schema array --wrap-single)
        } | schema struct --wrap-single)
        action: {|cmd|
          sql-insert (table sources) ($cmd.value | wrap (column sources value)) | update-source
        }
      }
      ensure: {
        args: ({
          value: ('string' | schema array --wrap-single)
        } | schema struct)
        action: {|cmd|
          sql-upsert (table sources) ($cmd.value | wrap (column sources value)) | update-source
        }
      }
      get: {
        args: ({
          value: (schema source)
        } | schema struct --wrap-single)
        action: {|cmd|
          let key = if ($cmd.value | describe -d).type == string {
            column sources value
          } else {
            primary-key
          }
          sql-run -p [$cmd.value] $"
            SELECT *
            FROM (table sources)
            WHERE ($key) == ?
          " -e 'source not found' | first | update-source
        }
      }
      list: {
        args: ({} | schema struct --wrap-null)
        action: {|cmd|
          sql-run $"
            SELECT *
            FROM (table sources)
          " | update-source
        }
      }
      move: {
        args: ({
          from: (schema source)
          to: string
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          let from = get-source $cmd.from
          let result = sql-run -p [$cmd.to] $"
            SELECT (primary-key) AS id
            FROM (table sources)
            WHERE (column sources value) == ?
          "
          if ($result | is-not-empty) {
            if not $cmd.force {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'destination already exists, use force to merge sources'
                  span: (metadata $cmd.to).span
                }
              }
            }
            let to = $result.0.id
            let result = sql-run $"
              SELECT (column entity-types name) AS types
              FROM (table entity-types)
            "
            let result = $result.types | each {|type|
              let result = sql-update (table entity $type) $'(column entity source) == ?' -p [$from] {
                (column entity source): $to
              } | update-entity
              [$type $result]
            } | into record
            sql-delete (table sources) $'(primary-key) == ?' -p [$from]
            $result
          } else {
            sql-update (table sources) $"(primary-key) == ?" -p [$from] {
              (column sources value): $cmd.to
            } | first | update-source
          }
        }
      }
      delete: {
        args: ({
          value: (schema source)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-single --wrap-missing)
        action: {|cmd|
          let value = get-source $cmd.value
          if not $cmd.force {
            let result = sql-run $"
              SELECT (column entity-types name) AS name
              FROM (table entity-types)
            "
            for type in $result.name {
              let result = sql-exists (table entity $type) $'(column entity source) == ?' -p [$value]
              if $result {
                error make {
                  msg: 'invalid operation'
                  label: {
                    text: 'source is in use, run with force to ignore'
                    span: (metadata $cmd.value).span
                  }
                }
              }
            }
          }
          sql-delete (table sources) $"(primary-key) == ?" -p [$value] | first | update-source
        }
      }
    } # source
    entity: {
      add: {
        args: ({
          type: (schema sql ident --strict)
          source: (schema source)
          data: [[]] # match against type->schema
        } | schema struct --wrap-missing)
        action: {|cmd|
          let result = sql-run -p [$cmd.type] $"
            SELECT (column entity-types schema) AS schema
            FROM (table entity-types)
            WHERE (column entity-types name) == ?
          " -e 'invalid entity type' -s (metadata $cmd.type).span
          let source = get-source $cmd.source
          # NOTE: normalize error message is garbage (doesn't say that normalize is causing it)
          let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
          sql-insert (table entity $cmd.type) ({
            (column entity source): $source
          } | data merge {column entity data $in} $data) | first | update-entity
        }
      }
      get: {
        args: ({
          type: (schema sql ident --strict)
          entity: (schema entity)
        } | schema struct)
        action: {|cmd|
          let entity = get-entity $cmd.type $cmd.entity
          sql-run -p [$entity] $"
            SELECT *
            FROM (view entity $cmd.type)
            WHERE (primary-key) == ?
          " | first | update-entity
        }
      }
      list: {
        args: ({
          type: [(schema sql ident --strict) nothing]
        } | schema struct --wrap-single --wrap-missing)
        action: {|cmd|
          let types = get-entity-types -t $cmd.type
          $types | each {|type|
            let result = sql-run $"
              SELECT *
              FROM (view entity $type)
            " | update-entity
            [$type, $result]
          } | into record
        }
      }
      attributes: {
        args: ({
          type: (schema sql ident --strict)
          entity: (schema entity)
        } | schema struct)
        action: {|cmd|
          let entity = get-entity $cmd.type $cmd.entity
          let result = sql-run -p [$cmd.type] $"
            SELECT (column type-map entity to) AS types
            FROM (table type-map entity)
            WHERE (column type-map entity from) == ?
          "
          $result.types | each {|type|
            sql-run -p [$entity] $"
              SELECT *
              FROM (view attributes)
              WHERE (primary-key) IN \(
                SELECT (column map entity to)
                FROM (table map entity $cmd.type $type)
                WHERE (column map entity from) == ?
              \)
            "
          } | flatten | update-attribute --view
        }
      }
      with: {
        attribute: {
          args: ({
            attribute: (schema attribute)
            type: [(schema sql ident --strict) nothing]
          } | schema struct --wrap-single --wrap-missing)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute
            let types = get-entity-types -t $cmd.type -f $attribute.type
            $types | each {|type|
              let result = sql-run -p [$attribute.id] $"
                SELECT *
                FROM (view entity $type)
                WHERE (primary-key) IN \(
                  SELECT (column map entity from)
                  FROM (table map entity $type $attribute.type)
                  WHERE (column map entity to) == ?
                \)
              " | update-entity
              if ($result | is-not-empty) { [$type, $result] }
            } | into record
          }
        }
      } # with
      move: {
        args: ({
          type: (schema sql ident --strict)
          from: (schema entity)
          to: ({
            source: [(schema source) nothing]
            # TODO: only partially specify data instead of all or nothing
            data: [[]] # match against type->schema (optional)
          } | schema struct --wrap-single --wrap-missing)
        } | schema struct)
        action: {|cmd|
          let from = get-entity $cmd.type $cmd.from --get-schema
          mut row = {}
          if ($cmd.to.source | is-not-empty) {
            let source = get-source $cmd.to.source
            $row = $row | insert (column entity source) $source
          }
          if ($cmd.to.data | is-not-empty) {
            let data = $cmd.to.data | normalize $from.schema
            $row = $row | data merge {column entity data $in} $data
          }
          if ($row | is-empty) {
            error make {
              msg: 'invalid operation'
              label: {
                text: 'at least one of source or data has to be not null'
                span: (metadata $cmd.to).span
              }
            }
          }
          sql-update (table entity $cmd.type) $'(primary-key) == ?' -p [$from.id] $row | first | update-entity
        }
      }
      delete: {
        args: ({
          type: (schema sql ident --strict)
          entity: (schema entity)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          let entity = get-entity $cmd.type $cmd.entity
          if not $cmd.force {
            let result = sql-run -p [$cmd.type] $"
              SELECT (column type-map entity to) AS to_
              FROM (table type-map entity)
              WHERE (column type-map entity from) == ?
            "
            for to in $result.to_ {
              let result = sql-exists (table map entity $cmd.type $to) $'(column map entity from) == ?' -p [$entity]
              if $result {
                error make {
                  msg: 'invalid operation'
                  label: {
                    text: 'entity has attributes, run with force to ignore'
                    span: (metadata $cmd.entity).span
                  }
                }
              }
            }
          }
          sql-delete (table entity $cmd.type) $'(primary-key) == ?' -p [$entity] | first | update-entity
        }
      }
    } # entity
    attribute: {
      add: {
        args: ({
          name: string
          type: (schema sql ident --strict)
          schema: (schema data-arg)
        } | schema struct --wrap-missing)
        action: {|cmd|
          let result = sql-run -p [$cmd.type] $"
            SELECT (column attribute-types columns) AS columns
            FROM (table attribute-types)
            WHERE (column attribute-types name) == ?
          " -e 'attribute type not found' -s (metadata $cmd.type).span
          if $result.0.columns != ($cmd.schema | length) {
            error make {
              msg: "invalid operation"
              label: {
                text: $"attribute schema does not match column count of attribute type \(($result.0.columns)\)"
                span: (metadata $cmd.schema).span
              }
            }
          }
          sql-insert (table attributes) {
            (column attributes name): $cmd.name
            (column attributes type): $cmd.type
            (column attributes schema): ($cmd.schema | schema data-schema | to msgpackz)
          } | first | update-attribute
        }
      }
      get: {
        args: ({
          attribute: (schema attribute)
        } | schema struct --wrap-single)
        action: {|cmd|
          let key = if ($cmd.attribute | describe -d).type == string {
            column attributes name
          } else {
            primary-key
          }
          sql-run -p [$cmd.attribute] $"
            SELECT *
            FROM (view attributes)
            WHERE ($key) == ?
          " -e 'attribute not found' | first | update-attribute --view
        }
      }
      list: {
        args: ({} | schema struct --wrap-null)
        action: {|cmd|
          sql-run $"
            SELECT *
            FROM (view attributes)
          " | update-attribute --view
        }
      }
      attributes: {
        args: ({
          attribute: (schema attribute)
        } | schema struct --wrap-single)
        action: {|cmd|
          let attribute = get-attribute $cmd.attribute
          let result = sql-run -p [$attribute.type] $"
            SELECT (column type-map attribute to) AS types
            FROM (table type-map attribute)
            WHERE (column type-map attribute from) == ?
          "
          $result.types | each {|type|
            sql-run -p [$attribute.id] $"
              SELECT *
              FROM (view attributes)
              WHERE (primary-key) IN \(
                SELECT (column map attribute to)
                FROM (table map attribute $attribute.type $type)
                WHERE (column map attribte from) == ?
              \)
            "
          } | flatten | update-attribute --view
        }
      }
      with: {
        attribute: {
          args: ({
            type: [(schema sql ident --strict) nothing]
            attribute: (schema attribute)
          } | schema struct --wrap-single --wrap-missing)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute
            let types = get-attribute-types -t $cmd.type -f $attribute.type
            $types | each {|type|
              sql-run -p [$attribute.id] $"
                SELECT *
                FROM (view attributes)
                WHERE (primary-key) IN \(
                  SELECT (column map attribute from)
                  FROM (table map attribute $type $attribute.type)
                  WHERE (column map attribute to) == ?
                \)
              "
            } | flatten | update-attribute --view
          }
        }
      } # with
      rename: {
        args: ({
          from: (schema attribute)
          to: string
        } | schema struct --wrap-missing)
        action: {|cmd|
          let from = get-attribute $cmd.from
          sql-update (table attributes) $"(primary-key) == ?" -p [$from.id] {
            (column attributes name): $cmd.to
          } | first | update-attribute
        }
      }
      patch: {
        args: ({
          attribute: (schema attribute)
          type: [(schema sql ident --strict) nothing]
          schema: (schema data-arg)
          migrate: [[]]
        } | schema struct --wrap-missing)
        action: {|cmd|
          let attribute = get-attribute $cmd.attribute
          let type = $cmd.type | default $attribute.type
          let result = sql-run -p [$type] $"
            SELECT (column attribute-types columns) AS columns, (column attribute-types unique) AS is_unique
            FROM (table attribute-types)
            WHERE (column attribute-types name) == ?
          " -e 'attribute type not found' -s (metadata $cmd.type).span
          let columns = $result.0.columns
          let unique = $result.0.is_unique
          let schema = $cmd.schema | schema data-schema
          let migrate = if ($cmd.migrate | is-not-empty) {
            $cmd.migrate | schema
          } else { $schema }
          if $attribute.type != $type {
            for it in (get-attribute-mappings $attribute.type) {
              let result = sql-delete $it.table $'($it.to) == ?' -p [$attribute.type]
              # NOTE: this fails when new columns are added to the entity tables
              let other_columns = [(primary-key), $it.from?, $it.to, (created-at), (modified-at)] | compact
              let data = $result
                | par-each {|row| $row
                  | select ...$other_columns
                  | data merge $it.data ($row
                    | reject ...$other_columns
                    | values
                    | normalize $migrate
                  )
                }
              let new_table = match $it.type {
                'entity' => {
                  let result = (sql-exists
                    (table type-map entity)
                    $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
                    -p [$it.entity, $type]
                  )
                  if not $result {
                    create-entity-map $it.entity $type $columns $unique
                    sql-insert (table type-map entity) {
                      (column type-map entity from): $it.entity
                      (column type-map entity to): $type
                    }
                  }
                  (table map entity $it.entity $type)
                },
                'attribute' => {
                  let result = (sql-exists
                    (table type-map attribute)
                    $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
                    -p [$it.attribute, $type]
                  )
                  if not $result {
                    create-attribute-map $it.attribute $type $columns $unique
                    sql-insert (table type-map attribute) {
                      (column type-map attribute from): $it.attribute
                      (column type-map attribute to): $type
                    }
                  }
                  (table map attribute $it.attribute $type)
                },
                'null' => {
                  let result = (sql-exists
                    (table type-map null)
                    $'(column type-map null to) == ?'
                    -p [$type]
                  )
                  if not $result {
                    create-null-map $type $columns $unique
                    sql-insert (table type-map null) {
                      (column type-map null to): $type
                    }
                  }
                  (table map null $type)
                }
              }
              sql-insert $new_table $data
            }
          } else {
            for it in (get-attribute-mappings $type) {
              let data_columns = 0..<($columns) | each $it.data | str join ', '
              let result = sql-run $"
                SELECT (primary-key) AS id, ($data_columns)
                FROM ($it.table)
              "
              let data = $result
                | par-each {|row| $row
                  | select id
                  | data merge $it.data ($row
                    | reject id
                    | values
                    | normalize $migrate
                  )
                }
              sql-upsert $it.table $data
            }
          }
          sql-update (table attributes) $'(column attributes name) == ?' -p [$cmd.name] {
            (column attributes type): $type
            (column attributes schema): ($schema | to msgpackz)
          }
        }
      }
      delete: {
        args: ({
          attribute: (schema attribute)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing --wrap-single)
        action: {|cmd|
          let attribute = get-attribute $cmd.attribute
          if $cmd.force {
            let result = sql-run -p [$attribute.type] $"
              SELECT (column type-map attribute to) AS types
              FROM (table type-map attribute)
              WHERE (column type-map attribute from) == ?
            "
            for type in $result.types {
              let result = (sql-exists
                (table map attribute $attribute.type $type)
                $'(column map attribute from) == ?'
                -p [$attribute.id]
              )
              if $result {
                error make {
                  msg: 'invalid operation'
                  label: {
                    text: 'attribute has attributes, use force to ignore'
                    span: (metadata $cmd.attribute).span
                  }
                }
              }
            }
            for it in (get-attribute-mappings $attribute.type) {
              let result = sql-exists $it.table $'($it.to) == ?' -p [$attribute.id]
              if $result {
                error make {
                  msg: 'invalid operation'
                  label: {
                    text: $'attribute is in use by ($it.type), use force to ignore'
                    span: (metadata $cmd.attribute).span
                  }
                }
              }
            }
          }
          sql-delete (table attributes) $'(primary-key) == ?' -p [$attribute.id] | first | update-attribute
        }
      }
    } # attribute
    'null': {
      attributes: {
        args: ({} | schema struct --wrap-null)
        action: {|cmd|
          let result = sql-run $"
            SELECT (column type-map null to) AS types
            FROM (table type-map null)
          "
          $result.types | each {|type|
            sql-run $"
              SELECT *
              FROM (view attributes)
              WHERE (primary-key) IN \(
                SELECT (column map attribute to)
                FROM (table map null $type)
              \)
            "
          } | flatten | update-attribute --view
        }
      }
    } # null
    map: {
      entity: {
        add: {
          args: ({
            type: (schema sql ident --strict)
            entity: (schema entity)
            attribute: (schema attribute)
            data: [[]] # match against attribute-type->schema
          } | schema struct --wrap-missing)
          action: {|cmd|
            let entity = get-entity $cmd.type $cmd.entity
            let attribute = get-attribute $cmd.attribute --get-schema
            let data = $cmd.data | normalize $attribute.schema
            let result = (sql-exists
              (table type-map entity)
              $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
              -p [$cmd.type $attribute.type]
            )
            if not $result {
              let result = sql-run -p [$attribute.type] $"
                SELECT (column attribute-types unique) AS is_unique, (column attribute-types columns) AS columns
                FROM (table attribute-types)
                WHERE (column attribute-types name) == ?
              "
              create-entity-map $cmd.type $attribute.type $result.0.columns ($result.0.is_unique != 0)
              sql-insert (table type-map entity) {
                (column type-map entity from): $cmd.type
                (column type-map entity to): $attribute.type
              }
            }
            sql-insert (table map entity $cmd.type $attribute.type) ({
              (column map entity from): $entity
              (column map entity to): $attribute.id
            } | data merge {column map entity data $in} $data) | first | update-map
          }
        }
        get: {
          args: ({
            type: (schema sql ident --strict)
            attribute: (schema attribute)
            id: int
          } | schema struct)
          action: {|cmd|
            (sql-exists
              (table entity-types)
              $'(column entity-types name) == ?'
              -p [$cmd.type]
              -m 'entity type' -s (metadata $cmd.type).span
            )
            let attribute = get-attribute $cmd.attribute
            (sql-exists
              (table type-map entity)
              $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
              -p [$cmd.type, $attribute.type]
              -m mapping
            )
            sql-run -p [$cmd.id, $attribute.id] $"
              SELECT *
              FROM (view map entity $cmd.type $attribute.type)
              WHERE (primary-key) == ? AND (namespaced (column map entity to) (primary-key)) == ?
            " -e 'bad or missing mapping id' -s (metadata $cmd.id).span | first | update-map
          }
        }
        get-all: {
          args: ({
            type: (schema sql ident --strict)
            entity: (schema entity)
            attribute: (schema attribute)
          } | schema struct)
          action: {|cmd|
            (sql-exists
              (table entity-types)
              $'(column entity-types name) == ?'
              -p [$cmd.type]
              -m 'entity type' -s (metadata $cmd.type).span
            )
            let attribute = get-attribute $cmd.attribute
            let result = (sql-exists
              (table type-map entity)
              $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
              -p [$cmd.type, $attribute.type]
            )
            if $result {
              let entity = get-entity $cmd.type $cmd.entity
              sql-run -p [$entity, $attribute.id] $"
                SELECT *
                FROM (view map entity $cmd.type $attribute.type)
                WHERE (namespaced (column map entity from) (primary-key)) == ?
                  AND (namespaced (column map entity to) (primary-key)) == ?
              " | update-map
            } else { [] }
          }
        }
        list: {
          args: ({
            type: (schema sql ident --strict)
            entity: (schema entity)
          } | schema struct)
          action: {|cmd|
            let entity = get-entity $cmd.type $cmd.entity
            let result = sql-run -p [$cmd.type] $"
              SELECT (column type-map entity to) AS types
              FROM (table type-map entity)
              WHERE (column type-map entity from) == ?
            "
            $result.types | each {|type|
              let result = sql-run -p [$entity] $"
                SELECT *
                FROM (view map entity $cmd.type $type)
                WHERE (namespaced (column map entity from) (primary-key)) == ?
              " | update-map
              [$type, $result]
            } | into record
          }
        }
        update: {
          args: ({
            type: (schema sql ident --strict)
            attribute: (schema attribute)
            id: int
            # TODO: allow to specify partial data only
            data: [[]] # match against attribute-type->schema
          } | schema struct)
          action: {|cmd|
            (sql-exists
              (table entity-types)
              $'(column entity-types name) == ?'
              -p [$cmd.type]
              -m 'entity type' -s (metadata $cmd.type).span
            )
            let attribute = get-attribute $cmd.attribute --get-schema
            let data = $cmd.data | normalize $attribute.schema
            let result = (sql-exists
              (table type-map entity)
              $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
              -p [$cmd.type, $attribute.type]
              -m mapping
            )
            (sql-update
              (table map entity $cmd.type $attribute.type)
              $"(primary-key) == ? AND (column map entity to) == ?"
              -p [$cmd.id, $attribute.id]
              (data merge {column map entity data $in} $data)
              -e 'bad or missing mapping data'
            ) | first | update-map
          }
        }
        delete: {
          args: ({
            type: (schema sql ident --strict)
            attribute: (schema attribute)
            id: int
          } | schema struct)
          action: {|cmd|
            (sql-exists
              (table entity-types)
              $'(column entity-types name) == ?'
              -p [$cmd.type]
              -m 'entity type' -s (metadata $cmd.type).span
            )
            let attribute = get-attribute $cmd.attribute
            (sql-exists
              (table type-map entity)
              $'(column type-map entity from) == ? AND (column type-map entity to) == ?'
              -p [$cmd.type, $attribute.type]
              -m mapping
            )
            (sql-delete
              (table map entity $cmd.type $attribute.type)
              $"(primary-key) == ? AND (column map entity to) == ?"
              -p [$cmd.id, $attribute.id]
              -e 'bad or mising mapping id'
            ) | first | update-map
          }
        }
      } # entity
      attribute: {
        add: {
          args: ({
            from: (schema attribute)
            to: (schema attribute)
            data: [[]] # match against to-type->schema
          } | schema struct --wrap-missing)
          action: {|cmd|
            let from = get-attribute $cmd.from
            let to = get-attribute $cmd.to --get-schema
            let data = $cmd.data | normalize $to.schema
            let result = (sql-exists
              (table type-map attribute)
              $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
              -p [$from.type, $to.type]
            )
            if not $result {
              let result = sql-run -p [$to.type] $"
                SELECT (column attribute-types unique) AS is_unique, (column attribute-types columns) AS columns
                FROM (table attribute-types)
                WHERE (column attribute-types name) == ?
              "
              create-attribute-map $from.type $to.type $result.0.columns ($result.0.is_unique != 0)
              sql-insert (table type-map attribute) {
                (column type-map attribute from): $from.type
                (column type-map attribute to): $to.type
              }
            }
            sql-insert (table map attribute $from.type $to.type) ({
              (column map attribute from): $from.id
              (column map attribute to): $to.id
            } | data merge {column map attribute data $in} $data) | first | update-map
          }
        }
        get: {
          args: ({
            from: (schema attribute)
            to: (schema attribute)
            id: int
          } | schema struct)
          action: {|cmd|
            let from = get-attribute $cmd.from
            let to = get-attribute $cmd.to
            (sql-exists
              (table type-map attribute)
              $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
              -p [$from.type, $to.type]
              -m mapping
            )
            sql-run -p [$cmd.id, $from.id, $to.id] $"
              SELECT *
              FROM (view map attribute $from.type $to.type)
              WHERE (primary-key) == ? AND
                (namespaced (column map attribute from) (primary-key)) == ? AND
                (namespaced (column map attribute to) (primary-key)) == ?
            " -e 'bad or missing mapping id' -s (metadata $cmd.id).span | first | update-map
          }
        }
        get-all: {
          args: ({
            from: (schema attribute)
            to: (schema attribute)
          } | schema struct)
          action: {|cmd|
            let from = get-attribute $cmd.from
            let to = get-attribute $cmd.to
            let result = (sql-exists
              (table type-map attribute)
              $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
              -p [$from.type, $to.type]
            )
            if $result {
              sql-run -p [$from.id, $to.id] $"
                SELECT *
                FROM (view map attribute $from.type $to.type)
                WHERE (namespaced (column map attribute from) (primary-key)) == ?
                  AND (namespaced (column map attribute to) (primary-key)) == ?
              " | update-map
            } else { [] }
          }
        }
        list: {
          args: ({
            attribute: (schema attribute)
          } | schema struct --wrap-single)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute
            let result = sql-run -p [$attribute.type] $"
              SELECT (column type-map attribute to) AS types
              FROM (table type-map attribute)
              WHERE (column type-map attribute from) == ?
            "
            $result.types | each {|type|
              let result = sql-run -p [$attribute.id] $"
                SELECT *
                FROM (view map attribute $attribute.type $type)
                WHERE (namespaced (column map attribute from) (primary-key)) == ?
              " | update-map
              [$type, $result]
            } | into record
          }
        }
        update: {
          args: ({
            from: (schema attribute)
            to: (schema attribute)
            id: int
            # TODO: allow to specify partial data only
            data: [[]] # match against attribute-type->schema
          } | schema struct)
          action: {|cmd|
            let from = get-attribute $cmd.from
            let to = get-attribute $cmd.to --get-schema
            let data = $cmd.data | normalize $to.schema
            let result = (sql-exists
              (table type-map attribute)
              $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
              -p [$from.type, $to.type]
              -m mapping
            )
            (sql-update
              (table map attribute $from.type $to.type)
              $"(primary-key) == ? AND (column map attribute from) == ? AND (column map attribute to) == ?"
              -p [$cmd.id, $from.id, $to.id]
              (data merge {column map attribute data $in} $data)
              -e 'bad or missing mapping id'
            ) | first | update-map
          }
        }
        delete: {
          args: ({
            from: (schema attribute)
            to: (schema attribute)
            id: int
          } | schema struct)
          action: {|cmd|
            let from = get-attribute $cmd.from
            let to = get-attribute $cmd.to
            (sql-exists
              (table type-map attribute)
              $'(column type-map attribute from) == ? AND (column type-map attribute to) == ?'
              -p [$from.type, $to.type]
              -m mapping
            )
            (sql-delete
              (table map attribute $from.type $to.type)
              $'(primary-key) == ? AND (column map attribute from) == ? AND (column map attribute to) == ?'
              -p [$cmd.id, $from, $to]
              -e 'bad or missing mapping id'
            ) | first | update-map
          }
        }
      } # attribute
      'null': {
        add: {
          args: ({
            attribute: (schema attribute)
            data: [[]] # match against type->schema
          } | schema struct --wrap-single --wrap-missing)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute --get-schema
            let data = $cmd.data | normalize $attribute.schema
            let result = sql-exists (table type-map null) $'(column type-map null to) == ?' -p [$attribute.type]
            if not $result {
              let result = sql-run -p [$attribute.type] $"
                SELECT (column attribute-types unique) AS is_unique, (column attribute-types columns) AS columns
                FROM (table attribute-types)
                WHERE (column attribute-types name) == ?
              "
              create-null-map $attribute.type $result.0.columns ($result.0.is_unique != 0)
              sql-insert (table type-map null) {
                (column type-map null to): $attribute.type
              }
            }
            sql-insert (table map null $attribute.type) ({
              (column map null to): $attribute.id
            } | data merge {column map null data $in} $data) | first | update-map
          }
        }
        get: {
          args: ({
            attribute: (schema attribute)
            id: int
          } | schema struct)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute
            (sql-exists
              (table type-map null)
              $'(column type-map null to) == ?'
              -p [$attribute.type]
              -m mapping
            )
            sql-run -p [$cmd.id, $attribute.id] $"
              SELECT *
              FROM (view map null $attribute.type)
              WHERE (primary-key) == ? AND (namespaced (column map null to) (primary-key)) == ?
            " -e 'bad or missing mapping id' -s (metadata $cmd.id).span | first | update-map
          }
        }
        get-all: {
          args: ({
            attribute: (schema attribute)
          } | schema struct)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute
            let result = (sql-exists
              (table type-map null)
              $'(column type-map null to) == ?'
              -p [$attribute.type]
            )
            if $result {
              sql-run -p [$attribute.id] $"
                SELECT *
                FROM (view map null $attribute.type)
                WHERE (namespaced (column map null to) (primary-key)) == ?
              " | update-map
            } else { [] }
          }
        }
        list: {
          args: ({} | schema struct --wrap-null)
          action: {|cmd|
            let result = sql-run $"
              SELECT (column type-map null to) AS types
              FROM (table type-map attribute)
            "
            $result.types | each {|type|
              let result = sql-run $"
                SELECT *
                FROM (view map null $type)
              " | update-map
              [$type, $result]
            } | into record
          }
        }
        update: {
          args: ({
            attribute: (schema attribute)
            id: int
            # TODO: allow to specify partial data only
            data: [[]] # match against attribute-type->schema
          } | schema struct)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute --get-schema
            let data = $cmd.data | normalize $attribute.schema
            (sql-exists
              (table type-map null)
              $'(column type-map null to) == ?'
              -p [$attribute.type]
              -m mapping
            )
            (sql-update
              (table map null $attribute.type)
              $"(primary-key) == ? AND (column map null to) == ?"
              -p [$cmd.id, $attribute.id]
              (data merge {column map null data $in} $data)
              -e 'bad or missing mapping id'
            ) | first | update-map
          }
        }
        delete: {
          args: ({
            attribute: (schema attribute)
            id: int
          } | schema struct)
          action: {|cmd|
            let attribute = get-attribute $cmd.attribute
            (sql-exists
              (table type-map null)
              $'(column type-map null to) == ?'
              -p [$attribute.type]
              -m mapping
            )
            (sql-delete
              (table map null $attribute.type)
              $'(primary-key) == ? AND (column map null to) == ?'
              -p [$cmd.id, $attribute.id]
              -e 'bad or missing mapping id'
            ) | first | update-map
          }
        }
      } # null
    } # map
  }}
}
