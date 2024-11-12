use naming.nu *

# schema that matches unsigned integer
export def 'schema uint' [
]: nothing -> any {
  plugin use schema
  [[int {
    if ($in >= 0) {
      {ok: $in}
    } else {
      {err: "integer can't be negative"}
    }
  }]] | schema
}
# schema that matches schema for data columns
export def 'schema data-arg' [
]: nothing -> any {
  plugin use schema
  {all: []} | schema array --wrap-single --wrap-null
}
# creates schema for data columns from arg
export def 'schema data-schema' [
]: any -> any {
  plugin use schema
  $in | schema tuple --wrap-single --wrap-null
}
# schema that matches valid SQLite identifier
export def 'schema sql ident' [
  --strict (-s)
]: nothing -> any {
  plugin use schema
  {
    if $strict and ($in | is-valid --no-escape) or not $strict and ($in | is-valid) {
      {ok: $in}
    } else {
      {err: 'invalid SQL identifier'}
    }
  } | schema
}
# schema that matches valid SQLite value
export def 'schema sql value' [
]: nothing -> any {
  plugin use schema
  # TODO: restrict to valid sql values
  [[]] | schema
}
# schema that matches SQLite record
export def 'schema sql row' [
]: nothing -> any {
  plugin use schema
  [(schema sql ident) (schema sql value)] | schema map --length 1..
}
# schema that matches list of SQLite records
export def 'schema sql table' [
]: nothing -> any {
  plugin use schema
  schema sql row | schema array --wrap-single --length 1..
}

export def 'commands init' [
  --file (-f): path
]: nothing -> record {
  let db = if ($file | is-not-empty) {
    open $file
  } else {
    stor open
  }
  if ($db | describe -d) != {type: custom, subtype: SQLiteDatabase} {
    error make {msg: "input was not a valid sqlite database"}
  }
  {__connection: $db}
}
# direct SQL commands (can break database integrity!)
export def 'commands sql' [
]: record<__connection> -> record {
  plugin use schema
  let db = $in.__connection
  $in | merge {sql: {
    generic: {
      args: ({
        query: string
        params: [[]]
      } | schema struct --wrap-missing)
      action: {|cmd|
        $db | query db $cmd.query -p $cmd.params
      }
    }
    insert: {
      args: ({
        table: (schema sql ident)
        rows: (schema sql table)
        allow_conflicts: [[nothing {fallback: false}] bool]
      } | schema struct --wrap-missing)
      action: {|cmd|
        let columns = $cmd.rows | columns
        if ($columns | length) != ($cmd.rows | first | columns | length) {
          error make {
            msg: 'invalid query'
            label: {
              text: 'not all columns exist on all rows'
              span: (metadata $cmd.rows)
            }
          }
        }
        let column_list = $columns | str join ', '
        let param_row = $"\(('' | fill -c ', ?' -w ($columns | length) | str substring 2..)\)"
        let values = $param_row ++ ('' | fill -c (",\n" ++ $param_row) -w ($cmd.rows | length | $in - 1))
        let result = $db | query db -p ($cmd.rows | each { values } | flatten) $"
          INSERT INTO ($cmd.table) \(($column_list)\)
          VALUES ($values)
          RETURNING *
        "
        if not $cmd.allow_conflicts and ($result | length) != ($cmd.rows | length) {
          error make {
            msg: 'failed query'
            label: {
              text: 'check constraints'
              span: (metadata $cmd.rows).span
            }
          }
        }
        $result
      }
    }
    update: {
      args: ({
        table: (schema sql ident)
        # TODO: syntax check / generate where clause
        where: [string nothing]
        # TODO: also support keyword parameters
        params: ([[]] | schema array --wrap-single --wrap-null)
        values: (schema sql row)
        single: [[nothing {fallback: false}] bool]
      } | schema struct --wrap-missing)
      action: {|cmd|
        let where = if ($cmd.where | is-empty) {
          ''
        } else {
          $'WHERE ($cmd.where)'
        }
        let updates = $cmd.values | columns | each {|c| $'($c) = ?' } | str join ",\n"
        let result = $db | query db -p (($cmd.values | values) ++ $cmd.params) $"
          UPDATE ($cmd.table)
          SET ($updates)
          ($where)
          RETURNING *
        "
        if $cmd.single and ($result | length) > 1 {
          error make {
            msg: "invalid operation"
            label: {
              text: "condition matches more then one row"
              span: (metadata $cmd.where).span
            }
          }
        }
        $result
      }
    }
    delete: {
      args: ({
        table: (schema sql ident)
        # TODO: syntax check / generate where clause
        where: [string nothing]
        # TODO: also support keyword parameters
        params: ([[]] | schema array --wrap-single --wrap-null)
        single: [[nothing {fallback: false}] bool]
      } | schema struct --wrap-missing)
      delete: {|cmd|
        let where = if ($cmd.where | is-empty) {
          ''
        } else {
          $'WHERE ($cmd.where)'
        }
        let result = $db| query db -p $cmd.params $"
          DELETE FROM ($cmd.table)
          ($where)
          RETURNING *
        "
        if $cmd.single and ($result | length) > 1 {
          error make {
            msg: "invalid operation"
            label: {
              text: "condition matches more then one row"
              span: (metadata $cmd.where).span
            }
          }
        }
        $result
      }
    }
  }}
}
# TODO: add modify commands
# TODO: add view/select commands
# TODO: add cleanup/delete commands
# database related commands
export def 'commands db' [
]: record<sql: record<generic, insert, update, delete>> -> record {
  plugin use schema
  let sql_generic = $in.sql.generic
  let sql_insert = $in.sql.insert
  let sql_update = $in.sql.update
  let sql_delete = $in.sql.delete
  def run [
    query: string
    --params(-p): any
  ]: nothing -> any {
    do $sql_generic.action ({
      query: $query
      params: $params
    } | normalize $sql_generic.args)
  }
  def insert [
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
  def update [
    table: string
    where: string
    values: record
    --params (-p): any
    --single (-s)
  ]: nothing -> any {
    do $sql_update.action ({
      table: $table
      where: $where
      params: $params
      values: $values
      single: $single
    } | normalize $sql_update.args)
  }
  def delete [
    table: string
    where: string
    --params (-p): any
    --single (-s)
  ]: nothing -> any {
    do $sql_delete.action ({
      table: $table
      where: $where
      params: $params
      single: $single
    } | normalize $sql_delete.args)
  }
  def timing-columns [] {
    # NOTE: store date using `$date | format date "%+"` (sql query param will implicit cast)
    # NOTE: load date using `$str | into datetime`
    $"
      (created-at) TEXT NOT NULL DEFAULT\(datetime\('now'\) || 'Z'\),
      (modified-at) TEXT NOT NULL DEFAULT\(datetime\('now'\) || 'Z'\)
    "
  }
  $in | merge {db: {
    init: {
      args: ({
      } | schema struct)
      action: {|cmd|
        run 'PRAGMA foreign_keys = ON'
        run 'PRAGMA recursive_triggers = OFF'
        run $"CREATE TABLE (table version) \(
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
        insert (table version) $versions
        run $"CREATE TABLE (table entity-types) \(
          (column entity-types name) TEXT NOT NULL PRIMARY KEY,
          (column entity-types columns) UNSIGNED_INTEGER NOT NULL CHECK\((column entity-types columns) >= 0\),
          (column entity-types schema) BLOB NOT NULL
        \)"
        run $"CREATE TABLE (table attribute-types) \(
          (column attribute-types name) TEXT NOT NULL PRIMARY KEY,
          (column attribute-types unique) BOOL NOT NULL CHECK\((column attribute-types unique) IN \(0, 1\)\),
          (column attribute-types columns) UNSIGNED_INTEGER NOT NULL CHECK\((column attribute-types columns) >= 0\)
        \)"
        run $"CREATE TABLE (table attributes) \(
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
        run $"CREATE INDEX (index (table attributes) (column attributes type))
          ON (table attributes)\((column attributes type)\)
        "
        run $"
          CREATE TRIGGER (trigger modified (table attributes))
          AFTER UPDATE
          ON (table attributes)
          BEGIN
            UPDATE (table attributes)
            SET (modified-at) = datetime\('now'\) || 'Z'
            WHERE (primary-key) == NEW.(primary-key);
          END
        "
        run $"CREATE TABLE (table type-map entity) \(
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
        run $"CREATE INDEX (index (table type-map entity) (column type-map entity from))
          ON (table type-map entity)\((column type-map entity from)\)
        "
        run $"CREATE INDEX (index (table type-map entity) (column type-map entity to))
          ON (table type-map entity)\((column type-map entity to)\)
        "
        run $"CREATE TABLE (table type-map attribute) \(
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
        run $"CREATE INDEX (index (table type-map attribute) (column type-map attribute from))
          ON (table type-map attribute)\((column type-map attribute from)\)
        "
        run $"CREATE INDEX (index (table type-map attribute) (column type-map attribute to))
          ON (table type-map attribute)\((column type-map attribute to)\)
        "
        run $"CREATE TABLE (table type-map null) \(
          (column type-map null to) TEXT NOT NULL UNIQUE,
          FOREIGN KEY \((column type-map null to)\)
            REFERENCES (table attribute-types) \((column attribute-types name)\)
              ON UPDATE RESTRICT
              ON DELETE RESTRICT
        \)"
        run $"CREATE TABLE (table sources) \(
          (primary-key) INTEGER NOT NULL PRIMARY KEY,
          (column sources value) TEXT NOT NULL UNIQUE,
          (timing-columns)
        \)"
        run $"
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
    version: {
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
          let result = run $"
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
            $row = $row | merge {(column version program): $cmd.program}
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
            $row = $row | merge {(column version data): $cmd.data}
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
            $row = $row | merge {(column version config): $cmd.config}
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
          update (table version) $'(primary-key) == 42' $row
        }
      }
    } # version
    entity-type: {
      add: {
        args: ({
          name: (schema sql ident --strict)
          schema: (schema data-arg)
        } | schema struct --wrap-missing)
        action: {|cmd|
          let columns = $cmd.schema | length
          mut unique = $"UNIQUE\((column entity source)"
          mut data_columns = ''
          for i in 0..<($columns) {
            $unique += $", (column entity data $i)"
            $data_columns += $"(column entity data $i) BLOB NOT NULL,\n"
          }
          $unique += ')'
          run $"CREATE TABLE (table entity $cmd.name) \(
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
          run $"CREATE INDEX (index (table entity $cmd.name) (column entity source))
            ON (table entity $cmd.name)\((column entity source)\)
          "
          run $"
            CREATE TRIGGER (trigger modified (table entity $cmd.name))
            AFTER UPDATE
            ON (table entity $cmd.name)
            BEGIN
              UPDATE (table entity $cmd.name)
              SET (modified-at) = datetime\('now'\) || 'Z'
              WHERE (primary-key) == NEW.(primary-key);
            END
          "
          run $"CREATE VIEW (view entity $cmd.name)
            AS SELECT s.(column sources value) AS (column entity source), e.*
            FROM (table entity $cmd.name) e
            LEFT JOIN (table sources) s ON e.(column entity source) == s.(primary-key)
          "
          insert (table entity-types) {
            (column entity-types name): $cmd.name
            (column entity-types columns): $columns
            (column entity-types schema): ($cmd.schema | schema data-schema | to msgpackz)
          }
        }
      }
      delete: {
        args: ({
          name: (schema sql ident --strict)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          # TODO: drop all map tables using this type
          # TODO: delete rows in maps meta-table
          # TODO: drop entity table
          # TODO: delete row in entity-types
          # TODO: cleanup/reorder freed source ids
          error make {msg: "not implemented"}
        }
      }
    } # entity-type
    attribute-type: {
      add: {
        args: ({
          name: (schema sql ident --strict)
          unique: [[nothing {fallback: false}] bool]
          columns: [[nothing {fallback: 0}] (schema uint)]
        } | schema struct --wrap-missing)
        action: {|cmd|
          insert (table attribute-types) {
            (column attribute-types name): $cmd.name
            (column attribute-types unique): $cmd.unique
            (column attribute-types columns): $cmd.columns
          }
        }
      }
      delete: {
        args: ({
          name: (schema sql ident --strict)
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          # TODO: drop all map tables using this type
          # TODO: delete rows in maps meta-table
          # TODO: delete all rows in attributes using this type
          # TODO: delete row in attribute-types
          # TODO: cleanup/reorder freed attribute ids
          error make {msg: "not implemented"}
        }
      }
    } # attribute-type
    source: {
      # TODO: version that allows adding source twice? (returning existing rows)
      add: {
        args: ({
          value: ('string' | schema array --wrap-single)
        } | schema struct)
        action: {|cmd|
          insert (table sources) ($cmd.value | wrap (column sources value))
        }
      }
      move: {
        args: ({
          from: [string int] # value or id
          to: string
        } | schema struct)
        action: {|cmd|
          let key = if ($cmd.from | describe -d).type == string { # value
            column sources value
          } else { # id
            primary-key
          }
          let result = run -p [$cmd.from] $"
            SELECT COUNT\(*\) AS count
            FROM (table sources)
            WHERE ($key) == ?
          "
          if $result.0.count == 0 {
            error make {
              msg: "invalid operation"
              label: {
                text: $"source not found \(($key)\)"
                span: (metadata $cmd.from).span
              }
            }
          }
          # TODO: manual check if to already exists
          update (table sources) $"($key) == ?" -p [$cmd.from] {
            (column sources value): $cmd.to
          }
        }
      }
      delete: {
        args: ({
          value: [string int] # value or id
          force: [[nothing {fallback: false}] bool]
        } | schema struct --wrap-missing)
        action: {|cmd|
          let key = if ($cmd.value | describe -d).type == string { #value
            column sources value
          } else { # id
            primary-key
          }
          let result = run -p [$cmd.value] $"
            SELECT COUNT\(*\) AS count
            FROM (table sources)
            WHERE ($key) == ?
          "
          if $result.0.count == 0 {
            error make {
              msg: "invalid operation"
              label: {
                text: $"source not found \(($key)\)"
                span: (metadata $cmd.value).span
              }
            }
          }
          delete (table sources) $"($key) == ?" -p [$cmd.value]
        }
      }
    } # source
    entity: {
      add: {
        args: ({
          type: (schema sql ident --strict)
          source: [string int] # name or id
          data: [[]] # match against type->schema
        } | schema struct --wrap-missing)
        action: {|cmd|
          plugin use schema
          let key = if ($cmd.source | describe -d).type == string {
            column sources value
          } else {
            primary-key
          }
          let result = run -p [$cmd.source] $"
            SELECT (primary-key) AS id
            FROM (table sources)
            WHERE ($key) == ?
          "
          if ($result | is-empty) {
            error make {
              msg: "invalid operation"
              label: {
                text: "missing source"
                span: (metadata $cmd.source).span
              }
            }
          }
          let source = $result.0.id
          let result = run -p [$cmd.type] $"
            SELECT (column entity-types schema) AS schema
            FROM (table entity-types)
            WHERE (column entity-types name) == ?
          "
          if ($result | is-empty) {
            error make {
              msg: "invalid operation"
              label: {
                text: "invalid entity type"
                span: (metadata $cmd.type).span
              }
            }
          }
          # NOTE: normalize error message is garbage (doesn't say that normalize is causing it)
          let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
          mut row = {
            (column entity source): $source
          }
          for i in 0..<($data | length) {
            $row = $row | merge {(column entity data $i): ($data | get $i)}
          }
          insert (table entity $cmd.type) $row
        }
      }
      move: {
        args: ({
          type: (schema sql ident --strict)
          from: ({
            source: [string int] # name or id
            data: [[]] # match against type->schema
          } | schema struct --wrap-single --wrap-missing)
          to: ({
            source: [string int nothing] # name or id (optional)
            # TODO: only partially specify data instead of all or nothing
            data: [[]] # match against type->schema (optional)
          } | schema struct --wrap-single --wrap-missing)
        } | schema struct)
        action: {|cmd|
          let result = run -p [$cmd.type] $"
            SELECT (column entity-types schema) AS schema
            FROM (table entity-types)
            WHERE (column entity-types name) == ?
          "
          if ($result | is-empty) {
            error make {
              msg: 'invalid operation'
              label: {
                text: 'unknown entity type'
                span: (metadata $cmd.type).span
              }
            }
          }
          let schema = $result.0.schema | from msgpackz | schema
          let from_data = $cmd.from.data | normalize $schema
          mut row = {}
          if ($cmd.to.source | is-not-empty) {
            let source = if ($cmd.to.source | describe -d).type == string {
              let result = run -p [$cmd.to.source] $"
                SELECT (primary-key) AS id
                FROM (table sources)
                WHERE (column sources value) == ?
              "
              if ($result | is-empty) {
                error make {
                  msg: 'invalid operation'
                  label: {
                    text: 'unknown source'
                    span: (metadata $cmd.to.source).span
                  }
                }
              }
              $result.0.id
            } else {
              $cmd.to.source
            }
            $row = $row | merge {(column entity source): $source}
          }
          if ($cmd.to.data | is-not-empty) {
            let to_data = $cmd.to.data | normalize $schema
            for it in ($to_data | enumerate) {
              $row = $row | merge {(column entity data $it.index): $it.item}
            }
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
          let source = if ($cmd.from.source | describe -d).type == string {
            let result = run -p [$cmd.from.source] $"
              SELECT (primary-key) AS id
              FROM (table sources)
              WHERE (column sources value) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'unknown source'
                  span: (metadata $cmd.from.source).span
                }
              }
            }
            $result.0.id
          } else {
            $cmd.from.source
          }
          mut where = $'(column entity source) == ?'
          for i in 0..<($from_data | length) {
            $where ++= $' AND (column entity data $i) == ?'
          }
          update (table entity $cmd.type) $where -p [$source ...$from_data] $row
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
          let result = run -p [$cmd.type] $"
            SELECT (column attribute-types columns) AS columns
            FROM (table attribute-types)
            WHERE (column attribute-types name) == ?
          "
          if ($result | is-empty) {
            error make {
              msg: "invalid operation"
              label: {
                text: "attribute type not found"
                span: (metadata $cmd.type).span
              }
            }
          }
          if $result.0.columns != ($cmd.schema | length) {
            error make {
              msg: "invalid operation"
              label: {
                text: $"attribute schema does not match column count of attribute type \(($result.0.columns)\)"
                span: (metadata $cmd.schema).span
              }
            }
          }
          insert (table attributes) {
            (column attributes name): $cmd.name
            (column attributes type): $cmd.type
            (column attributes schema): ($cmd.schema | schema data-schema | to msgpackz)
          }
        }
      }
      rename: {
        args: ({
          from: [string int] # name or id
          to: string
        } | schema struct --wrap-missing)
        action: {|cmd|
          let key = if ($cmd.from | describe -d).type == string { # name
            column attributes name
          } else { # id
            primary-key
          }
          let result = run -p [$cmd.from] $"
            SELECT COUNT\(*\) AS count
            FROM (table attributes)
            WHERE ($key) == ?
          "
          if $result.count.0 == 0 {
            error make {
              msg: "invalid operation"
              label: {
                text: $"attribute not found \(($key)\)"
                span: (metadata $cmd.from).span
              }
            }
          }
          update (table attributes) $"($key) == ?" -p [$cmd.from] {
            (column attributes name): $cmd.to
          }
        }
      }
      delete: {
        args: ({
          name: [string int] # name or id
        } | schema struct --wrap-missing)
        action: {|cmd|
          # TODO: get type of attribute
          # TODO: get all maps using the type
          # TODO: delete all mappings using this attribute
          # TODO delete row in attributes
          error make {msg: "not implemented"}
        }
      }
    } # attribute
    map: {
      entity: {
        add: {
          args: ({
            type: (schema sql ident --strict)
            entity: ({
              source: [int string] # name or id
              data: [[]] # match against type->schema
            } | schema struct --wrap-single --wrap-missing)
            attribute: [string int] # name or id
            data: [[]] # match against attribute-type->schema
          } | schema struct --wrap-missing)
          action: {|cmd|
            let result = run -p [$cmd.type] $"
              SELECT (column entity-types schema) AS schema
              FROM (table entity-types)
              WHERE (column entity-types name) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: "entity type not found"
                  span: (metadata $cmd.type).span
                }
              }
            }
            let data = $cmd.entity.data | normalize ($result.0.schema | from msgpackz | schema)
            let source = if ($cmd.entity.source | describe -d).type == string {
              let result = run -p [$cmd.entity.source] $"
                SELECT (primary-key) AS id
                FROM (table sources)
                WHERE (column sources value) == ?
              "
              if ($result | is-empty) {
                error make {
                  msg: "invalid operation"
                  label: {
                    text: "source not found"
                    span: (metadata $cmd.entity.source).span
                  }
                }
              }
              $result.0.id
            } else {
              $cmd.entity.source
            }
            mut where = $'(column entity source) == ?'
            for i in 0..<($data | length) {
              $where += $' AND (column entity data $i) == ?'
            }
            let result = run -p [$source ...$data] $"
              SELECT (primary-key) AS id
              FROM (table entity $cmd.type)
              WHERE ($where)
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: "entity not found"
                  span: (metadata $cmd.entity).span
                }
              }
            }
            let entity = $result.0.id
            let key = if ($cmd.attribute | describe -d).type == string { # name
              column attributes name
            } else { # id
              primary-key
            }
            let result = run -p [$cmd.attribute] $"
              SELECT (primary-key) AS id, (column attributes type) AS type, (column attributes schema) AS schema
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: $"attribute not found \(($key)\)"
                  span: (metadata $cmd.attribute).span
                }
              }
            }
            let attribute = $result.0.id
            let attribute_type = $result.0.type
            let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
            let result = run -p [$cmd.type, $attribute_type] $"
              SELECT COUNT\(*\) AS count
              FROM (table type-map entity)
              WHERE (column type-map entity from) == ? AND (column type-map entity to) == ?
            "
            if $result.0.count == 0 {
              let result = run -p [$attribute_type] $"
                SELECT (column attribute-types unique) AS is_unique, (column attribute-types columns) AS columns
                FROM (table attribute-types)
                WHERE (column attribute-types name) == ?
              "
              let unique = $result.0.is_unique != 0
              let columns = $result.0.columns
              let unique_check = if $unique {
                $",\nUNIQUE\((column map entity from), (column map entity to)\)"
              } else { "" }
              mut data_columns = ""
              for i in 0..<($columns) {
                $data_columns += $"(column map entity data $i) BLOB NOT NULL,\n"
              }
              run $"CREATE TABLE (table map entity $cmd.type $attribute_type) \(
                (primary-key) INTEGER NOT NULL PRIMARY KEY,
                (column map entity from) INTEGER NOT NULL,
                (column map entity to) INTEGER NOT NULL,
                ($data_columns)
                (timing-columns),
                FOREIGN KEY \((column map entity from)\)
                  REFERENCES (table entity $cmd.type) \((primary-key)\)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE,
                FOREIGN KEY \((column map entity to)\)
                  REFERENCES (table attributes) \((primary-key)\)
                    ON UPDATE CASCADE
                    ON DELETE CASCADE
                ($unique_check)
              \)"
              if not $unique {
                run $"CREATE INDEX (index map entity pair $cmd.type $attribute_type)
                  ON (table map entity $cmd.type $attribute_type)\((column map entity from), (column map entity to)\)
                "
              }
              run $"CREATE INDEX (index (table map entity $cmd.type $attribute_type) (column map entity from))
                ON (table map entity $cmd.type $attribute_type)\((column map entity from)\)
              "
              run $"CREATE INDEX (index (table map entity $cmd.type $attribute_type) (column map entity to))
                ON (table map entity $cmd.type $attribute_type)\((column map entity to)\)
              "
              run $"
                CREATE TRIGGER (trigger modified (table map entity $cmd.type $attribute_type))
                AFTER UPDATE
                ON (table map entity $cmd.type $attribute_type)
                BEGIN
                  UPDATE (table map entity $cmd.type $attribute_type)
                  SET (modified-at) = datetime\('now'\) || 'Z'
                  WHERE (primary-key) == NEW.(primary-key);
                END
              "
              run $"CREATE VIEW (view map entity $cmd.type $attribute_type)
                AS SELECT m.(created-at), m.(modified-at), m.(primary-key),
                e.*,
                a.(column attributes name) AS (column map entity to),
                m.*
                FROM (table map entity $cmd.type $attribute_type) m
                LEFT JOIN (table entity $cmd.type) e ON m.(column map entity from) == e.(primary-key)
                LEFT JOIN (table attributes) a ON m.(column map entity to) == a.(primary-key)
              "
              insert (table type-map entity) {
                (column type-map entity from): $cmd.type
                (column type-map entity to): $attribute_type
              }
            }
            mut row = {
              (column map entity from): $entity
              (column map entity to): $attribute
            }
            for it in ($data | enumerate) {
              $row = $row | merge {(column map entity data $it.index): $it.item}
            }
            insert (table map entity $cmd.type $attribute_type) $row
          }
        }
        update: {
          args: ({
            type: (schema sql ident --strict)
            attribute: [string int] # name or id
            id: int
            # TODO: allow to specify partial data only
            data: [[]] # match against attribute-type->schema
          } | schema struct)
          action: {|cmd|
            let result = run -p [$cmd.type] $"
              SELECT COUNT\(*\) AS count
              FROM (table entity-types)
              WHERE (column entity-types name) == ?
            "
            if $result.0.count == 0 {
              error make {
                msg: "invalid operation"
                label: {
                  text: "unkown entity type"
                  span: (metadata $cmd.type).span
                }
              }
            }
            let key = if ($cmd.attribute | describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.attribute] $"
              SELECT (primary-key) AS id, (column attributes type) AS type, (column attributes schema) AS schema
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: "unkown attribute"
                  span: (metadata $cmd.attribute).span
                }
              }
            }
            let attribute = $result.0.id
            let type = $result.0.type
            let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
            let result = run -p [$cmd.type, $type] $"
              SELECT COUNT\(*\) AS count
              FROM (table type-map entity)
              WHERE (column type-map entity from) == ? AND (column type-map entity to) == ?
            "
            if $result.0.count == 0 {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'invalid mapping (table does not exist)'
                }
              }
            }
            mut row = {}
            for it in ($data | enumerate) {
              $row = $row | merge {(column map entity data $it.index): $it.item}
            }
            let result = (update
              (table map entity $cmd.type $type)
              $"(primary-key) == ? AND (column map entity to) == ?"
              -p [$cmd.id, $attribute]
              $row
            )
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'bad or missing mapping id'
                  span: (metadata $cmd.id).span
                }
              }
            }
            $result
          }
        }
        # TODO: commands to remove mappings
      } # entity
      attribute: {
        add: {
          args: ({
            from: [string int] # name or id
            to: [string int]
            data: [[]] # match against to-type->schema
          } | schema struct --wrap-missing)
          action: {|cmd|
            let key = if ($cmd.from | describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.from] $"
              SELECT (primary-key) AS id, (column attributes type) AS type
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'unknown attribute'
                  span: (metadata $cmd.from).span
                }
              }
            }
            let from = $result.0.id
            let from_type = $result.0.type
            let key = if ($cmd.to | describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.to] $"
              SELECT (primary-key) AS id, (column attributes type) AS type, (column attributes schema) AS schema
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'unknown attribute'
                  span: (metadata $cmd.to).span
                }
              }
            }
            let to = $result.0.id
            let to_type = $result.0.type
            let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
            let result = run -p [$from_type, $to_type] $"
              SELECT COUNT\(*\) AS count
              FROM (table type-map attribute)
              WHERE (column type-map attribute from) == ? AND (column type-map attribute to) == ?
            "
            if $result.0.count == 0 {
              let result = run -p [$to_type] $"
                SELECT (column attribute-types unique) AS is_unique, (column attribute-types columns) AS columns
                FROM (table attribute-types)
                WHERE (column attribute-types name) == ?
              "
              let unique = $result.0.is_unique != 0
              let columns = $result.0.columns
              let unique_check = if $unique {
                $",\nUNIQUE\((column map attribute from), (column map attribute to)\)"
              } else { "" }
              mut data_columns = ""
              for i in 0..<($columns) {
                $data_columns += $"(column map attribute data $i) BLOB NOT NULL,\n"
              }
              run $"CREATE TABLE (table map attribute $from_type $to_type) \(
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
                run $"CREATE INDEX (index map attribute pair $from_type $to_type)
                  ON (table map attribute $from_type $to_type)\((column map attribute from), (column map attribute to)\)
                "
              }
              run $"CREATE INDEX (index (table map attribute $from_type $to_type) (column map attribute from))
                ON (table map attribute $from_type $to_type)\((column map attribute from)\)
              "
              run $"CREATE INDEX (index (table map attribute $from_type $to_type) (column map attribute to))
                ON (table map attribute $from_type $to_type)\((column map attribute to)\)
              "
              run $"
                CREATE TRIGGER (trigger modified (table map attribute $from_type $to_type))
                AFTER UPDATE
                ON (table map attribute $from_type $to_type)
                BEGIN
                  UPDATE (table map attribute $from_type $to_type)
                  SET (modified-at) = datetime\('now'\) || 'Z'
                  WHERE (primary-key) == NEW.(primary-key);
                END
              "
              run $"CREATE VIEW (view map attribute $from_type $to_type)
                AS SELECT m.(created-at), m.(modified-at), m.(primary-key),
                  f.(column attribute-types name) AS (column map attribute from),
                  t.(column attribute-types name) AS (column map attribute to),
                  m.*
                FROM (table map attribute $from_type $to_type) m
                LEFT JOIN (table attributes) f ON m.(column map attribute from) == f.(primary-key)
                LEFT JOIN (table attributes) t ON m.(column map attribute to) == t.(primary-key)
              "
              insert (table type-map attribute) {
                (column type-map attribute from): $from_type
                (column type-map attribute to): $to_type
              }
            }
            mut row = {
              (column map attribute from): $from
              (column map attribute to): $to
            }
            for it in ($data | enumerate) {
              $row = $row | merge {(column map attribute data $it.index): $it.item}
            }
            insert (table map attribute $from_type $to_type) $row
          }
        }
        update: {
          args: ({
            from: [string int] # name or id
            to: [string int] # name or id
            id: int
            # TODO: allow to specify partial data only
            data: [[]] # match against attribute-type->schema
          } | schema struct)
          action: {|cmd|
            let key = if ($cmd.from| describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.from] $"
              SELECT (primary-key) AS id, (column attributes type) AS type
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: "unkown attribute"
                  span: (metadata $cmd.from).span
                }
              }
            }
            let from = $result.0.id
            let from_type = $result.0.type
            let key = if ($cmd.to | describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.to] $"
              SELECT (primary-key) AS id, (column attributes type) AS type, (column attributes schema) AS schema
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: "unkown attribute"
                  span: (metadata $cmd.to).span
                }
              }
            }
            let to = $result.0.id
            let to_type = $result.0.type
            let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
            let result = run -p [$from_type, $to_type] $"
              SELECT COUNT\(*\) AS count
              FROM (table type-map attribute)
              WHERE (column type-map attribute from) == ? AND (column type-map attribute to) == ?
            "
            if $result.0.count == 0 {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'invalid mapping (table does not exist)'
                }
              }
            }
            mut row = {}
            for it in ($data | enumerate) {
              $row = $row | merge {(column map attribute data $it.index): $it.item}
            }
            let result = (update
              (table map attribute $from_type $to_type)
              $"(primary-key) == ? AND (column map attribute from) == ? AND (column map attribute to) == ?"
              -p [$cmd.id, $from, $to]
              $row
            )
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'bad or missing mapping id'
                  span: (metadata $cmd.id).span
                }
              }
            }
            $result
          }
        }
      } # attribute
      'null': {
        add: {
          args: ({
            attribute: [string int]
            data: [[]] # match against type->schema
          } | schema struct --wrap-single --wrap-missing)
          action: {|cmd|
            let key = if ($cmd.attribute | describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.attribute] $"
              SELECT (primary-key) AS id, (column attributes type) AS type, (column attributes schema) AS schema
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'unknown attribute'
                  span: (metadata $cmd.attribute).span
                }
              }
            }
            let attribute = $result.0.id
            let type = $result.0.type
            let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
            let result = run -p [$type] $"
              SELECT COUNT\(*\) AS count
              FROM (table type-map null)
              WHERE (column type-map null to) == ?
            "
            if $result.0.count == 0 {
              let result = run -p [$type] $"
                SELECT (column attribute-types unique) AS is_unique, (column attribute-types columns) AS columns
                FROM (table attribute-types)
                WHERE (column attribute-types name) == ?
              "
              let unique = $result.0.is_unique != 0
              let columns = $result.0.columns
              let unique_check = if $unique { 'UNIQUE'  } else { '' }
              mut data_columns = ""
              for i in 0..<($columns) {
                $data_columns += $"(column map null data $i) BLOB NOT NULL,\n"
              }
              run $"CREATE TABLE (table map null $type) \(
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
                run $"CREATE INDEX (index (table map null $type) (column map null to))
                  ON (table map null $type)\((column map null to)\)
                "
              }
              run $"
                CREATE TRIGGER (trigger modified (table map null $type))
                AFTER UPDATE
                ON (table map null $type)
                BEGIN
                  UPDATE (table map null $type)
                  SET (modified-at) = datetime\('now'\) || 'Z'
                  WHERE (primary-key) == NEW.(primary-key);
                END
              "
              run $"CREATE VIEW (view map null $type)
                AS SELECT m.(created-at), m.(modified-at), m.(primary-key),
                  a.(column attribute-types name) AS (column map null to),
                  m.*
                FROM (table map null $type) m
                LEFT JOIN (table attributes) a ON m.(column map null to) == a.(primary-key)
              "
              insert (table type-map null) {
                (column type-map null to): $type
              }
            }
            mut row = {
              (column map null to): $attribute
            }
            for it in ($data | enumerate) {
              $row = $row | merge {(column map null data $it.index): $it.item}
            }
            insert (table map null $type) $row
          }
        }
        update: {
          args: ({
            attribute: [string int] # name or id
            id: int
            # TODO: allow to specify partial data only
            data: [[]] # match against attribute-type->schema
          } | schema struct)
          action: {|cmd|
            let key = if ($cmd.attribute | describe -d).type == string {
              column attributes name
            } else {
              primary-key
            }
            let result = run -p [$cmd.attribute] $"
              SELECT (primary-key) AS id, (column attributes type) AS type, (column attributes schema) AS schema
              FROM (table attributes)
              WHERE ($key) == ?
            "
            if ($result | is-empty) {
              error make {
                msg: "invalid operation"
                label: {
                  text: "unkown attribute"
                  span: (metadata $cmd.attribute).span
                }
              }
            }
            let attribute = $result.0.id
            let type = $result.0.type
            let data = $cmd.data | normalize ($result.0.schema | from msgpackz | schema)
            let result = run -p [$type] $"
              SELECT COUNT\(*\) AS count
              FROM (table type-map null)
              WHERE (column type-map null to) == ?
            "
            if $result.0.count == 0 {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'invalid mapping (table does not exist)'
                }
              }
            }
            mut row = {}
            for it in ($data | enumerate) {
              $row = $row | merge {(column map null data $it.index): $it.item}
            }
            let result = (update
              (table map null $type)
              $"(primary-key) == ? AND (column map null to) == ?"
              -p [$cmd.id, $attribute]
              $row
            )
            if ($result | is-empty) {
              error make {
                msg: 'invalid operation'
                label: {
                  text: 'bad or missing mapping id'
                  span: (metadata $cmd.id).span
                }
              }
            }
            $result
          }
        }
      } # null
    } # map
  }}
}
