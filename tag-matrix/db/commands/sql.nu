use ../schema.nu

# NOTE: this value is not enforced, try a smaller value then queries are failing
export const SQL_MAX_PARAMETERS = 999

# direct SQL commands (can break database integrity!)
export def main [
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
        def make-query [
          rows: int
        ] {
          let column_list = $columns | str join ', '
          let param_row = $"\(('' | fill -c ', ?' -w ($columns | length) | str substring 2..)\)"
          let values = $param_row ++ ('' | fill -c (",\n" ++ $param_row) -w ($rows - 1))
          $"
            INSERT INTO ($cmd.table) \(($column_list)\)
            VALUES ($values)
            RETURNING *
          "
        }
        if ($columns | length) != ($cmd.rows | first | columns | length) {
          error make {
            msg: 'invalid query'
            label: {
              text: 'not all columns exist on all rows'
              span: (metadata $cmd.rows)
            }
          }
        }
        let chunk_size = $SQL_MAX_PARAMETERS // ($columns | length)
        let query = make-query $chunk_size
        let result = $cmd.rows | chunks $chunk_size | each {|chunk|
          let query = if ($chunk | length) != $chunk_size {
            make-query ($chunk | length)
          } else { $query }
          $db | query db -p ($chunk | each { values } | flatten) $query
        } | flatten
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
    upsert: {
      args: ({
        table: (schema sql ident)
        rows: (schema sql table)
        conflict: (schema sql ident)
      } | schema struct)
      action: {|cmd|
        let columns = $cmd.rows | columns
        def make-query [
          rows: int
        ] {
          let column_list = $columns | str join ', '
          let param_row = $"\(('' | fill -c ', ?' -w ($columns | length) | str substring 2..)\)"
          let values = $param_row ++ ('' | fill -c (",\n" ++ $param_row) -w ($rows - 1))
          let updates = $columns | each { $'($in) = excluded.($in)' } | str join ",\n"
          $"
            INSERT INTO ($cmd.table) \(($column_list)\)
            VALUES ($values)
            ON CONFLICT \(($cmd.conflict)\)
            DO UPDATE SET
            ($updates)
            RETURNING *
          "
        }
        if ($columns | length) != ($cmd.rows | first | columns | length) {
          error make {
            msg: 'invalid query'
            label: {
              text: 'not all columns exist on all rows'
              span: (metadata $cmd.rows)
            }
          }
        }
        if not ($cmd.conflict in $columns) {
          error make {
            msg: 'invalid query'
            label: {
              text: 'conflict has to be a column in rows'
              span: (metadata $cmd.conflict).span
            }
          }
        }
        let chunk_size = $SQL_MAX_PARAMETERS // ($columns | length)
        let query = make-query $chunk_size
        $cmd.rows | chunks $chunk_size | each {|chunk|
          let query = if ($chunk | length) != $chunk_size {
            make-query ($chunk | length)
          } else { $query }
          $db | query db -p ($chunk | each { values } | flatten) $query
        } | flatten
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
      action: {|cmd|
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
