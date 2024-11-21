use commands.nu *

# give test arguments for entity types
export def 'data entity-types' [
  --invalid (-i) # return invalid data instead
] {
  if $invalid {[
    {name: '[invalid entity type name]'}
    {name: et_neg_cols, columns: -1}
  ]} else {[
    {name: etype_simple}
    {name: 'etype_complex', schema: [int string]}
  ]}
}
# give test arguments for attribute types
export def 'data attribute-types' [
  --invalid (-i) # return invalid data instead
] {
  if $invalid {[
    {name: '[invalid attribute type name]', columns: 0, unique: false}
    {name: 'at_neg_cols', columns: -1, unique: false}
    {name: 'at_unique_not_bool', columns: 1, unique: 'true'}
  ]} else {[
    {name: atype_tag, columns: 0, unique: true}
    {name: atype_single, columns: 1, unique: true}
    {name: atype_multi, columns: 1, unique: false}
  ]}
}
# give test arguments for attributes
export def 'data attributes' [
  --invalid (-i) # return invalid data instead
  --rename (-r) # return rename operations instead
] {
  if $rename {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {from: 1, to: 'renamed by id'}
      {from: a_multi, to: 'renamed by name'}
    ]}
  } else {
    if $invalid {[
      {name: a_bad_type, type: unknown_atype}
      {name: a_schema_mismatch, type: a_tag, schema: int}
    ]} else {[
      {name: a_tag, type: atype_tag}
      {name: a_single, type: atype_single, schema: int}
      {name: a_single_alt, type: atype_single, schema: string}
      {name: a_multi, type: atype_multi, schema: string}
    ]}
  }
}
# give test arguments for sources
export def 'data sources' [
  --move (-m) # return moving operations instead
] {
  if $move {[
    {from: 2, to: 'moved by id'}
    {from: 'Source C', to: 'moved by name'}
  ]} else {
    {value: ((seq char A E) | each {|it| 'Source ' + $it })}
  }
}
export def 'data entities' [
  --invalid (-i) # return invalid data instead
  --move (-m) # return moving operations instead
] {
  if $move {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {type: etype_simple, from: {source: 1}, to: 'Source E'}
      {type: etype_complex, from: {source: 3, data: [1 a]}, to: {source: 5}}
      {type: etype_complex, from: {source: 3, data: [2 b]}, to: {data: [3 c]}}
      {type: etype_complex, from: {source: 4, data: [1 a]}, to: {source: 5, data: [4 d]}}
    ]}
  } else {
    if $invalid {[
      {type: 'invalid entity type', source: 1}
      {type: etype_simple, source: -1}
      {type: etype_simple, source: 'unknown source'}
      {type: etype_complex, source: 1, data: 'wrong data'}
    ]} else {[
      {type: etype_simple, source: 1}
      {type: etype_simple, source: 2}
      {type: etype_complex, source: 3, data: [1 a]}
      {type: etype_complex, source: 3, data: [2 b]}
      {type: etype_complex, source: 'Source D', data: [1 a]}
    ]}
  }
}
export def 'data map entities' [
  --invalid (-i) # return invalid data instead
  --update (-u) # return update operations instead
] {
  if $update {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {type: etype_simple, attribute: 2, id: 1, data: 42}
      {type: etype_complex, attribute: 4, id: 2, data: 'changed text'}
    ]}
  } else {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {type: etype_simple, entity: 1, attribute: a_tag}
      {type: etype_complex, entity: {source: 3, data: [1 a]}, attribute: 1}
      {type: etype_complex, entity: {source: 'Source C', data: [2 b]}, attribute: a_single, data: 1}
      {type: etype_simple, entity: 'Source B', attribute: a_single, data: 2}
      {type: etype_simple, entity: {source: 2}, attribute: a_single_alt, data: 'value'}
      {type: etype_complex, entity: {source: 4, data: [1 a]}, attribute: a_multi, data: 'some text'}
      {type: etype_complex, entity: {source: 'Source D', data: [1 a]}, attribute: 4, data: 'other text'}
    ]}
  }
}
export def 'data map attributes' [
  --invalid (-i) # return invalid data instead
  --update (-u) # return update operations instead
] {
  if $update {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {from: 3, to: 2, id: 1, data: 69}
    ]}
  } else {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {from: a_single_alt, to: a_tag}
      {from: 4, to: 1}
      {from: 3, to: a_single, data: 42}
    ]}
  }
}
export def 'data map null' [
  --invalid (-i) # return invalid data instead
  --update (-u) # return update operations instead
] {
  if $update {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {attribute: 'renamed by name', id: 2, data: 'changed value'}
    ]}
  } else {
    if $invalid {[
      # TODO: setup invalid cases
    ]} else {[
      {attribute: a_tag}
      {attribute: 2, data: 12}
      {attribute: 4, data: 'null value'}
      {attribute: a_multi, data: 'also null value'}
    ]}
  }
}

# execute command my name
export def run [
  cmd: cell-path
  args: any
  --silent (-s): any # bool
]: record -> any {
  plugin use schema
  let silent = $silent | default false
  if not $silent {
    print -e $'run command ($cmd | colorize)'
    print -e $'arguments: ($args | colorize)'
  }
  let cmd = $in | get $cmd
  let t = date now
  let args = $args | normalize $cmd.args
  if not $silent {
    print -e $'applied schema in (ansi blue)((date now) - $t)(ansi reset)'
  }
  let t = date now
  let result = do $cmd.action $args
  if not $silent {
    print -e $'ran command in (ansi blue)((date now) - $t)(ansi reset)'
  }
  $result
}

# create colored debug representation
def colorize [
]: any -> string {
  debug | nu-highlight
}
export def 'commands debug' [
]: record<db> -> record {
  plugin use schema
  let cmds = $in
  def run [
    args: any
  ]: cell-path -> any {
    let cmd = $in
    let cmd = $cmds | get $cmd
    do $cmd.action ($args | normalize $cmd.args)
  }
  $cmds | merge {debug: {
    print: {
      version: {
        args: ({} | schema struct)
        action: {|cmd|
          let version = $.db.version.get | run {}
          print -e ($version | table)
        }
      }
      entity-types: {
        args: ({} | schema struct)
        action: {|cmd|
          let types = $.db.entity-type.list | run {}
            | update schema { table -e -i false }
          print -e ($types | table -e -i false)
        }
      }
      attribute-types: {
        args: ({} | schema struct)
        action: {|cmd|
          let types = $.db.attribute-type.list | run {}
          print -e ($types | table -e -i false)
        }
      }
      attributes: {
        args: ({
          skip: ('int' | schema array --wrap-null)
        } | schema struct --wrap-single --wrap-missing)
        action: {|cmd|
          let attributes = $.db.attribute.list | run {}
            | where { not ($in.id in $cmd.skip) }
            | each { select name type }
          print -e ($attributes | table -e -i false)
        }
      }
      tree: {
        args: ({
        } | schema struct)
        action: {|cmd|
          def print-subtree [
            map: record
            --prefix: string
            --seen: list<int>
            --last
          ] {
            let prefix = $prefix | default ''
            let marker = if $last { " \u{2514}" } else { " \u{251C}" }
            let data = if $map.data_columns != 0 {
              0..<($map.data_columns) | each {|i| $map | get $'data($i)' } | colorize
            } else { '' }
            print -e $'($prefix)($marker)(ansi green)($map.to)(ansi reset)($data)'
            mut seen = $seen | default []
            let expand = not ($map.'to:id' in $seen)
            $seen ++= $map.'to:id'
            if $expand {
              let prefix = $prefix ++ (if $last { '  ' } else { " \u{2502}" })
              let children = $.db.map.attribute.list | run $map.'to:id'
              let children = $children | columns | reduce -f [] {|type, all| $all ++ ($children | get $type) }
              for child in ($children | enumerate) {
                $seen = if $child.index + 1 == ($children | length) {
                  print-subtree $child.item --prefix $prefix --seen $seen --last
                } else {
                  print-subtree $child.item --prefix $prefix --seen $seen
                }
              }
            }
            $seen
          }
          let entities = $.db.entity.list | run {}
          let types = $entities | columns
          mut seen = []
          for type in $types {
            let type = $.db.entity-type.get | run $type
            print -e $'(ansi green_bold)($type.name)(ansi reset)'
            let cmp = $.source ++ (0..<($type.subsource_columns) | each {|i| [$'subsource($i)'] | into cell-path })
            for entity in ($entities | get $type.name | sort-by ...$cmp) {
              let subsources = 0..<($type.subsource_columns) | each {|i| $entity | get $'subsource($i)' | colorize }
              print -e ('  ' ++ (($"(ansi green)($entity.source)(ansi reset)" ++ $subsources) | str join ':'))
              let attributes = $.db.map.entity.list | run {type: $type.name, entity: $entity.id}
              let attributes = $attributes | columns | reduce -f [] {|type, all| $all ++ ($attributes | get $type) }
              for map in ($attributes | enumerate) {
                let remap = $map.item | rename -c {attribute: to, 'attribute:id': 'to:id'}
                $seen = if $map.index + 1 == ($attributes | length) {
                  print-subtree $remap --prefix '  ' --seen $seen --last
                } else {
                  print-subtree $remap --prefix '  ' --seen $seen
                }
              }
            }
          }
          $seen
        }
      }
    } # print
  }}
}
export def 'stor collect' [
  --no-timings (-t)
] {
  use naming.nu *
  mut result = {}
  let tables = stor open | query db 'SELECT name FROM sqlite_master WHERE type IN ("table", "view")' | get name
  for table in $tables {
    mut data = stor open | query db $'SELECT * FROM ($table | escape)'
    let columns = $data | columns
    mut duplicates = $columns | where {|it| $it =~ ':[0-9]+$' }
    # TODO: check for all different schema column names
    if 'schema' in $columns {
      $data = $data | merge ($data | get schema | each { from msgpackz | get items } | wrap schema)
    }
    if (created-at) in $columns {
      if $no_timings {
        $duplicates ++= (created-at)
      } else {
        $data = $data | merge ($data | get (created-at) | each { into datetime } | wrap (created-at))
      }
    }
    if (modified-at) in $columns {
      if $no_timings {
        $duplicates ++= (modified-at)
      } else {
        $data = $data | merge ($data | get (modified-at) | each { into datetime } | wrap (modified-at))
      }
    }
    $result = $result | merge {($table): ($data | reject ...$duplicates)}
  }
  $result
}
# print the in-memory sqlite database
export def 'stor print' [
  --label (-l): string
  --cmds: record<debug>
] {
  if ($label | is-not-empty) {
    let label = $'[(ansi reset)(ansi green_bold) ($label) (ansi reset)(ansi blue)]'
    let label = $label | fill --character '=' --alignment center --width (term size).columns
    print -e $'(ansi blue)($label)(ansi reset)'
  }
  if ($cmds | is-empty) {
    print -e (stor collect --no-timings | table --expand --index false)
  } else {
    print -e $'(ansi green_underline)version(ansi reset)'
    $cmds | run $.debug.print.version {} --silent true
    print -e $'(ansi green_underline)entity types(ansi reset)'
    $cmds | run $.debug.print.entity-types {} --silent true
    print -e $'(ansi green_underline)attribute types(ansi reset)'
    $cmds | run $.debug.print.attribute-types {} --silent true
    print -e $'(ansi green_underline)entities(ansi reset)'
    let seen_attributes = $cmds | run $.debug.print.tree {} --silent true
    print -e $'(ansi green_underline)unused attributes(ansi reset)'
    $cmds | run $.debug.print.attributes $seen_attributes --silent true
  }
}

export def main [
  --save (-s): string # store final database in file
  --modify (-m) # test modifiing data
  --delete (-d) # test deletion of database
  --profile (-p) # print profiling  information
] {
  stor reset
  let cmds = commands init | commands sql | commands db | commands debug
  $cmds | run $.db.init {} --silent (not $profile)
  for et in (data entity-types) {
    $cmds | run $.db.entity-type.add $et --silent (not $profile)
  }
  # TODO: test invalid entity types
  for at in (data attribute-types) {
    $cmds | run $.db.attribute-type.add $at --silent (not $profile)
  }
  # TODO: test invalid attribute types
  for a in (data attributes) {
    $cmds | run $.db.attribute.add $a --silent (not $profile)
  }
  # TODO: test invalid attributes
  $cmds | run $.db.source.add (data sources) --silent (not $profile)
  for e in (data entities) {
    $cmds | run $.db.entity.add $e --silent (not $profile)
  }
  # TODO: test invalid entities
  for m in (data map entities) {
    $cmds | run $.db.map.entity.add $m --silent (not $profile)
  }
  # TODO: test invalid entity mappings
  for m in (data map attributes) {
    $cmds | run $.db.map.attribute.add $m --silent (not $profile)
  }
  # TODO: test invalid attribute mappings
  for m in (data map null) {
    $cmds | run $.db.map.null.add $m --silent (not $profile)
  }
  # TODO: test invalid null mappings
  stor print -l 'after setup' --cmds $cmds
  if $modify {
    $cmds | run $.db.version.update {program: '1.0', data: '2', config: '0.2'} --silent (not $profile)
    for s in (data sources --move) {
      $cmds | run $.db.source.move $s --silent (not $profile)
    }
    for e in (data entities --move) {
      $cmds | run $.db.entity.move $e --silent (not $profile)
    }
    # TODO: test invalid entity moves
    for a in (data attributes --rename) {
      $cmds | run $.db.attribute.rename $a --silent (not $profile)
    }
    # TODO: test invalid attribute renames
    for m in (data map entities --update) {
      $cmds | run $.db.map.entity.update $m --silent (not $profile)
    }
    # TODO: test invalid entity map updates
    for m in (data map attributes --update) {
      $cmds | run $.db.map.attribute.update $m --silent (not $profile)
    }
    # TODO: test invalid attribute map updates
    for m in (data map null --update) {
      $cmds | run $.db.map.null.update $m --silent (not $profile)
    }
    # TODO: test invalid null map updates
    stor print -l 'after changes' --cmds $cmds
  }
  if $delete {
    $cmds | run $.db.map.null.delete {attribute: 2, id: 1} --silent (not $profile)
    $cmds | run $.db.map.entity.delete {type: etype_simple, attribute: 1, id: 1} --silent (not $profile)
    $cmds | run $.db.attribute.delete {attribute: a_single_alt} --silent (not $profile)
    $cmds | run $.db.entity.delete {type: etype_simple, entity: 2, force: true} --silent (not $profile)
    $cmds | run $.db.source.delete {value: 5, force: true} --silent (not $profile)
    $cmds | run $.db.attribute-type.delete {name: atype_multi, force: true} --silent (not $profile)
    $cmds | run $.db.entity-type.delete {name: etype_complex, force: true} --silent (not $profile)
    $cmds | run $.db.clean {} --silent (not $profile)
    stor print -l 'after deletion' --cmds $cmds
  }
  if ($save | is-not-empty) {
    stor export -f $save
  }
}
