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
      {type: etype_simple, from: 1, to: 'Source E'}
      {type: etype_complex, from: {source: 3, data: [1 a]}, to: 5}
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
      {type: etype_simple, entity: 2, attribute: a_single_alt, data: 'value'}
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

# create colored debug representation
def colorize [
]: any -> string {
  debug | nu-highlight
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
] {
  if ($label | is-not-empty) {
    let label = $'[(ansi reset)(ansi green_bold) ($label) (ansi reset)(ansi blue)]'
    let label = $label | fill --character '=' --alignment center --width (term size).columns
    print -e $'(ansi blue)($label)(ansi reset)'
  }
  print -e (stor collect --no-timings | table --expand --index false)
}

# execute command my name
export def run [
  cmd: cell-path
  args: record
]: record -> any {
  plugin use schema
  print -e $'run command ($cmd | colorize)'
  print -e $'arguments: ($args | colorize)'
  let cmd = $in | get $cmd
  let t = date now
  let args = $args | normalize $cmd.args
  print -e $'applied schema in (ansi blue)((date now) - $t)(ansi reset)'
  let t = date now
  let result = do $cmd.action $args
  print -e $'ran command in (ansi blue)((date now) - $t)(ansi reset)'
  $result
}


export def main [
  --save (-s): string # store final database in file
  --modify (-m) # test modifiing data
  --delete (-d) # test deletion of database
] {
  stor reset
  let cmds = commands init | commands sql | commands db
  $cmds | run $.db.init {}
  for et in (data entity-types) {
    $cmds | run $.db.entity-type.add $et
  }
  # TODO: test invalid entity types
  for at in (data attribute-types) {
    $cmds | run $.db.attribute-type.add $at
  }
  # TODO: test invalid attribute types
  for a in (data attributes) {
    $cmds | run $.db.attribute.add $a
  }
  # TODO: test invalid attributes
  $cmds | run $.db.source.add (data sources)
  for e in (data entities) {
    $cmds | run $.db.entity.add $e
  }
  # TODO: test invalid entities
  for m in (data map entities) {
    $cmds | run $.db.map.entity.add $m
  }
  # TODO: test invalid entity mappings
  for m in (data map attributes) {
    $cmds | run $.db.map.attribute.add $m
  }
  # TODO: test invalid attribute mappings
  for m in (data map null) {
    $cmds | run $.db.map.null.add $m
  }
  # TODO: test invalid null mappings
  stor print -l 'after setup'
  if $modify {
    $cmds | run $.db.version.update {program: '1.0', data: '2', config: '0.2'}
    for s in (data sources --move) {
      $cmds | run $.db.source.move $s
    }
    for e in (data entities --move) {
      $cmds | run $.db.entity.move $e
    }
    # TODO: test invalid entity moves
    for a in (data attributes --rename) {
      $cmds | run $.db.attribute.rename $a
    }
    # TODO: test invalid attribute renames
    for m in (data map entities --update) {
      $cmds | run $.db.map.entity.update $m
    }
    # TODO: test invalid entity map updates
    for m in (data map attributes --update) {
      $cmds | run $.db.map.attribute.update $m
    }
    # TODO: test invalid attribute map updates
    for m in (data map null --update) {
      $cmds | run $.db.map.null.update $m
    }
    # TODO: test invalid null map updates
    stor print -l 'after changes'
  }
  if $delete {
    # TODO: test deleting everything
    stor print -l 'after deletion'
  }
  if ($save | is-not-empty) {
    stor export -f $save
  }
}
