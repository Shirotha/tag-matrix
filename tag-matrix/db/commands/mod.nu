export module sql.nu
export module db.nu

# FIXME: causes a variable_not_found error sometimes?
# merge two records recursivly
export def deep-merge [
  rhs: record
]: record -> record {
  let lhs = $in
  mut result = $rhs
  for column in ($lhs | columns) {
    let val = $lhs | get $column
    if $column in $result {
      let other = $result | get $column
      if ($val | describe -d).type == record and ($other | describe -d).type == record {
        $result = $result | update $column { $val | deep-merge $other }
      }
    } else {
      $result = $result | insert $column $val
    }
  }
  $result
}

export def init [
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
