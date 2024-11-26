use naming.nu *

# schema that matches unsigned integer
export def uint [
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
export def data-arg [
]: nothing -> any {
  plugin use schema
  {all: []} | schema array --wrap-single --wrap-null
}
# creates schema for data columns from arg
export def data-schema [
]: any -> any {
  plugin use schema
  $in | schema tuple --wrap-single --wrap-null
}
# schema that matches valid SQLite identifier
export def 'sql ident' [
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
export def 'sql value' [
]: nothing -> any {
  plugin use schema
  # TODO: restrict to valid sql values
  [[]] | schema
}
# schema that matches SQLite record
export def 'sql row' [
]: nothing -> any {
  plugin use schema
  [(sql ident) (sql value)] | schema map --length 1..
}
# schema that matches list of SQLite records
export def 'sql table' [
]: nothing -> any {
  plugin use schema
  sql row | schema array --wrap-single --length 1..
}
# schema that matches source identification
export def source [
]: nothing -> any {
  plugin use schema
  [string int] | schema
}
# schema that matches entity identification
export def entity [
]: nothing -> any {
  # NOTE: int will be treated as id, not as source because first valid pattern will be matched
  [int ({
    source: (source)
    data: [[]] # match against type->schema
  } | schema struct --wrap-single --wrap-missing)] | schema
}
# schema that matches attribute identification
export def attribute [
]: nothing -> any {
  plugin use schema
  [string int] | schema
}
