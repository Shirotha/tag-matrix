const SQLITE_IDENT = '^(?<ident>[a-zA-Z_][a-zA-Z0-9_]*)$'
const SQLITE_ESCAPED = '^\[(?<inner>.*?)\]$'
const IDENTIFIER = $'($SQLITE_IDENT)|($SQLITE_ESCAPED)'

export def is-valid [
  --no-escape (-e) # disallow escaped identifiers
]: any -> bool {
  if ($in | describe -d).type != string { return false }
  $in =~ if $no_escape { $SQLITE_IDENT } else { $IDENTIFIER }
}
export def check [
]: any -> string {
  if not ($in | is-valid) {
    error make {
      msg: "malformatted query"
      label: {
        text: "invalid identifier"
        span: (metadata $in).span
      }
    }
  }
  $in
}
export def escape [
  --force (-f)
]: string -> string {
  if $force or $in !~ $IDENTIFIER {
    $'[($in)]'
  } else {
    $in
  }
}
export def unescape [
]: string -> string {
  let s = $in
  $s | parse -r $SQLITE_ESCAPED | get 0?.inner | default $s
}
def check-non-negative [
]: int -> int {
  if $in < 0 {
    error make {
      msg: "malformatted query"
      label: {
        text: "invalid data column number"
        span: (metadata $in).span
      }
    }
  } else {
    $in
  }
}

export def primary-key [
]: nothing -> string { 'id' }
export def created-at [
]: nothing -> string { 'created_at' }
export def modified-at [
]: nothing -> string { 'modified_at' }
export def index [
  table: string,
  column: string
]: nothing -> string { $'[($table | check | unescape):($column | check | unescape)]' }
export def 'trigger modified' [
  table: string
]: nothing -> string { $'[($table | check | unescape)->modified]' }

export def 'table version' [
]: nothing -> string { 'version' }
export def 'column version program' [
]: nothing -> string { 'program' }
export def 'column version data' [
]: nothing -> string { 'data' }
export def 'column version config' [
]: nothing -> string { 'config' }

export def 'table entity-types' [
]: nothing -> string { 'entity_types' }
export def 'column entity-types name' [
]: nothing -> string { 'name' }
export def 'column entity-types columns' [
]: nothing -> string { 'data_column_count' }
export def 'column entity-types schema' [
]: nothing -> string { 'schema' }

export def 'table attribute-types' [
]: nothing -> string { 'attribute_types' }
export def 'column attribute-types name' [
]: nothing -> string { 'name' }
export def 'column attribute-types unique' [
]: nothing -> string { 'is_unique' }
export def 'column attribute-types columns' [
]: nothing -> string { 'data_column_count' }

export def 'table type-map entity' [
]: nothing -> string { 'entity_maps' }
export def 'column type-map entity from' [
]: nothing -> string { 'entity' }
export def 'column type-map entity to' [
]: nothing -> string { 'attribute' }
export def 'table type-map attribute' [
]: nothing -> string { 'attribute_maps' }
export def 'column type-map attribute from' [
]: nothing -> string { '[from]' }
export def 'column type-map attribute to' [
]: nothing -> string { '[to]' }
export def 'table type-map null' [
]: nothing -> string { 'null_maps' }
export def 'column type-map null to' [
]: nothing -> string { 'attribute' }

export def 'table sources' [
]: nothing -> string { 'sources' }
export def 'column sources value' [
]: nothing -> string { 'value' }

export def 'table entity' [
  type: string
]: nothing -> string { $'[entity.($type | check)]' }
export def 'column entity source' [
]: nothing -> string { 'source' }
export def 'column entity data' [
  i: int
]: nothing -> string { $'sub_source($i | check-non-negative)' }
export def 'view entity' [
  type: string
]: nothing -> string { $'[@(table entity $type | unescape)]' }
export def 'table attributes' [
]: nothing -> string { $'attributes' }
export def 'column attributes name' [
]: nothing -> string { 'name' }
export def 'column attributes type' [
]: nothing -> string { 'type' }
export def 'column attributes schema' [
]: nothing -> string { 'schema' }

export def 'table map entity' [
  entity: string
  attribute: string
]: nothing -> string { $'[entity.($entity | check) -> ($attribute | check)]' }
export def 'column map entity from' [
]: nothing -> string { 'entity' }
export def 'column map entity to' [
]: nothing -> string { 'attribute' }
export def 'column map entity data' [
  i: int
]: nothing -> string { $'data($i | check-non-negative)' }
export def 'index map entity pair' [
  entity: string
  attribute: string
]: nothing -> string { $'[(table map entity $entity $attribute | unescape):pair]' }
export def 'view map entity' [
  entity: string
  attribute: string
]: nothing -> string { $'[@(table map entity $entity $attribute | unescape)]' }
export def 'table map attribute' [
  from: string
  to: string
]: nothing -> string { $'[attribute.($from | check) -> ($to | check)]' }
export def 'column map attribute from' [
]: nothing -> string { '[from]' }
export def 'column map attribute to' [
]: nothing -> string { '[to]' }
export def 'column map attribute data' [
  i: int
]: nothing -> string { $'data($i | check-non-negative)' }
export def 'index map attribute pair' [
  from: string
  to: string
]: nothing -> string { $'[(table map attribute $from $to | unescape):pair]' }
export def 'view map attribute' [
  from: string
  to: string
]: nothing -> string { $'[@(table map attribute $from $to | unescape)]' }
export def 'table map null' [
  attribute: string
]: nothing -> string { $'[null -> ($attribute | check)]' }
export def 'column map null to' [
]: nothing -> string { 'attribute' }
export def 'column map null data' [
  i: int
]: nothing -> string { $'data($i | check-non-negative)' }
export def 'view map null' [
  attribute: string
]: nothing -> string { $'[@(table map null $attribute | unescape)]' }

