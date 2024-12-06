export module run.nu
export module make-commands.nu

# TODO: use data types of subsource in matching process
# try to guess entity type from (sub-)source
export def guess-entity-type [
  source: string
  data: list
  --patterns (-p): record # map of regex -> entity-type
]: table<name, columns> -> record {
  let options = $in | where columns == ($data | length)
  match ($options | length) {
    0 => { return {err: missing, reason: data} },
    1 => { return {ok: ($options | get 0.name)} },
  }
  if ($patterns | is-empty) { return {err: ambiguous, reason: data, options: $options.name} }
  let matching = $patterns
    | items {|regex, entity| if $source =~ $regex { $entity } }
    | compact
  let options = $options | where name in $matching
  match ($options | length) {
    0 => {err: missing, reason: pattern},
    1 => {ok: $options.0.name},
    _ => {err: ambiguous, reason: pattern, options: $options.name}
  }
}

# TODO: helper to extract data from run result (and do error handling on failure)
