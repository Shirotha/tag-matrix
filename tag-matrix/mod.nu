export module db/

const BARE_STRING = '\S+'
export const CONFIG_NAME = 'tag-matrix.nu$'
export const DATA_FOLDER = '.tag-matrix'
export const NESTED_CONFIG = $'($DATA_FOLDER)/config.nu'

export-env {
  $env.TAG_MATRIX_CONFIG_DIRS = $env.TAG_MATRIX_CONFIG_DIRS? | default []
}

# look for config file starting from `pwd` and going up.
# valid config files are `CONFIG_NAME` or `NESTED_CONFIG`
def find-config-here [
] {
  let result = ls -f | where type == file and name =~ $CONFIG_NAME
  # NOTE: multiple config files in same folder is undefined behaviour
  if ($result | is-not-empty) { return $result.0.name }
  if ($NESTED_CONFIG | path type) == file { return ($NESTED_CONFIG | path expand) }
  if (pwd | path split | length) <= 1 { return null }
  cd ..
  find-config-here
}

def config-root [
]: path -> path {
  let dir = $in | path dirname
  if ($dir | str ends-with $'(char psep)($DATA_FOLDER)') {
    $dir | path dirname
  } else { $dir }
}

# TODO: how to pass arbitrary flags?
export def main [
  --config (-c): path
  --attribute (-a) # interpret first string as attribute name (instead of source)
  --entity-type (-E): string # specify entity type in case of ambigouity
  --attribute-type (-A): string # specify attribute type
  ...args
] {
  let config = if ($config | is-empty) {
    let result = find-config-here
    if ($result | is-empty) {
      error make {
        msg: 'no valid config found (try using --config)'
      }
    }
    $result
  } else {
    mut found = null
    # TODO: check for absolute paths first
    for root in ((pwd) ++ $env.TAG_MATRIX_CONFIG_DIRS) {
      let result = $root | path join $config
      if ($result | path type) == file {
        $found = $result
        break
      }
    }
    if ($found | is-empty) {
      error make {
        msg: 'config does not exists'
        label: {
          text: 'not found relative to $TAG_MATRIX_CONFIG_DIRS'
          span: (metadata $config).span
        }
      }
    }
    $found
  }
  let command = $args | take while { $in =~ $BARE_STRING }
  let args = {
    pos: ($args | skip ($command | length))
    entity: $entity_type
    attribute: $attribute_type
  }
  $env.TAG_MATRIX_FILE = $config
  $env.TAG_MATRIX_ROOT = $config | config-root
  # NOTE: this breaks if config path includes `'`
  let commands = $"
    module config {
      export use '($config)' *

      export def main [
        --attribute \(-a\): string
        --entity-type \(-E\): string
        --attribute-type \(-A\): string
        ...args
      ] {
        use tag-matrix/cfg/default-main.nu
          cmds | default-main {
          attribute: $attribute
          entity-type: $entity_type
          attribute-type: $attribute_type
          args: $args
        }
      }
    }
    use config
    let args = $in | from msgpack
    \(config ($command | str join ' ')
      (0..<($args.pos | length) | each {|i| $'$args.pos.($i)' } | str join ' ')
      (if $attribute { '--attribute' })
      (if ($entity_type | is-not-empty) { '--entity-type $args.entity' })
      (if ($attribute_type | is-not-empty) { '--attribute-type $args.attribute' })
    \) | table -e | print
  "
  # NOTE: msgpack serialization will convert cell-path into list<string> (fix with into cell-path)
  $args | to msgpack | ^$nu.current-exe --stdin --commands $commands
  # TODO: find a better way to forward result (use environment variable or temporary file?)
  # if $env.LAST_EXIT_CODE == 0 { $result | from msgpack }
}
