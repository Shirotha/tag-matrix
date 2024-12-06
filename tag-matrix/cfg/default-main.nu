use ../db/schema.nu
use ../db/commands/
use run.nu
use make-commands.nu

def 'normalize main' [
] {
  plugin use schema
  $in | normalize ({
    attribute: [[nothing {fallback: false}] bool]
    entity-type: [nothing (schema sql ident --strict)]
    attribute-type: [nothing (schema sql ident --strict)]
    args: ({all: []} | schema array --wrap-single --wrap-null)
  } | schema struct --wrap-missing)
}

# defines default behaviour when not specified by config
export def main [
  args: record
]: record -> any {
  let cmds = $in
  let args = $args | normalize main
  match $args.args {
    ["run", $cmd, $arg] => ($cmds | run [[$cmd, $arg]]),
    ["run", $cb] => ($cmds | run $cb),
    _ => (error make {
      msg: 'unknown command'
      label: {
        text: 'invalid arguments'
        span: (metadata $args.args).span
      }
    })
  }
  # TODO: infer flags when not given
  # TODO: implement default commands
  # - add --attribute-type atype aname [schema]
  #   add new attribute of given type
  # - add --entity-type etype path/to/file
  #   add new entity of given type
  # - add path/to/file [data]
  #   add new entity with implied type and sub-source
  # - set path/to/file [data] aname [data]
  #   set (add if not exists) attribute to existing entity with implied type
  # - set --attribute aname1 aname2 [data]
  #   set (add if not exists) attribute to other attribute
  # - unset path/to/file aname
  #   remove attribute from entity
  # - unset --attribute aname1 aname2
  #   remove attribute from other attribute
  # - remove path/to/file [data]
  #   remove entity
  # - remove --attribute aname
  #   remove attribute
  # TODO: commands to list/view existing data
  # TODO: process result and do error handling
}
