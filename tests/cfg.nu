use ../tag-matrix/cfg/ *

export def cmds [] { {
  data-dir: local # of of [local, global]
  use-force: false # allow automatic commands to use force
  schema: {
    entity-types: [
      [name schema];
      [note null]
    ]
    attribute-types: [
      [name   unique columns];
      [tag    true   0]
      [single true   1]
      [multi  false  1]
    ]
    default-attributes: [
      [name type   schema];
      [text single string]
    ]
  }
} | make-commands }

export def view [
  title: string
] {
  let response = cmds | run [[$.db.map.entity.get-all, {
    type: note
    entity: {source: $title}
    attribute: text
  }]]
  let text = $response.0.result.0.data0
  if ($env.PAGER? | is-not-empty) {
    let tmp = mktemp -t $'($title).XXX' --suffix .txt
    $text | save -f $tmp
    ^$nu.current-exe -c $'($env.PAGER) ($tmp)'
    rm $tmp
  } else {
    print -e $text
  }
  null
}

export def edit [
  title: string
] {
  let cmds = cmds
  let response = $cmds | run --result [[$.db.map.entity.get-all, {
    type: note
    entity: {source: $title}
    attribute: text
  }]]
  let text = if $response.status == failed {
    if $response.reason != command {
      error make {msg: $"command failed:\n($response | table -e)"}
    } else { '' }
  } else {
    $response.result.0.result.0.data0
  }
  let tmp = mktemp -t $'($title).XXX' --suffix .txt
  $text | save -f $tmp
  let cmd = $env.VISUAL? | default $env.EDITOR? | default vi
  ^$nu.current-exe -c $'($cmd) ($tmp)'
  if $response.status == success {
    $cmds | run [[$.db.map.entity.update {
      type: note
      attribute: text
      id: $response.result.0.result.0.id
      data: (open $tmp)
    }]]
  } else {
    $cmds | run [
    [$.db.source.add $title]
    [$.db.entity.add {
      type: note
      source: $title
    }]
    [$.db.map.entity.add {
      type: note
      entity: {source: $title}
      attribute: text
      data: (open $tmp)
    }]]
  }
}
