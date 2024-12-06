# TODO: disallow '**.**.x'
const SINGLE = '\*?([a-zA-Z_][a-zA-Z0-9_]*\*?)*'
const PART = $"\(($SINGLE)|\\*\\*\)"
const WILDCARD_PATTERN = $"^\(($PART)\\.\)*($SINGLE)$"

def wildcard-predicate [
]: string -> closure {
  mut pattern = '^'
  for part in ($in | split row '.') {
    if $part == '**' {
      $pattern += '[a-zA-Z0-9_.]*?'
    } else {
      $pattern += $part | str replace --all '*' '[a-zA-Z0-9_]*'
    }
    $pattern += '\.'
  }
  let pattern = $pattern
    | str substring 0..<($pattern | str length | $in - 2)
    | $in + '$'
  { $in =~ $pattern }
}

export def 'normalize command-buffer' [
  cmds: record
] {
  plugin use schema
  let command = {
      path: [cell-path nothing [string {
        split row '.' | into cell-path
      }]]
      args: schema
      action: closure # primary action
      rollback: [closure nothing] # run during rollback
      commit: [closure nothing] # run after all other commands (can't rollback this)
    } | schema struct --wrap-missing
  $in | normalize ([[
    cell-path
    [string {
      {ok: ($in | split row '.' | into cell-path)}
    }]
    [('string' | schema array) {
      {ok: ($in | into cell-path)}
    }]
    $command
  ], {all: []} # arguments (match against 0.args)
  ] | schema tuple --wrap-missing --wrap-single
    | schema array --wrap-single --length 1..
  )  | each {|it|
    # NOTE: cmd has to be constructed here because custom schema can't capture custom values
    let cmd = if ($it.0 | describe -d).type == cell-path {
      $cmds | get $it.0
        | default null rollback
        | default null commit
        | insert path $it.0
    } else { $it.0 }
    [$cmd, ($it.1 | normalize $cmd.args)]
  }
}
def 'normalize events' [
] {
  plugin use schema
  $in | normalize ({
    timing: [{value: before} {value: after}]
    trigger: [
      closure # predicate with path input
      [cell-path { # trigger on equality
        let path = $in
        {ok: { $in == $path }}
      }]
      [string { # trigger on glob match
        if $in !~ $WILDCARD_PATTERN {
          return {err: 'invalid wildcard pattern (see `glob`)'}
        }
        $in | wildcard-predicate
      }]
    ]
    action: closure # primary action
    rollback: [closure nothing] # run during rollback
    commit: [closure nothing] # run after all other commands (can't rollback this)
  } | schema struct --wrap-missing
    | schema array --wrap-single --wrap-null)
}

# TODO: define read-only version that doesn't need to worry about fallback
# execute a chain of internal commands as a single transaction (will rollback on error)
export def main [
  cb: any # list of commands
  --events (-e): list # list of events
  --result (-r) # instead of throwing error, return result with error report instead
]: record<sql> -> any {
  let cmds = $in
  def run [
    cmd: cell-path
    args: any
  ] {
    let cmd = $cmds | get $cmd
    do $cmd.action ($args | normalize $cmd.args)
  }
  let cb = $cb | normalize command-buffer $cmds
  let events = $events | normalize events
  mut results = []
  mut rollbacks = []
  mut commits = []
  run sql.generic 'BEGIN TRANSACTION'
  let error = try {
    # TODO: currently this will only rollback completed commands, the last (currently running) command is not rolled back
    for cmd in $cb {
      let events = $events
        | where { do $in.trigger $cmd.path }
        | group-by timing
      for event in $events.before? {
        do $event.action $cmd.path
        if ($event.rollback | is-not-empty) {
          $rollbacks = $rollbacks | prepend [$event.rollback, $cmd.0.path $cmd.1]
        }
        if ($event.commit | is-not-empty) {
          $commits = $commits | append [$event.commit, $cmd.0.path $cmd.1]
        }
      }
      let result = do $cmd.0.action $cmd.1
      $results ++= {cmd: $cmd.0.path, args: $cmd.1, result: $result}
      if ($cmd.0.rollback | is-not-empty) {
        $rollbacks = $rollbacks | prepend [$cmd.0.rollback, $cmd.1, $result]
      }
      if ($cmd.0.commit | is-not-empty) {
        $commits = $commits | append [$cmd.0.commit, $cmd.1, $result]
      }
      for event in $events.after? {
        do $event.action $cmd.path $result
        if ($event.rollback | is-not-empty) {
          $rollbacks = $rollbacks | prepend [$event.rollback, $cmd.0.path $cmd.1 $result]
        }
        if ($event.commit | is-not-empty) {
          $commits = $commits | append [$event.commit, $cmd.0.path $cmd.1 $result]
        }
      }
    }
    null
  } catch {|e| $e}
  let response = if ($error | is-empty) {
    let result = try {
      for commit in $commits { do $commit.0 ...($commit | skip 1) }
      {status: success, result: $results}
    } catch {|c|
      {status: failed, reason: commit, commit-error: $c}
    }
    run sql.generic 'COMMIT'
    $result
  } else {
    let result = try {
      for rollback in $rollbacks { do $rollback.0 ...($rollback | skip 1) }
      {status: failed, reason: command, error: $error}
    } catch {|r|
      {status: failed, reason: rollback, error: $error, rollback-error: $r}
    }
    run sql.generic 'ROLLBACK'
    $result
  }
  if not $result {
    if $response.status == failed {
      # TODO: print original errors better
      error make {msg: (match $response.reason {
        command => $'commands were rollbacked because of error: ($response.error)'
        rollback => $"error during rollback process: ($response.rollback-error)\noriginal error: ($response.error)"
        commit => $'error during commit process: ($response.commit-error)'
      })}
    } else { $response.result }
  } else { $response }
}
