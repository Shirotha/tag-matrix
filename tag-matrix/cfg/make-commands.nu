use ../db/commands/
use ../ DATA_FOLDER
use run.nu

export const DB_FILE = 'db.sqlite'
export const GLOBAL_DATA_DIR = '../tag-matrix'
export const GLOBAL_REGISTRY = 'registry.nuon'
export const ENTITY_TYPE_CACHE = 'entity-types.nuon'
export const ATTRIBUTE_TYPE_CACHE = 'attribute-types.nuon'

def default-commands [
  --file: path
  --config: record
]: nothing -> record {
  commands init --file $file --config $config | commands sql | commands db
}

def find-db [
  root: path
  --global
]: nothing -> path {
  if $global {
    let data_dir = $nu.data-dir | path join $GLOBAL_DATA_DIR
    mkdir $data_dir
    let registry_file = $data_dir | path join $GLOBAL_REGISTRY
    let registry = if ($registry_file | path exists) {
      open $registry_file
    } else { [] }
    let entry = $registry | where config == $root | get 0?.data
    if ($entry | is-empty) {
      mut entry = random uuid
      while ($data_dir | path join $entry | path exists) {
        $entry = random uuid
      }
      let data_dir = $data_dir | path join $entry
      mkdir $data_dir
      $registry
        | append {config: $root, data: $entry}
        | save --force $registry_file
      $data_dir | path join $DB_FILE
    } else {
      $data_dir | path join $entry $DB_FILE
    }
  } else {
    let data_dir = $root | path join $DATA_FOLDER
    mkdir $data_dir
    $data_dir | path join $DB_FILE
  }
}

export def main [
]: record -> record {
  let config = $in
  let global = $config.global? == global
  let file = find-db $env.TAG_MATRIX_ROOT --global=$global
  if not ($file | path exists) {
    let cmds = default-commands --config $config
    mut cb = [$.db.init]
    let entities = $config.schema?.entity-types? | default []
    $cb ++= $entities
      | each { [$.db.entity-type.add, $in] }
    let attributes = $config.schema?.attribute-types? | default []
    $cb ++= $attributes
      | each { [$.db.attribute-type.add, $in] }
    $cb ++= $config.schema?.default-attributes?
      | default []
      | each { [$.db.attribute.add, $in] }
    $cmds | run $cb
    stor export --file-name $file
    let data_dir = $file | path dirname
    $entities | save --force ($data_dir | path join $ENTITY_TYPE_CACHE)
    $attributes | save --force ($data_dir | path join $ATTRIBUTE_TYPE_CACHE)
    default-commands --file $file
  } else {
    let cmds = default-commands --file $file --config $config
    mut cb = []
    let data_dir = $file | path dirname
    let modified = (ls $env.TAG_MATRIX_FILE).0.modified
    let force = $config.use-force? | default false
    let entity_cache = $data_dir | path join $ENTITY_TYPE_CACHE
    if not ($entity_cache | path exists) or (ls $entity_cache).0.modified < $modified {
      let old_entities = if ($entity_cache | path exists) {
        open $entity_cache
      } else { [] }
      let new_entities = $config.schema?.entity-types? | default []
      let names = ($old_entities.name ++ $new_entities.name) | uniq
      for name in $names {
        if $name in $old_entities.name {
          if $name in $new_entities.name {
            let old_schema = $old_entities | where name == $name | get 0.schema
            let new_entity = $new_entities | where name == $name | first
            if $old_schema != $new_entity.schema {
              $cb ++= [[$.db.entity-type.patch, $new_entity]]
            }
          } else {
            $cb ++= [[$.db.entity-type.delete, {name: $name, force: $force}]]
          }
        } else {
          let new_entity = $new_entities | where name == $name | first
          $cb ++= [[$.db.entity-type.add, $new_entity]]
        }
      }
      $new_entities | save --force $entity_cache
    }
    let attribute_cache = $data_dir | path join $ATTRIBUTE_TYPE_CACHE
    if not ($attribute_cache | path exists) or (ls $attribute_cache).0.modified < $modified {
      let old_attributes = if ($attribute_cache | path exists) {
        open $attribute_cache
      } else { [] }
      let new_attributes = $config.schema?.attribute_types? | default []
      let names = ($old_attributes.name ++ $new_attributes.name) | uniq
      for name in $names {
        if $name in $old_attributes.name {
          if $name in $new_attributes.name {
            let old_attribute = $old_attributes | where name == $name | first
            let new_attribute = $new_attributes | where name == $name | first
            if $old_attribute != $new_attribute {
              $cb ++= [[$.db.attribute-type.patch, ($new_attribute | insert force $force)]]
            }
          } else {
            $cb ++= [[$.db.attribute-type.delete, {name: $name, force: $force}]]
          }
        } else {
          let new_attribute = $new_attributes | where name == $name | first
          $cb ++= [[$.db.attribute-type.add, $new_attribute]]
        }
      }
      $new_attributes | save --force $attribute_cache
    }
    if ($cb | is-not-empty) {
      $cmds | run $cb
    }
    $cmds
  }
}
