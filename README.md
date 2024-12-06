# Tag Matrix

Generic tagging solution based on the Entity-Attribute-Value (EAV) model.
This allowes for sparse entity -> attribute and attribute -> attribute relations.

## Testing

To create a test database file use
```sh
nu db/test.nu --save test.sqlite
```

To test commands against a sample configuration use
```nu
use tag-matrix/ main
tag-matrix --config tests/cfg.nu ...
```
