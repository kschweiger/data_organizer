# Data Organzier

Collection of general purpose and specific utilities and tools to interact with structured databases.



# Configuration

## Tables

Tables can be defined via config files:

```toml
tables=["table_1"]

[table_1]
    name="possibly_longer_name_for_table_1"
    [table_1.A]
        ctype="INT"
        is_primary=true
        is_unique=true
    [table_1.B]
        ctype="INT"
        is_primary=true
    [table_1.C]
        ctype="FLOAT"
    [table_1.D]
        ctype="FLOAT"
        nullable=false
```

Based on the list in the `tables` option. For each element a nested object as given
above is expected. The `name` will be used in the Database. For a column, define a
sub-element. Required is the `ctype` option. Optional are `is_primary`, `is_unique`,
and `nullable`

## Secrets

Create a dedicated config file for secrets. Convention by DynaConf is `.secrets.toml`.
Most basic you want something like:

```toml
[db]
dynaconf_merge=true
  password="....."
```

Adding `dynaconf_merge=true` is required in order to add the password to the db settings
in the main configuration file.