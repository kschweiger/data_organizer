# Data Organzier
[![CI](https://github.com/kschweiger/data_organizer/actions/workflows/test.yml/badge.svg)](https://github.com/kschweiger/data_organizer/actions/workflows/test.yml)

Collection of general purpose and specific utilities and tools to interact with structured databases.


# Configuration

## Tables

Tables can be defined via config files:

Example
```toml
dynaconf_merge=true
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
        default=2
```

Currently supported types (that are tested to be handeled correctly) are:
- `INT`, `FLOAT`
- `TIME`, `DATE`, 'INTERVAL'
- `VARCHAR`
- `SERIAL`


Based on the list in the `tables` option. For each element a nested object as given
above is expected. The `name` will be used in the Database. For a column, define a
sub-element. Required is the `ctype` option. Optional are `is_primary`, `is_unique`,
`nullable`, and `default`. If you want to a nullable column to default to null, add
`default="NULL"`.

**Attention**: It is highly recommended to add `dynaconf_merge=true` on top of the
additional config files. Especially if multiple tables are defined in different config
files, adding the `dynaconf_merge=true` merges all tables options in the overall config.

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


# Utilities

## edit-table
```
Usage: edit-table [OPTIONS] [CONFIG_FILES]...

  CLI app to add data to a table defined in the passed CONFIG_FILES.

Options:
  --conf_base TEXT         Base directory containing all config files. All
                           other condif file paths will be interpreted
                           relative to this  [default: conf/]
  --default_settings TEXT  Main settings file defining e.g. DB options and
                           table configuration. Pass the file relative to
                           --conf_base.  [default: settings.toml]
  --secrets TEXT           File containing the secrets (e.g. Database
                           password).  [default: .secrets.toml]
  --help                   Show this message and exit.
```

Inputting `""`, `"NULL"`, or `"NONE"` will insert NULL into the table.

## extract-byte

```
Usage: extract-byte [OPTIONS] [CONFIG_FILES]...

  Utility that saves the values in a byte column to a file. The filename is
  based on the columns that are returned by the query to the database. Pass
  additional one or more CONFIG_FILES as first arguments.

Options:
  --conf_base TEXT         Base directory containing all config files. All
                           other condif file paths will be interpreted
                           relative to this  [default: conf/]
  --default_settings TEXT  Main settings file defining e.g. DB options and
                           table configuration. Pass the file relative to
                           --conf_base.  [default: settings.toml]
  --secrets TEXT           File containing the secrets (e.g. Database
                           password).  [default: .secrets.toml]
  --path PATH              Full output path. Must exist!  [required]
  --prefix TEXT            Prefix of the output file  [required]
  --ext TEXT               Extension of the output file  [required]
  --data_column TEXT       Name of the column that will be saved to file.
                           Expecting byte column type  [required]
  --columns TEXT           Comma separated list of columns of the main table
                           (defined with --table) that will be used for the
                           output file name.  [required]
  --table TEXT             Main table that contains the data that should be
                           written to file.  [required]
  --join_tables TEXT       Optional table that will be joined to the main
                           table yia the column defined in --join_on. Can be
                           passed multiple time to join multiple tables.
                           Attention: All join are relative to the main table
  --join_on TEXT           Column that will be used in the join of the main
                           table to the join tables. It is assumed that the
                           column is present in the main and the join table.
                           Can be passed multiple time to join multiple
                           tables.
  --join_columns TEXT      Comma separated list of columns selected from the
                           joined table. Can be passed multiple time to join
                           multiple tables.
  --debug                  Enable debug logging.
  --help                   Show this message and exit.
```