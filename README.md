# pg2mysql

`pg2mysql` was created to facilitate migrating data from PostgreSQL to MySQL
given mostly equivalent schemas.

In PostgreSQL it is common to use the `text` datatype for character data, which
the [PostgreSQL documentation](https://www.postgresql.org/docs/9.1/static/datatype-character.html)
describes as having effectively no limit. In MySQL this is not the case, as the
datatype with the same name (`text`) is limited to 65535, and the more common
datatype, `varchar`, is defined with an explicit limit (e.g. `varchar(255)`).

This means that, given a column with `text` datatype in PostgreSQL, there must
be enough room in the equivalent MySQL column for the data in PostgreSQL to be
safely migrated over. This tool can be used to validate the target MySQL schema
against a populated PostgreSQL database. Later, it will be able to perform the
migration, as well.

### Install from source

```
go install github.com/pivotal-cf/pg2mysql
```

## Usage

Create a config:

```
$ cat > config.yml <<EOF
mysql:
  database: cloud_controller
  username: cloud_controller
  password: some-password
  host: 192.168.10.1
  port: 3306

postgresql:
  database: cloud_controller
  username: cloud_controller
  password: some-password
  host: 192.168.10.2
  port: 5432
  ssl_mode: disable
EOF
```

Note: See [PostgreSQL documentation](https://www.postgresql.org/docs/9.1/static/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS)
for valid SSL mode values.

Run the validator:

```
$ pg2mysql -c config.yml validate
found incompatible rows in apps with IDs [2]
found incompatible rows in app_usage_events with IDs [9 10 11 12]
found incompatible rows in events with IDs [16 17 18]
```

If there are any incompatible rows, as in above, they will need to be modified
before proceeding with a migration.
