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
against a populated PostgreSQL database and, provided the data in the
PostgreSQL database is compatible, the migration to move the data from
PostgreSQL to MySQL.

## Install from source

```
go get github.com/pivotal-cf/pg2mysql/cmd/pg2mysql
```

## Usage

Create a config:

```
$ cat > config.yml <<EOF
mysql:
  database: some-dbname
  username: some-user
  password: some-password
  host: 192.168.10.1
  port: 3306

postgresql:
  database: some-dbname
  username: some-user
  password: some-password
  host: 192.168.10.2
  port: 5432
  ssl_mode: disable
EOF
```

_Note: See [PostgreSQL documentation](https://www.postgresql.org/docs/9.1/static/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS)_
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

Run the migrator:

```
$ pg2mysql -c config.yml migrate --truncate
inserted 1 records into spaces_developers
inserted 0 records into security_groups_spaces
inserted 0 records into service_bindings
inserted 2 records into droplets
inserted 2 records into organizations
inserted 3 records into lockings
inserted 0 records into service_dashboard_clients
inserted 0 records into route_bindings
...
```

_Note: The `--truncate` flag will truncate each table prior to copying data over._

Run the verifier after migration to confirm the data has been migrated as expected:

```
$ pg2mysql -c config.yml verify
Verifying table spaces_developers...OK
Verifying table security_groups_spaces...OK
Verifying table service_bindings...OK
Verifying table droplets...
  FAILED: 1 row missing
  Missing IDs: 1,3,5
Verifying table organizations...OK
Verifying table lockings...OK
Verifying table service_dashboard_clients...OK
Verifying table route_bindings...OK
```

Verify does an exact comparison (except for timestamps; see _Note_) of the
contents of each row of each table in PostgreSQL to see that a matching row
exists in MySQL.

_Note: The verify command assumes that the precise PostgreSQL timestamps are
truncated when doing the migration over to MySQL. However, it has been found
that this behavior is not consistent with all forms of MySQL. Official MySQL
rounds the timestamps whereas MariaDB truncates. A PR to intelligently support
both would be happily received._
