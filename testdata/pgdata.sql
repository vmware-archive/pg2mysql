CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS table_with_id (
  id integer NOT NULL,
  name text NOT NULL,
  null_name text,
  ci_name citext NOT NULL,
  created_at timestamp without time zone DEFAULT now() NOT NULL,
  truthiness bool NOT NULL
);

CREATE TABLE IF NOT EXISTS table_with_string_id (
  id varchar(36) NOT NULL,
  name varchar(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS table_without_id (
  name text NOT NULL,
  null_name text,
  ci_name text NOT NULL,
  created_at timestamp without time zone DEFAULT now() NOT NULL,
  truthiness bool NOT NULL
);
