CREATE TABLE IF NOT EXISTS `table_with_id` (
  `id` integer NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `null_name` varchar(255),
  `ci_name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `truthiness` tinyint(1) NOT NULL);

CREATE TABLE IF NOT EXISTS `table_with_string_id` (
  `id` varchar(36) NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL);

CREATE TABLE IF NOT EXISTS table_without_id (
  `name` varchar(255) NOT NULL,
  `null_name` varchar(255),
  `ci_name` varchar(255) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `truthiness` tinyint(1) NOT NULL);
