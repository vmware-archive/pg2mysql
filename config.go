package pg2mysql

type Config struct {
	MySQL struct {
		Database  string `yaml:"database"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		Charset   string `yaml:"charset"`
		Collation string `yaml:"collation"`
	} `yaml:"mysql"`

	PostgreSQL struct {
		Database string `yaml:"database"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		SSLMode  string `yaml:"ssl_mode"`
	} `yaml:"postgresql"`
}
