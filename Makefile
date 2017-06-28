CGO_ENABLED=0

all: linux

linux:
	GOOS=linux GOARCH=amd64 go build -o pg2mysql_linux cmd/pg2mysql/main.go

clean:
	rm pg2mysql_linux
