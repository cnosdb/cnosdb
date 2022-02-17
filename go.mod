module github.com/cnosdb/cnosdb

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/cnosdb/cnosql v0.0.0
	github.com/cnosdb/common v0.0.0
	github.com/cnosdb/db v0.0.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/gogo/protobuf v1.3.2
	github.com/gorilla/mux v1.8.0
	github.com/hashicorp/go-hclog v0.9.1
	github.com/hashicorp/raft v1.3.1
	github.com/hashicorp/raft-boltdb v0.0.0-20210422161416-485fa74b0b01
	github.com/klauspost/compress v1.13.6 // indirect
	github.com/klauspost/pgzip v1.2.5
	github.com/peterh/liner v1.2.1
	github.com/pkg/errors v0.9.1
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.2.1
	github.com/tinylib/msgp v1.1.6
	go.uber.org/zap v1.19.0
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/text v0.3.7
	gopkg.in/fatih/pool.v2 v2.0.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace (
	github.com/cnosdb/cnosql => ./.vendor/cnosql
	github.com/cnosdb/common => ./.vendor/common
	github.com/cnosdb/db => ./.vendor/db
)
