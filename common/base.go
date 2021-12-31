package common

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
)

// TableColumn 表结构
type TableColumn struct {
	OrdinalPosition uint16         `db:"ORDINAL_POSITION"` // position
	ColumnName      string         `db:"COLUMN_NAME"`      // name
	ColumnType      string         `db:"COLUMN_TYPE"`      // type
	ColumnKey       sql.NullString `db:"COLUMN_KEY"`       // key
	IsNullable      string         `db:"IS_NULLABLE"`      // nullable
	ColumnComment   sql.NullString `db:"COLUMN_COMMENT"`   // comment
	ColumnDefault   sql.NullString `db:"COLUMN_DEFAULT"`   // default value
}

// TableInfo 表信息
type TableInfo struct {
	Name    string         `db:"table_name"`    // name
	Comment sql.NullString `db:"table_comment"` // comment
}

var dbConf Conf

// Conf 数据库配置
type Conf struct {
	Dialselect string
	Host       string
	Username   string
	Password   string
	Database   string
	Port       int
	Charset    string
	Output     string
	Tables     string
}

func init() {
	// init flag for command
	flag.CommandLine.Usage = func() {
		fmt.Println("Usage: mysql_to_md [options...]\n" +
			"--help  This help text" + "\n" +
			"-s      dial select.     default mysql" + "\n" +
			"-h      host.     default 127.0.0.1" + "\n" +
			"-u      username. default root" + "\n" +
			"-p      password. default root" + "\n" +
			"-d      database. default mysql" + "\n" +
			"-P      port.     default 3306" + "\n" +
			"-c      charset.  default utf8" + "\n" +
			"-o      output.   default current location\n" +
			"-t      tables.   default all table and support ',' separator for filter, every item can use regexp" +
			"")
		os.Exit(0)
	}
	dialselect := flag.String("s", "mysql", "dialselect(mysql)")
	host := flag.String("h", "127.0.0.1", "host(127.0.0.1)")
	username := flag.String("u", "root", "username(root)")
	password := flag.String("p", "root", "password(root)")
	database := flag.String("d", "mysql", "database(mysql)")
	port := flag.Int("P", 3306, "port(3306)")
	charset := flag.String("c", "utf8", "charset(utf8)")
	output := flag.String("o", "", "output location")
	tables := flag.String("t", "", "choose tables")
	flag.Parse()
	dbConf = Conf{
		Dialselect: *dialselect,
		Host:       *host,
		Username:   *username,
		Password:   *password,
		Database:   *database,
		Port:       *port,
		Charset:    *charset,
		Output:     *output,
		Tables:     *tables,
	}
	fmt.Println(dbConf)
}

// GetTargetIndexMap
// get choose table index by regexp. then you can batch choose table like `time_zone\w`
func GetTargetIndexMap(tableNameArr []string, item string) map[int]int {
	indexMap := make(map[int]int)
	for i := 0; i < len(tableNameArr); i++ {
		if match, _ := regexp.MatchString(item, tableNameArr[i]); match {
			if _, ok := indexMap[i]; ok {
				continue
			}
			indexMap[i] = i
		}
	}
	return indexMap
}

// GetDBConf 获取db配置
func GetDBConf() *Conf {
	return &dbConf
}
