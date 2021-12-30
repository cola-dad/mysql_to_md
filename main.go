package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

/**
Database Configuration
*/
var (
	dialselect = flag.String("s", "mysql", "dialselect(mysql)")
	host       = flag.String("h", "127.0.0.1", "host(127.0.0.1)")
	username   = flag.String("u", "root", "username(root)")
	password   = flag.String("p", "root", "password(root)")
	database   = flag.String("d", "mysql", "database(mysql)")
	port       = flag.Int("P", 3306, "port(3306)")
	charset    = flag.String("c", "utf8", "charset(utf8)")
	output     = flag.String("o", "", "output location")
	tables     = flag.String("t", "", "choose tables")
)

/**
Structured Query Language
*/
const (
	// SqlTables 查看数据库所有数据表SQL
	SqlTables = "SELECT `table_name`,`table_comment` FROM `information_schema`.`tables` WHERE `table_schema`='%s'"
	// SqlTableColumn 查看数据表列信息SQL
	SqlTableColumn = "SELECT `ORDINAL_POSITION`,`COLUMN_NAME`,`COLUMN_TYPE`,`COLUMN_KEY`,`IS_NULLABLE`,`EXTRA`,`COLUMN_COMMENT`,`COLUMN_DEFAULT` FROM `information_schema`.`columns` WHERE `table_schema`='%s' AND `table_name`='%s' ORDER BY `ORDINAL_POSITION` ASC"
	// SqlTableCreate 查看建表语句
	SqlTableCreate = "SHOW CREATE TABLE %s"
)

/**
struct for table column
*/
type tableColumn struct {
	OrdinalPosition uint16         `db:"ORDINAL_POSITION"` // position
	ColumnName      string         `db:"COLUMN_NAME"`      // name
	ColumnType      string         `db:"COLUMN_TYPE"`      // type
	ColumnKey       sql.NullString `db:"COLUMN_KEY"`       // key
	IsNullable      string         `db:"IS_NULLABLE"`      // nullable
	Extra           sql.NullString `db:"EXTRA"`            // extra
	ColumnComment   sql.NullString `db:"COLUMN_COMMENT"`   // comment
	ColumnDefault   sql.NullString `db:"COLUMN_DEFAULT"`   // default value
}

/**
struct for table message
*/
type tableInfo struct {
	Name    string         `db:"table_name"`    // name
	Comment sql.NullString `db:"table_comment"` // comment
}

type tableCreateSql struct {
	Table     string `db:"Table"`
	CreateSql string `db:"Create Table"`
}

/**
connect mysql service
*/
func connect() (db *gorm.DB, err error) {
	// generate dataSourceName

	switch *dialselect {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", *username, *password, *host, *port, *database, *charset)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	case "clickhouse":
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s", *host, *port, *database, *username, *password)
		db, err = gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
	}
	return db, err
}

/**
query table info about scheme
*/
func queryTables(db *gorm.DB, dbName string) ([]tableInfo, error) {
	var tableCollect []tableInfo
	var tableArray []string
	var commentArray []sql.NullString

	fmt.Println(fmt.Sprintf(SqlTables, dbName))
	rows, err := db.Raw(fmt.Sprintf(SqlTables, dbName)).Rows()
	if err != nil {
		return tableCollect, err
	}

	for rows.Next() {
		var info tableInfo
		err = rows.Scan(&info.Name, &info.Comment)
		if err != nil {
			fmt.Printf("execute query tables action error,had ignored, detail is [%v]\n", err.Error())
			continue
		}

		tableCollect = append(tableCollect, info)
		tableArray = append(tableArray, info.Name)
		commentArray = append(commentArray, info.Comment)
	}
	// filter tables when specified tables params
	if *tables != "" {
		tableCollect = nil
		chooseTables := strings.Split(*tables, ",")
		indexMap := make(map[int]int)
		for _, item := range chooseTables {
			subIndexMap := getTargetIndexMap(tableArray, item)
			for k, v := range subIndexMap {
				if _, ok := indexMap[k]; ok {
					continue
				}
				indexMap[k] = v
			}
		}

		if len(indexMap) != 0 {
			for _, v := range indexMap {
				var info tableInfo
				info.Name = tableArray[v]
				info.Comment = commentArray[v]
				tableCollect = append(tableCollect, info)
			}
		}
	}

	return tableCollect, err
}

/**
query table column message
*/
func queryTableColumn(db *gorm.DB, dbName string, tableName string) ([]tableColumn, error) {
	// 定义承载列信息的切片
	var columns []tableColumn

	rows, err := db.Raw(fmt.Sprintf(SqlTableColumn, dbName, tableName)).Rows()
	if err != nil {
		fmt.Printf("execute query table column action error, detail is [%v]\n", err.Error())
		return columns, err
	}
	for rows.Next() {
		var column tableColumn
		err = rows.Scan(
			&column.OrdinalPosition,
			&column.ColumnName,
			&column.ColumnType,
			&column.ColumnKey,
			&column.IsNullable,
			&column.Extra,
			&column.ColumnComment,
			&column.ColumnDefault)
		if err != nil {
			fmt.Printf("query table column scan error, detail is [%v]\n", err.Error())
			return columns, err
		}
		columns = append(columns, column)
	}

	return columns, err
}

func queryCreateSql(db *gorm.DB, tableName string) (string, error) {
	var createSql tableCreateSql
	var err error
	rows, err := db.Raw(fmt.Sprintf(SqlTableCreate, tableName)).Rows()
	for rows.Next() {
		rows.Scan(&createSql.Table, &createSql.CreateSql)
	}
	if err != nil {
		fmt.Printf("execute query table create sql error, detail is [%v]\n", err.Error())
		return "", err
	}
	reg := regexp.MustCompile(`AUTO_INCREMENT=\d+ `)
	res := reg.ReplaceAllString(createSql.CreateSql, "")
	return res, nil
}

/**
get choose table index by regexp. then you can batch choose table like `time_zone\w`
*/
func getTargetIndexMap(tableNameArr []string, item string) map[int]int {
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

/**
init func
*/
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
	flag.Parse()
}

/**
main func
*/
func main() {
	// connect mysql service
	db, connectErr := connect()
	if connectErr != nil {
		fmt.Printf("\033[31mmysql sql open failed ... \033[0m \n")
		return
	}

	// query all table name
	tables, err := queryTables(db, *database)

	if err != nil {
		fmt.Printf("\033[31mquery tables of database error ... \033[0m \n%v\n", err.Error())
		return
	}

	// create and open markdown file
	if *output == "" {
		// automatically generated if no output file path is specified
		*output = *database + "_" + time.Now().Format("20060102_150405") + ".md"
	}
	mdFile, err := os.OpenFile(*output, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("\033[31mcreate and open markdown file error \033[0m \n%v\n", err.Error())
		return
	}

	// make markdown format content
	var tableContent = "## " + *database + " tables message\n"
	for index, table := range tables {
		// make content process log
		fmt.Printf("%d/%d the %s table is making ...\n", index+1, len(tables), table.Name)

		// markdown header title
		if table.Comment.String == "" {
			// markdown header title
			tableContent += "#### " + strconv.Itoa(index+1) + "、 " + table.Name + "\n"
		} else {
			// markdown header title
			tableContent += "#### " + strconv.Itoa(index+1) + "、 " + table.Name + "-" + table.Comment.String + "\n"
		}

		// markdown table header
		tableContent += "\n" +
			"| 序号 | 字段 | 类型 | 键 | 允许空 | 默认值 | 注释 |\n" +
			"| :--: | :--: | :--: | :--: | :--: | :--: | :--: |\n"
		var columnInfo, columnInfoErr = queryTableColumn(db, *database, table.Name)
		if columnInfoErr != nil {
			fmt.Printf("\033[31mqueryTableColumn  error ... \033[0m \n%v\n", err.Error())
			return
		}
		var createSql, createSqlErr = queryCreateSql(db, table.Name)
		if createSqlErr != nil {
			fmt.Printf("\033[31mqueryCreateSql error ... \033[0m \n%v\n", err.Error())
			return
		}
		for _, info := range columnInfo {
			tableContent += fmt.Sprintf(
				"| %d | %s | %s | %s | %s | %s | %s |\n",
				info.OrdinalPosition,
				info.ColumnName,
				info.ColumnType,
				info.ColumnKey.String,
				info.IsNullable,
				info.ColumnDefault.String,
				strings.ReplaceAll(strings.ReplaceAll(info.ColumnComment.String, "|", "\\|"), "\n", ""),
			)
		}
		tableContent += "\n\n```sql\n"
		tableContent += createSql
		tableContent += "\n```\n\n"
	}
	mdFile.WriteString(tableContent)

	// close database and file handler for release
	err = mdFile.Close()
	fmt.Printf("\033[32mmysql_to_md finished ... \033[0m \n")
}
