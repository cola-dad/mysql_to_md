package main

import (
	"errors"
	"fmt"
	"mysql_to_md/ch"
	"mysql_to_md/common"
	"mysql_to_md/mariadb"
	"os"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Handler md导出处理
type Handler interface {
	QueryTables() ([]common.TableInfo, error)
	QueryTableColumn(tableName string) ([]common.TableColumn, error)
	QueryCreateSql(tableName string) (string, error)
}

func newHandler() (Handler, error) {
	// generate dataSourceName
	dbConf := common.GetDBConf()
	switch dbConf.Dialselect {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", dbConf.Username, dbConf.Password, dbConf.Host, dbConf.Port, dbConf.Database, dbConf.Charset)
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		return &mariadb.Mariadb{DB: db, Conf: dbConf}, err
	case "clickhouse":
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s", dbConf.Host, dbConf.Port, dbConf.Database, dbConf.Username, dbConf.Password)
		db, err := gorm.Open(clickhouse.Open(dsn), &gorm.Config{})
		return &ch.Clickhouse{DB: db, Conf: dbConf}, err
	}
	return nil, errors.New("no connect")
}

func main() {
	// connect mysql service
	handler, connectErr := newHandler()
	if connectErr != nil {
		fmt.Printf("\033[31mmysql sql open failed ... \033[0m \n")
		return
	}
	// query all table name
	tables, err := handler.QueryTables()

	if err != nil {
		fmt.Printf("\033[31mquery tables of database error ... \033[0m \n%v\n", err.Error())
		return
	}
	dbConf := common.GetDBConf()
	// create and open markdown file
	if dbConf.Output == "" {
		// automatically generated if no output file path is specified
		dbConf.Output = dbConf.Database + "_" + time.Now().Format("20060102_150405") + ".md"
	}
	mdFile, err := os.OpenFile(dbConf.Output, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("\033[31mcreate and open markdown file error \033[0m \n%v\n", err.Error())
		return
	}

	// make markdown format content
	var tableContent = "## " + dbConf.Database + " tables message\n"
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
		var columnInfo, columnInfoErr = handler.QueryTableColumn(table.Name)
		if columnInfoErr != nil {
			fmt.Printf("\033[31mqueryTableColumn  error ... \033[0m \n%v\n", err.Error())
			return
		}
		var createSql, err = handler.QueryCreateSql(table.Name)
		if err != nil {
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
