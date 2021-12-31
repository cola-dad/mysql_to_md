package mariadb

import (
	"database/sql"
	"fmt"
	"mysql_to_md/common"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

// Mariadb Mariadb
type Mariadb struct {
	DB   *gorm.DB
	Conf *common.Conf
}

const (
	// SqlTables 查看数据库所有数据表SQL
	SqlTables = "SELECT `table_name`,`table_comment` FROM `information_schema`.`tables` WHERE `table_schema`='%s'"
	// SqlTableColumn 查看数据表列信息SQL
	SqlTableColumn = "SELECT `ORDINAL_POSITION`,`COLUMN_NAME`,`COLUMN_TYPE`,`COLUMN_KEY`,`IS_NULLABLE`,`COLUMN_COMMENT`,`COLUMN_DEFAULT` FROM `information_schema`.`columns` WHERE `table_schema`='%s' AND `table_name`='%s' ORDER BY `ORDINAL_POSITION` ASC"
	// SqlTableCreate 查看建表语句
	SqlTableCreate = "SHOW CREATE TABLE %s"
)

type tableCreateSql struct {
	Table     string `db:"Table"`
	CreateSql string `db:"Create Table"`
}

// QueryTables 查询所有表
func (m *Mariadb) QueryTables() ([]common.TableInfo, error) {
	var tableCollect []common.TableInfo
	var tableArray []string
	var commentArray []sql.NullString

	fmt.Println(fmt.Sprintf(SqlTables, m.Conf.Database))
	rows, err := m.DB.Raw(fmt.Sprintf(SqlTables, m.Conf.Database)).Rows()
	if err != nil {
		return tableCollect, err
	}

	for rows.Next() {
		var info common.TableInfo
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
	if m.Conf.Tables != "" {
		tableCollect = nil
		chooseTables := strings.Split(m.Conf.Tables, ",")
		indexMap := make(map[int]int)
		for _, item := range chooseTables {
			subIndexMap := common.GetTargetIndexMap(tableArray, item)
			for k, v := range subIndexMap {
				if _, ok := indexMap[k]; ok {
					continue
				}
				indexMap[k] = v
			}
		}

		if len(indexMap) != 0 {
			for _, v := range indexMap {
				var info common.TableInfo
				info.Name = tableArray[v]
				info.Comment = commentArray[v]
				tableCollect = append(tableCollect, info)
			}
		}
	}

	return tableCollect, err
}

// QueryTableColumn 查询表字段
func (m *Mariadb) QueryTableColumn(tableName string) ([]common.TableColumn, error) {
	// 定义承载列信息的切片
	var columns []common.TableColumn

	querySql := SqlTableColumn

	rows, err := m.DB.Raw(fmt.Sprintf(querySql, m.Conf.Database, tableName)).Rows()
	if err != nil {
		fmt.Printf("execute query table column action error, detail is [%v]\n", err.Error())
		return columns, err
	}
	for rows.Next() {
		var column common.TableColumn
		err = rows.Scan(
			&column.OrdinalPosition,
			&column.ColumnName,
			&column.ColumnType,
			&column.ColumnKey,
			&column.IsNullable,
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

// QueryCreateSql 查询建表语句
func (m *Mariadb) QueryCreateSql(tableName string) (string, error) {
	var createSql tableCreateSql
	var err error
	rows, err := m.DB.Raw(fmt.Sprintf(SqlTableCreate, tableName)).Rows()
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
