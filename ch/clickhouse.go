package ch

import (
	"database/sql"
	"fmt"
	"mysql_to_md/common"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

// Clickhouse Clickhouse
type Clickhouse struct {
	DB   *gorm.DB
	Conf *common.Conf
}

const (
	// CHSqlTables 查看数据库所有数据表SQL-clickhouse
	CHSqlTables = "SELECT  `table` as table_name,'' as table_comment from system.parts where database ='%s' group by table_name"
	// CHSqlTableColumn 查看数据表列信息SQL-clickhouse
	CHSqlTableColumn = "SELECT  `position` as ORDINAL_POSITION,name as COLUMN_NAME,type as COLUMN_TYPE,is_in_partition_key as COLUMN_KEY, '' as IS_NULLABLE,comment  as COLUMN_COMMENT,default_expression as COLUMN_DEFAULT from system.columns where database = '%s' and table = '%s'"
	// SqlTableCreate 查看建表语句
	SqlTableCreate = "SHOW CREATE TABLE %s"
)

type chTableCreateSql struct {
	CreateSql string `db:"statement"`
}

// QueryTables 查询所有表
func (c *Clickhouse) QueryTables() ([]common.TableInfo, error) {
	var tableCollect []common.TableInfo
	var tableArray []string
	var commentArray []sql.NullString

	querySql := CHSqlTables
	rows, err := c.DB.Raw(fmt.Sprintf(querySql, c.Conf.Database)).Rows()
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
	if c.Conf.Tables != "" {
		tableCollect = nil
		chooseTables := strings.Split(c.Conf.Tables, ",")
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
func (c *Clickhouse) QueryTableColumn(tableName string) ([]common.TableColumn, error) {
	// 定义承载列信息的切片
	var columns []common.TableColumn

	querySql := CHSqlTableColumn

	rows, err := c.DB.Raw(fmt.Sprintf(querySql, c.Conf.Database, tableName)).Rows()
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
func (c *Clickhouse) QueryCreateSql(tableName string) (string, error) {
	var createSql chTableCreateSql
	var err error
	rows, err := c.DB.Raw(fmt.Sprintf(SqlTableCreate, tableName)).Rows()
	for rows.Next() {
		rows.Scan(&createSql.CreateSql)
	}
	if err != nil {
		fmt.Printf("execute query table create sql error, detail is [%v]\n", err.Error())
		return "", err
	}
	reg := regexp.MustCompile(`AUTO_INCREMENT=\d+ `)
	res := reg.ReplaceAllString(createSql.CreateSql, "")
	return res, nil
}
