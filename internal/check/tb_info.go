package check

import (
	"context"
	"database/sql"
	"errors"

	"fmt"
	"gorm.io/gorm"

	"gorm.io/gen/internal/model"
)

const (
	//query table structure (mysql)
	columnQuery = "SELECT COLUMN_NAME,COLUMN_COMMENT,DATA_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_TYPE,COLUMN_DEFAULT,EXTRA " +
		"FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = ? AND TABLE_NAME =? " +
		"ORDER BY ORDINAL_POSITION"

	//query table index (mysql)
	indexQuery = "SELECT TABLE_NAME,COLUMN_NAME,INDEX_NAME,SEQ_IN_INDEX,NON_UNIQUE " +
		"FROM information_schema.STATISTICS " +
		"WHERE TABLE_SCHEMA = ? AND TABLE_NAME =?"

	//query table structure (postgresql)
	columnQueryPg = "SELECT COLUMN_NAME AS \"COLUMN_NAME\",'' AS \"COLUMN_COMMENT\",DATA_TYPE AS \"DATA_TYPE\",IS_NULLABLE AS \"IS_NULLABLE\"," +
		"'' AS \"COLUMN_KEY\",'varchar' AS \"COLUMN_TYPE\", COLUMN_DEFAULT AS \"COLUMN_DEFAULT\",'' AS \"EXTRA\" " +
		"FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = ? AND TABLE_NAME =? " +
		"ORDER BY ORDINAL_POSITION"
)

type ITableInfo interface {
	GetTbColumns(schemaName string, tableName string) (result []*model.Column, err error)

	GetTbIndex(schemaName string, tableName string) (result []*model.Index, err error)
}

func getITableInfo(db *gorm.DB) ITableInfo {
	switch db.Name() {
	case "mysql":
		return &mysqlTableInfo{db: db}
	case "postgres":
		return &postgresTableInfo{db: db}
	default:
		panic("unsupported database")
	}
}

func getTbColumns(db *gorm.DB, schemaName string, tableName string, indexTag bool) (result []*model.Column, err error) {
	if db == nil {
		return nil, errors.New("gorm db is nil")
	}

	mt := getITableInfo(db)
	result, err = mt.GetTbColumns(schemaName, tableName)
	if err != nil {
		return nil, err
	}
	if !indexTag || len(result) == 0 {
		return result, nil
	}

	index, err := mt.GetTbIndex(schemaName, tableName)
	if err != nil { //ignore find index err
		db.Logger.Warn(context.Background(), "GetTbIndex for %s,err=%s", tableName, err.Error())
		return result, nil
	}
	if len(index) == 0 {
		return result, nil
	}
	im := model.GroupByColumn(index)
	for _, c := range result {
		c.Indexes = im[c.ColumnName]
	}
	return result, nil
}

type mysqlTableInfo struct {
	db *gorm.DB
}

type postgresTableInfo struct {
	db *gorm.DB
}

//GetTbColumns Mysql struct
func (t *mysqlTableInfo) GetTbColumns(schemaName string, tableName string) (result []*model.Column, err error) {
	return result, t.db.Raw(columnQuery, schemaName, tableName).Scan(&result).Error
}

//GetTbIndex Mysql index
func (t *mysqlTableInfo) GetTbIndex(schemaName string, tableName string) (result []*model.Index, err error) {
	return result, t.db.Raw(indexQuery, schemaName, tableName).Scan(&result).Error
}

func (t *postgresTableInfo) GetTbColumns(schemaName string, tableName string) (result []*model.Column, err error) {
	if err = t.db.Raw(columnQueryPg, schemaName, tableName).Scan(&result).Error; err != nil {
		return nil, err
	}
	if rows, err := t.db.Raw(fmt.Sprintf("select * from %s LIMIT 0", tableName)).Rows(); err != nil {
		return nil, err
	} else {
		if types, err := rows.ColumnTypes(); err != nil {
			return nil, err
		} else {
			m := make(map[string]*sql.ColumnType)
			for i := range types {
				m[types[i].Name()] = types[i]
			}
			for _, r := range result {
				t := m[r.ColumnName]
				if t.Name() == r.ColumnName {
					r.ColumnType = t.DatabaseTypeName()
				}
			}
		}
	}
	return result, nil
}

func (t *postgresTableInfo) GetTbIndex(schemaName string, tableName string) (result []*model.Index, err error) {
	// todo
	panic("Not support postgresql")
}
