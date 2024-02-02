package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/tlarsendataguy-yxdb/yxdb-go"
	"github.com/tlarsendataguy-yxdb/yxdb-go/yxrecord"
	"os"
	"strings"
)

func main() {
	// load args and environment vars needed for the application to run
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	// establish connection to the YXDB file
	r, err := yxdb.ReadFile(config.YxdbPath)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer func() { _ = r.Close() }()

	// establish connection to SQL Server
	db, err := sql.Open(`mssql`, config.ConnStr)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}
	defer func() { _ = db.Close() }()

	// create the SQL table, if requested
	if config.DoCreate {
		err = CreateTable(r, db, config.TableName)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			return
		}
	}

	// load data into SQL Server
	err = InsertRows(r, db, config.TableName)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}

type Config struct {
	YxdbPath  string
	TableName string
	DoCreate  bool
	ConnStr   string
}

func loadConfig() (Config, error) {
	yxdbPath := flag.String(`yxdb`, ``, `Path to the YXDB file`)
	tableName := flag.String(`table`, ``, `SQL table to create and/or upload records to`)
	doCreate := flag.Bool(`createTable`, false, `Set to True to create the table in SQL Server`)
	flag.Parse()
	if *yxdbPath == `` || *tableName == `` {
		return Config{}, errors.New(`yxdb and table are required parameters`)
	}
	connStr := os.Getenv(`SQL_CONN_STR`)
	if connStr == `` {
		return Config{}, errors.New(`the SQL_CONN_STR environment variable is not set`)
	}
	cleanedPath := strings.Trim(*yxdbPath, `"`)
	cleanedTable := strings.Trim(*tableName, `"`)
	return Config{
		YxdbPath:  cleanedPath,
		TableName: cleanedTable,
		DoCreate:  *doCreate,
		ConnStr:   connStr,
	}, nil
}

func CreateTable(r yxdb.Reader, db *sql.DB, tableName string) error {
	stmt := generateCreateTable(r, tableName)
	_, err := db.ExecContext(context.Background(), stmt)
	return err
}

func generateCreateTable(r yxdb.Reader, tableName string) string {
	fields := r.ListFields()
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("CREATE TABLE %v (\r\n", tableName))
	for index, field := range fields {
		builder.WriteString(fmt.Sprintf("[%v] %v", field.Name, ayxTypeToSqlType(field)))
		if index < len(fields)-1 {
			builder.WriteRune(',')
		}
		builder.WriteString("\r\n")
	}
	builder.WriteString(`);`)
	return builder.String()
}

func ayxTypeToSqlType(field yxrecord.YxdbField) string {
	switch field.Type {
	case yxrecord.Int64, yxrecord.Byte:
		return `INT`
	case yxrecord.Float64:
		return `FLOAT(53)`
	case yxrecord.Boolean:
		return `BIT`
	case yxrecord.Date:
		return `DATETIME2`
	case yxrecord.Blob:
		return `VARBINARY(MAX)`
	default:
		return `NVARCHAR(MAX)`
	}
}

func InsertRows(r yxdb.Reader, db *sql.DB, tableName string) error {
	fields := r.ListFields()
	columns := make([]string, len(fields))
	for index, field := range fields {
		columns[index] = field.Name
	}

	bulkImportStr := mssql.CopyIn(tableName, mssql.BulkOptions{KeepNulls: true}, columns...)
	stmt, err := db.Prepare(bulkImportStr)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	values := make([]any, len(fields))
	rowCount := 0
	for r.Next() {
		extractRecordInto(r, values)
		_, err = stmt.Exec(values...)
		if err != nil {
			return err
		}
		rowCount++
		if rowCount%100000 == 0 {
			fmt.Printf("INFO: Processed %v of %v records\n", rowCount, r.NumRecords())
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	fmt.Printf("INFO: Finished processing %v records\n", rowCount)
	return nil
}

func extractRecordInto(r yxdb.Reader, values []any) {
	fields := r.ListFields()
	var value any
	var isNull bool
	for index, field := range fields {
		switch field.Type {
		case yxrecord.Int64:
			value, isNull = r.ReadInt64WithIndex(index)
		case yxrecord.Date:
			value, isNull = r.ReadTimeWithIndex(index)
		case yxrecord.Float64:
			value, isNull = r.ReadFloat64WithIndex(index)
		case yxrecord.Blob:
			isNull = false
			value = r.ReadBlobWithIndex(index)
		case yxrecord.Byte:
			value, isNull = r.ReadByteWithIndex(index)
		case yxrecord.Boolean:
			value, isNull = r.ReadBoolWithIndex(index)
		default:
			value, isNull = r.ReadStringWithIndex(index)
		}
		if isNull {
			value = nil
		}
		values[index] = value
	}
}
