package dbutils

import (
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

func First(db *sqlx.DB, v interface{}, query string, args ...interface{}) error {
	err := db.QueryRow(query, args...).Scan(v)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	default:
		return nil
	}
}
func TxFirst(db *sqlx.Tx, v interface{}, query string, args ...interface{}) error {
	err := db.QueryRow(query, args...).Scan(v)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		return err
	default:
		return nil
	}
}
func ToString(db *sqlx.DB, query, sep string, args ...interface{}) (string, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var r []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
		r = append(r, name)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(r, sep), nil
}
func FirstString(db *sqlx.DB, query string, args ...interface{}) (v string, err error) {
	err = First(db, &v, query, args...)
	return
}
func FirstInt(db *sqlx.DB, query string, args ...interface{}) (v int, err error) {
	err = First(db, &v, query, args...)
	return
}
func FirstInt64(db *sqlx.DB, query string, args ...interface{}) (v int64, err error) {
	err = First(db, &v, query, args...)
	return
}
func TxFirstInt(db *sqlx.Tx, query string, args ...interface{}) (v int, err error) {
	err = TxFirst(db, &v, query, args...)
	return
}
func TxFirstInt64(db *sqlx.Tx, query string, args ...interface{}) (v int64, err error) {
	err = TxFirst(db, &v, query, args...)
	return
}
func JSON(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	var tableData []map[string]interface{}
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}

	return tableData, nil
}
