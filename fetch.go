package clickhouse

import (
	"fmt"
	"strconv"
	"time"
)

//TODO maybe need to add GetArray<Type> func(column string) (Type, error) where Type is all listen types above

// Columns returns columns list
func (result Result) Columns() []string {
	var columns []string

	for column := range result.data {
		columns = append(columns, column)
	}

	return columns
}

// Exist returns true if field is exist or false
func (result Result) Exist(column string) bool {
	message := fmt.Sprintf("Try to check if exist by `%s`", column)
	cfg.logger.debug(message)

	_, ok := result.data[column]

	return ok
}

// String returns value of string value
func (result Result) String(column string) (string, error) {
	message := fmt.Sprintf("Try to get value by `%s`", column)
	cfg.logger.debug(message)

	value, ok := result.data[column]

	if !ok {
		err := fmt.Errorf("can't get value by `%s`", column)

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return "", err
	}

	message = fmt.Sprintf("Success get `%s` = %s", column, value)
	cfg.logger.debug(message)

	return value, nil
}

// Bytes returns value of bytes
func (result Result) Bytes(column string) ([]byte, error) {
	value, err := result.String(column)
	if err != nil {
		return []byte{}, err
	}

	return []byte(value), nil
}

func (result Result) getUInt(column string, bitSize int) (uint64, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	i, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		err := fmt.Errorf("can't convert value %s to uint%d: %s", value, bitSize, err.Error())

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	return i, nil
}

// Bool returns value as bool
func (result Result) Bool(column string) (bool, error) {
	i, err := result.getUInt(column, 8)

	return i == 1, err
}

// UInt8 returns value as uint8
func (result Result) UInt8(column string) (uint8, error) {
	i, err := result.getUInt(column, 8)

	return uint8(i), err
}

// UInt16 returns value as uint16
func (result Result) UInt16(column string) (uint16, error) {
	i, err := result.getUInt(column, 16)

	return uint16(i), err
}

// UInt32 returns value as uint32
func (result Result) UInt32(column string) (uint32, error) {
	i, err := result.getUInt(column, 32)

	return uint32(i), err
}

// UInt64 returns value as uint64
func (result Result) UInt64(column string) (uint64, error) {
	i, err := result.getUInt(column, 64)

	return uint64(i), err
}

func (result Result) getInt(column string, bitSize int) (int64, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	i, err := strconv.ParseInt(value, 10, bitSize)
	if err != nil {
		err := fmt.Errorf("can't convert value %s to int%d: %s", value, bitSize, err.Error())

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	return i, nil
}

// Int8 returns value as int8
func (result Result) Int8(column string) (int8, error) {
	i, err := result.getInt(column, 8)

	return int8(i), err
}

// Int16 returns value as int16
func (result Result) Int16(column string) (int16, error) {
	i, err := result.getInt(column, 16)

	return int16(i), err
}

// Int32 returns value as int32
func (result Result) Int32(column string) (int32, error) {
	i, err := result.getInt(column, 32)

	return int32(i), err
}

// Int64 returns value as int64
func (result Result) Int64(column string) (int64, error) {
	i, err := result.getInt(column, 64)

	return int64(i), err
}

func (result Result) getFloat(column string, bitSize int) (float64, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	f, err := strconv.ParseFloat(value, bitSize)
	if err != nil {
		err := fmt.Errorf("can't convert value %s to float%d: %s", value, bitSize, err.Error())

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	return f, nil
}

// Float32 returns value as float32
func (result Result) Float32(column string) (float32, error) {
	f, err := result.getFloat(column, 32)

	return float32(f), err
}

// Float64 returns value as float64
func (result Result) Float64(column string) (float64, error) {
	f, err := result.getFloat(column, 64)

	return float64(f), err
}

// Date returns value as date
func (result Result) Date(column string) (time.Time, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		err := fmt.Errorf("can't convert value %s to date: %s", value, err.Error())

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	return t, nil
}

// DateTime returns value as datetime
func (result Result) DateTime(column string) (time.Time, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		err := fmt.Errorf("can't convert value %s to datetime: %s", value, err.Error())

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	return t, nil
}
