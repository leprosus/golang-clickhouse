package clickhouse

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
	"bufio"
	"errors"
	"regexp"
	"strconv"
)

type Conn struct {
	host       string
	port       int
	user       string
	pass       string
	fqnd       string
	timeout    int
	SetTimeout func(timeout int)
	Exec       func(query string) (error)
	Fetch      func(query string) (*Iter, error)
}

type Iter struct {
	columns     map[string]int
	scanner     *bufio.Scanner
	data        map[string]string
	Next        func() (bool)
	GetUInt8    func(column string) (uint, error)
	GetUInt16   func(column string) (uint16, error)
	GetUInt32   func(column string) (uint32, error)
	GetUInt64   func(column string) (uint64, error)
	GetInt8Get  func(column string) (int8, error)
	GetInt16    func(column string) (int16, error)
	GetInt32    func(column string) (int32, error)
	GetInt64    func(column string) (int64, error)
	GetFloat32  func(column string) (float32, error)
	GetFloat64  func(column string) (float64, error)
	GetString   func(column string) (string, error)
	GetDate     func(column string) (time.Time, error)
	GetDateTime func(column string) (time.Time, error)
	//TODO maybe need to add GetArray<Type> func(column string) (Type, error) where Type is all listen types above
	Close       func() (error)
}

func Conn(host string, port int, user string, pass string) (*Conn, error) {
	return &Conn{
		host: host,
		port: port,
		user: user,
		pass: pass,
		timeout: 600}, nil
}

func (conn *Conn) Exec(query string) (error) {
	reader, err := conn.doQuery(query)

	if err != nil {
		return err
	}

	defer reader.Close()

	ioutil.ReadAll(reader)

	return nil
}

func (conn *Conn) SetTimeout(timeout int) {
	conn.timeout = timeout
}

func (conn *Conn) getFQDN() string {
	if conn.fqnd == nil {
		conn.fqnd = fmt.Sprintf("%s:%s@%s:%d", conn.user, conn.pass, conn.host, conn.port)
	}

	return conn.fqnd
}

func (conn Conn) doQuery(query string) (io.ReadCloser, error) {
	//TODO maybe there must be more complex regexp (using will show)
	re := regexp.MustCompile(";? *$")
	query = re.ReplaceAllString(query, "FORMAT TabSeparatedWithNames")

	client := http.Client{
		Timeout: conn.timeout * time.Second,
	}

	req, err := http.NewRequest("POST", "http://" + conn.getFQDN(), strings.NewReader(query))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Can't connect to host %s", conn.getFQDN()))
	}

	req.Header.Set("Content-Type", "text/plain")

	res, err := client.Do(req)

	if res.StatusCode == 500 {
		bytes, _ := ioutil.ReadAll(res.Body)

		return nil, errors.New(string(bytes))
	}

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Can't do request to host %s", conn.getFQDN()))
	}

	return res.Body, nil
}

func (conn *Conn) Fetch(query string) (*Iter, error) {
	reader, err := conn.doQuery(query)

	if err != nil {
		return nil, err
	}

	defer reader.Close()

	iter := Iter{}

	iter.scanner = bufio.NewScanner(reader)
	if iter.scanner.Scan() {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		for index, column := range matches {
			iter.columns[column] = index
		}
	} else if err := iter.scanner.Err(); err != nil {
		return nil, errors.New("Can't fetch response")
	}

	return iter, nil
}

func (iter *Iter) Next() (bool) {
	next := iter.scanner.Scan()

	if next {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		for column, index := range iter.columns {
			iter.data[column] = matches[index]
		}
	}

	return next
}

func (iter Iter) GetString(column string) (string, error) {
	value, err := iter.data[column]

	if err != nil {
		return "", errors.New(fmt.Sprintf("Can't fetch value by `%s`", column))
	}

	return value, nil
}

func (iter *Iter) getUInt(column string, bitSize int) (interface{}, error) {
	value, err := iter.GetString(column)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to uint%d", value, bitSize))
	}

	if bitSize == 8 {
		return uint8(i), nil
	} else if bitSize == 16 {
		return uint16(i), nil
	} else if bitSize == 32 {
		return uint32(i), nil
	}

	return i, nil
}

func (iter *Iter) GetUInt8(column string) (uint8, error) {
	return iter.getUInt(column, 8)
}

func (iter *Iter) GetUInt16(column string) (uint16, error) {
	return iter.getUInt(column, 16)
}

func (iter *Iter) GetUInt32(column string) (uint32, error) {
	return iter.getUInt(column, 32)
}

func (iter *Iter) GetUInt64(column string) (uint64, error) {
	return iter.getUInt(column, 64)
}

func (iter *Iter) getInt(column string, bitSize int) (interface{}, error) {
	value, err := iter.GetString(column)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(value, 10, bitSize)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to int%d", value, bitSize))
	}

	if bitSize == 8 {
		return int8(i), nil
	} else if bitSize == 16 {
		return int16(i), nil
	} else if bitSize == 32 {
		return int32(i), nil
	}

	return i, nil
}

func (iter *Iter) GetInt8(column string) (uint8, error) {
	return iter.getInt(column, 8)
}

func (iter *Iter) GetInt16(column string) (uint16, error) {
	return iter.getInt(column, 16)
}

func (iter *Iter) GetInt32(column string) (uint32, error) {
	return iter.getInt(column, 32)
}

func (iter *Iter) GetInt64(column string) (uint64, error) {
	return iter.getInt(column, 64)
}

func (iter *Iter) getFloat(column string, bitSize int) (interface{}, error) {
	value, err := iter.GetString(column)
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(value, bitSize)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to float%d", value, bitSize))
	}

	if bitSize == 32 {
		return float32(f), nil
	}

	return f, nil
}

func (iter *Iter) GetFloat32(column string) (float32, error) {
	return iter.getFloat(column, 32)
}

func (iter *Iter) GetFloat64(column string) (float64, error) {
	return iter.getFloat(column, 64)
}

func (iter *Iter) GetDate(column string) (time.Time, error) {
	value, err := iter.GetString(column)
	if err != nil {
		return 0, err
	}

	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to date", value))
	}

	return t.Unix(), nil
}

func (iter *Iter) GetDateTime(column string) (time.Time, error) {
	value, err := iter.GetString(column)
	if err != nil {
		return 0, err
	}

	t, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to datetime", value))
	}

	return t.Unix(), nil
}