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
	host    string
	port    int
	user    string
	pass    string
	fqnd    string
	timeout time.Duration
}

type Iter struct {
	columns map[string]int
	scanner *bufio.Scanner
	Result  Result
}

//TODO maybe need to add GetArray<Type> func(column string) (Type, error) where Type is all listen types above
type Result struct {
	data map[string]string
}

func Connect(host string, port int, user string, pass string) (*Conn, error) {
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

func (conn *Conn) SetTimeout(timeout time.Duration) {
	conn.timeout = timeout
}

func (conn *Conn) getFQDN() string {
	if conn.fqnd == "" {
		conn.fqnd = fmt.Sprintf("%s:%s@%s:%d", conn.user, conn.pass, conn.host, conn.port)
	}

	return conn.fqnd
}

func (conn Conn) doQuery(query string) (io.ReadCloser, error) {
	client := http.Client{
		Timeout: conn.timeout * time.Second}

	req, err := http.NewRequest("POST", "http://" + conn.getFQDN(), strings.NewReader(query))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Can't connect to host %s", conn.getFQDN()))
	}

	req.Header.Set("Content-Type", "text/plain")

	res, err := client.Do(req)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Can't do request to host %s", conn.getFQDN()))
	} else if res.StatusCode == 500 {
		bytes, _ := ioutil.ReadAll(res.Body)

		return nil, errors.New(string(bytes))
	}

	return res.Body, nil
}

func (conn *Conn) Fetch(query string) (Iter, error) {
	//TODO maybe there must be more complex regexp (using will show)
	re := regexp.MustCompile(";? *$")
	query = re.ReplaceAllString(query, " FORMAT TabSeparatedWithNames")

	reader, err := conn.doQuery(query)

	if err != nil {
		return Iter{}, err
	}

	defer reader.Close()

	iter := Iter{}
	iter.columns = make(map[string]int)

	iter.scanner = bufio.NewScanner(reader)
	if iter.scanner.Scan() {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		for index, column := range matches {
			iter.columns[column] = index
		}
	} else if err := iter.scanner.Err(); err != nil {
		return Iter{}, errors.New("Can't fetch response")
	}

	return iter, nil
}

func (iter *Iter) Next() (bool) {
	next := iter.scanner.Scan()

	if next {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		iter.Result = Result{}
		iter.Result.data = make(map[string]string)
		for column, index := range iter.columns {
			iter.Result.data[column] = matches[index]
		}
	}

	return next
}

func (result Result) GetString(column string) (string, error) {
	value, ok := result.data[column]

	if !ok {
		return "", errors.New(fmt.Sprintf("Can't fetch value by `%s`", column))
	}

	return value, nil
}

func (result Result) getUInt(column string, bitSize int) (uint64, error) {
	value, err := result.GetString(column)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to uint%d", value, bitSize))
	}

	return i, nil
}

func (result Result) GetUInt8(column string) (uint8, error) {
	i, err := result.getUInt(column, 8)

	return uint8(i), err
}

func (result Result) GetUInt16(column string) (uint16, error) {
	i, err := result.getUInt(column, 16)

	return uint16(i), err
}

func (result Result) GetUInt32(column string) (uint32, error) {
	i, err := result.getUInt(column, 32)

	return uint32(i), err
}

func (result Result) GetUInt64(column string) (uint64, error) {
	i, err := result.getUInt(column, 64)

	return uint64(i), err
}

func (result Result) getInt(column string, bitSize int) (int64, error) {
	value, err := result.GetString(column)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(value, 10, bitSize)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to int%d", value, bitSize))
	}

	return i, nil
}

func (result Result) GetInt8(column string) (int8, error) {
	i, err := result.getInt(column, 8)

	return int8(i), err
}

func (result Result) GetInt16(column string) (int16, error) {
	i, err := result.getInt(column, 16)

	return int16(i), err
}

func (result Result) GetInt32(column string) (int32, error) {
	i, err := result.getInt(column, 32)

	return int32(i), err
}

func (result Result) GetInt64(column string) (int64, error) {
	i, err := result.getInt(column, 64)

	return int64(i), err
}

func (result Result) getFloat(column string, bitSize int) (float64, error) {
	value, err := result.GetString(column)
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(value, bitSize)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Can't convert value %s to float%d", value, bitSize))
	}

	return f, nil
}

func (result Result) GetFloat32(column string) (float32, error) {
	f, err := result.getFloat(column, 32)

	return float32(f), err
}

func (result Result) GetFloat64(column string) (float64, error) {
	f, err := result.getFloat(column, 64)

	return float64(f), err
}

func (result Result) GetDate(column string) (time.Time, error) {
	value, err := result.GetString(column)
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		return time.Time{}, errors.New(fmt.Sprintf("Can't convert value %s to date", value))
	}

	return t, nil
}

func (result Result) GetDateTime(column string) (time.Time, error) {
	value, err := result.GetString(column)
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		return time.Time{}, errors.New(fmt.Sprintf("Can't convert value %s to datetime", value))
	}

	return t, nil
}