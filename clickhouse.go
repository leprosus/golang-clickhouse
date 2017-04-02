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
	"net/url"
)

const (
	MegaByte = 1024 * 1024
	GigaByte = 1024 * MegaByte
)

type Conn struct {
	host           string
	port           int
	user           string
	pass           string
	fqnd           string
	timeout        time.Duration
	maxMemoryUsage int
}

type Iter struct {
	columns map[string]int
	reader  io.ReadCloser
	scanner *bufio.Scanner
	Result  Result
}

type Result struct {
	data map[string]string
}

var debug = false

func New(host string, port int, user string, pass string) (*Conn, error) {
	return &Conn{
		host: host,
		port: port,
		user: user,
		pass: pass,
		timeout: 600,
		maxMemoryUsage: 2 * 1024 * 1024 * 1024}, nil
}

func (conn *Conn) MaxMemoryUsage(limit int) {
	conn.maxMemoryUsage = limit

	if debug {
		fmt.Printf("Set max_memory_usage = %d\n", limit)
	}
}

func (conn *Conn) Timeout(timeout time.Duration) {
	conn.timeout = timeout

	if debug {
		fmt.Printf("Set connection timeout = %d s\n", timeout)
	}
}

func (conn *Conn) Debug(state bool) {
	debug = state

	if debug {
		fmt.Printf("Set debug mode = %t\n", state)
	}
}

func (conn *Conn) Exec(query string) (error) {
	if debug {
		fmt.Printf("Try to execute \"%s\"\n", query)
	}

	reader, err := conn.doQuery(query)

	if err != nil {
		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return err
	}

	defer reader.Close()

	ioutil.ReadAll(reader)

	if debug {
		fmt.Printf("The query is executed \"%s\"\n", query)
	}

	return nil
}

func (conn *Conn) Fetch(query string) (Iter, error) {
	if debug {
		fmt.Printf("Try to fetch \"%s\"\n", query)
	}

	re := regexp.MustCompile(";? *$")
	query = re.ReplaceAllString(query, " FORMAT TabSeparatedWithNames")

	iter := Iter{}

	var err error
	iter.reader, err = conn.doQuery(query)

	if err != nil {
		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return Iter{}, err
	}

	iter.columns = make(map[string]int)

	if debug {
		fmt.Print("Open stream to fetch\n")
	}

	iter.scanner = bufio.NewScanner(iter.reader)
	if iter.scanner.Scan() {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		for index, column := range matches {
			iter.columns[column] = index
		}

		if debug {
			fmt.Print("Load fields names\n")
		}
	} else if err := iter.scanner.Err(); err != nil {
		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return Iter{}, errors.New("Can't fetch response")
	}

	return iter, nil
}

func (iter *Iter) Next() (bool) {
	if debug {
		fmt.Print("Check if has more data\n")
	}

	next := iter.scanner.Scan()

	if err := iter.scanner.Err(); err != nil {
		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return false
	}

	if next {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		iter.Result = Result{}
		iter.Result.data = make(map[string]string)
		for column, index := range iter.columns {
			iter.Result.data[column] = matches[index]
		}

		if debug {
			fmt.Print("Load new data\n")
		}
	}

	return next
}

func (iter Iter) Close() {
	iter.reader.Close()

	if debug {
		fmt.Print("The query is fetched \n")
	}
}

//TODO maybe need to add GetArray<Type> func(column string) (Type, error) where Type is all listen types above

func (result Result) String(column string) (string, error) {
	if debug {
		fmt.Printf("Try to get value by `%s`\n", column)
	}

	value, ok := result.data[column]

	if !ok {
		err := errors.New(fmt.Sprintf("Can't get value by `%s`", column))

		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return "", err
	}

	return value, nil
}

func (result Result) getUInt(column string, bitSize int) (uint64, error) {
	value, err := result.String(column)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseUint(value, 10, bitSize)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to uint%d", value, bitSize))

		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return 0, err
	}

	return i, nil
}

func (result Result) UInt8(column string) (uint8, error) {
	i, err := result.getUInt(column, 8)

	return uint8(i), err
}

func (result Result) UInt16(column string) (uint16, error) {
	i, err := result.getUInt(column, 16)

	return uint16(i), err
}

func (result Result) UInt32(column string) (uint32, error) {
	i, err := result.getUInt(column, 32)

	return uint32(i), err
}

func (result Result) UInt64(column string) (uint64, error) {
	i, err := result.getUInt(column, 64)

	return uint64(i), err
}

func (result Result) getInt(column string, bitSize int) (int64, error) {
	value, err := result.String(column)
	if err != nil {
		return 0, err
	}

	i, err := strconv.ParseInt(value, 10, bitSize)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to int%d", value, bitSize))

		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return 0, err
	}

	return i, nil
}

func (result Result) Int8(column string) (int8, error) {
	i, err := result.getInt(column, 8)

	return int8(i), err
}

func (result Result) Int16(column string) (int16, error) {
	i, err := result.getInt(column, 16)

	return int16(i), err
}

func (result Result) Int32(column string) (int32, error) {
	i, err := result.getInt(column, 32)

	return int32(i), err
}

func (result Result) Int64(column string) (int64, error) {
	i, err := result.getInt(column, 64)

	return int64(i), err
}

func (result Result) getFloat(column string, bitSize int) (float64, error) {
	value, err := result.String(column)
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(value, bitSize)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to float%d", value, bitSize))

		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return 0, err
	}

	return f, nil
}

func (result Result) Float32(column string) (float32, error) {
	f, err := result.getFloat(column, 32)

	return float32(f), err
}

func (result Result) Float64(column string) (float64, error) {
	f, err := result.getFloat(column, 64)

	return float64(f), err
}

func (result Result) Date(column string) (time.Time, error) {
	value, err := result.String(column)
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to date", value))

		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return time.Time{}, err
	}

	return t, nil
}

func (result Result) DateTime(column string) (time.Time, error) {
	value, err := result.String(column)
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to datetime", value))

		if debug {
			fmt.Printf("Catch error %s\n", err.Error())
		}

		return time.Time{}, err
	}

	return t, nil
}

func (conn *Conn) getFQDN() string {
	if conn.fqnd == "" {
		conn.fqnd = fmt.Sprintf("%s:%s@%s:%d", conn.user, conn.pass, conn.host, conn.port)

		if debug {
			fmt.Printf("Connection FQDN is %s\n", conn.fqnd)
		}
	}

	return conn.fqnd
}

func (conn Conn) doQuery(query string) (io.ReadCloser, error) {
	client := http.Client{
		Timeout: conn.timeout * time.Second}

	options := url.Values{}
	options.Set("max_memory_usage", fmt.Sprintf("%d", conn.maxMemoryUsage))

	req, err := http.NewRequest("POST", "http://" + conn.getFQDN() + "/?" + options.Encode(), strings.NewReader(query))
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