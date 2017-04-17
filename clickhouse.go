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
	maxMemoryUsage int
	connectTimeout int
	sendTimeout    int
	receiveTimeout int
	attemptsAmount int
	attemptWait    int
}

type Iter struct {
	columns  map[string]int
	reader   io.ReadCloser
	scanner  *bufio.Scanner
	err      error
	Result   Result
	isClosed bool
}

type Result struct {
	data map[string]string
}

type config struct {
	logger logger
}

type logger struct {
	debug func(message string)
	info  func(message string)
	warn  func(message string)
	error func(message string)
	fatal func(message string)
}

var cfg config = config{
	logger: logger{
		debug: func(message string) {},
		info:  func(message string) {},
		warn:  func(message string) {},
		error: func(message string) {},
		fatal: func(message string) {}}}

func New(host string, port int, user string, pass string) (*Conn, error) {
	cfg.logger.info("Clickhouse is initialized")

	return &Conn{
		host:           host,
		port:           port,
		user:           user,
		pass:           pass,
		connectTimeout: 10,
		receiveTimeout: 300,
		sendTimeout:    300,
		maxMemoryUsage: 2 * 1024 * 1024 * 1024,
		attemptsAmount: 1,
		attemptWait:    0}, nil
}

// Sets logger for debig
func Debug(callback func(message string)) {
	cfg.logger.debug = callback
	cfg.logger.debug("Set custom debug logger")
}

// Sets logger for into
func Info(callback func(message string)) {
	cfg.logger.info = callback
	cfg.logger.debug("Set custom info logger")
}

// Sets logger for warning
func Warn(callback func(message string)) {
	cfg.logger.warn = callback
	cfg.logger.debug("Set custom warning logger")
}

// Sets logger for error
func Error(callback func(message string)) {
	cfg.logger.error = callback
	cfg.logger.debug("Set custom error logger")
}

// Sets logger for fatal
func Fatal(callback func(message string)) {
	cfg.logger.fatal = callback
	cfg.logger.debug("Set custom fatal logger")
}

func (conn *Conn) Attempts(amount int, wait int) {
	conn.attemptsAmount = amount
	conn.attemptWait = wait
}

// Sets new maximum memory usage value
func (conn *Conn) MaxMemoryUsage(limit int) {
	conn.maxMemoryUsage = limit

	message := fmt.Sprintf("Set max_memory_usage = %d", limit)
	cfg.logger.info(message)
}

// Sets new connection timeout
func (conn *Conn) ConnectTimeout(timeout int) {
	conn.connectTimeout = timeout

	message := fmt.Sprintf("Set connect_timeout = %d s", timeout)
	cfg.logger.info(message)
}

// Sets new send timeout
func (conn *Conn) SendTimeout(timeout int) {
	conn.sendTimeout = timeout

	message := fmt.Sprintf("Set send_timeout = %d s", timeout)
	cfg.logger.info(message)
}

// Sets new recieve timeout
func (conn *Conn) ReceiveTimeout(timeout int) {
	conn.receiveTimeout = timeout

	message := fmt.Sprintf("Set receive_timeout = %d s", timeout)
	cfg.logger.info(message)
}

// Executes new query
func (conn *Conn) Exec(query string) (error) {
	var message string
	if len(query) > 1000 {
		message = fmt.Sprintf("Try to execute: %s ...", query[0:1000])
	} else {
		message = fmt.Sprintf("Try to execute: %s", query)
	}
	cfg.logger.debug(message)

	reader, err := conn.doQuery(query)
	if err != nil {
		message = fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return err
	}

	defer reader.Close()

	ioutil.ReadAll(reader)

	message = fmt.Sprintf("The query is executed %s", query)
	cfg.logger.debug(message)

	return nil
}

// Executes new query and fetches all data
func (conn *Conn) Fetch(query string) (Iter, error) {
	var message string
	if len(query) > 1000 {
		message = fmt.Sprintf("Try to execute: %s ...", query[0:1000])
	} else {
		message = fmt.Sprintf("Try to execute: %s", query)
	}
	cfg.logger.debug(message)

	re := regexp.MustCompile("(FORMAT [A-Za-z0-9]+)? *;? *$")
	query = re.ReplaceAllString(query, " FORMAT TabSeparatedWithNames")

	iter := Iter{}

	var err error
	iter.reader, err = conn.doQuery(query)

	if err != nil {
		return Iter{}, err
	}

	iter.columns = make(map[string]int)

	cfg.logger.debug("Open stream to fetch")

	iter.scanner = bufio.NewScanner(iter.reader)
	if iter.scanner.Scan() {
		line := iter.scanner.Text()

		matches := strings.Split(line, "\t")
		for index, column := range matches {
			iter.columns[column] = index
		}

		cfg.logger.debug("Load fields names")
	} else if err := iter.scanner.Err(); err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.fatal(message)

		return Iter{}, errors.New(fmt.Sprintf("Can't fetch response: %s", err.Error()))
	}

	return iter, nil
}

// Executes new query and fetches one row
func (conn *Conn) FetchOne(query string) (Result, error) {
	iter, err := conn.Fetch(query)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return Result{}, err
	}

	defer iter.Close()

	if iter.Next() {
		return iter.Result, nil
	}

	return Result{}, nil
}

// Returns next row of data
func (iter *Iter) Next() (bool) {
	cfg.logger.debug("Check if has more data")

	next := iter.scanner.Scan()

	if iter.err = iter.scanner.Err(); iter.err != nil {
		message := fmt.Sprintf("Catch error %s", iter.err.Error())
		cfg.logger.fatal(message)

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

		cfg.logger.debug("Load new data")
	} else {
		iter.Close()
	}

	return next
}

// Returns error of iterator
func (iter Iter) Err() (error) {
	return iter.err
}

// Closes stream
func (iter Iter) Close() {
	if !iter.isClosed {
		iter.reader.Close()

		cfg.logger.debug("The query is fetched")
	}
}

//TODO maybe need to add GetArray<Type> func(column string) (Type, error) where Type is all listen types above

// Returns value of string value
func (result Result) String(column string) (string, error) {
	message := fmt.Sprintf("Try to get value by `%s`", column)
	cfg.logger.debug(message)

	value, ok := result.data[column]

	if !ok {
		err := errors.New(fmt.Sprintf("Can't get value by `%s`", column))

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return "", err
	}

	message = fmt.Sprintf("Success get `%s` = %s", column, value)
	cfg.logger.debug(message)

	return value, nil
}

// Returns value of bytes
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
		err := errors.New(fmt.Sprintf("Can't convert value %s to uint%d: %s", value, bitSize, err.Error()))

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	return i, nil
}

// Returns value as uint8
func (result Result) UInt8(column string) (uint8, error) {
	i, err := result.getUInt(column, 8)

	return uint8(i), err
}

// Returns value as uint16
func (result Result) UInt16(column string) (uint16, error) {
	i, err := result.getUInt(column, 16)

	return uint16(i), err
}

// Returns value as uint32
func (result Result) UInt32(column string) (uint32, error) {
	i, err := result.getUInt(column, 32)

	return uint32(i), err
}

// Returns value as uint64
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
		err := errors.New(fmt.Sprintf("Can't convert value %s to int%d: %s", value, bitSize, err.Error()))

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	return i, nil
}

// Returns value as int8
func (result Result) Int8(column string) (int8, error) {
	i, err := result.getInt(column, 8)

	return int8(i), err
}

// Returns value as int16
func (result Result) Int16(column string) (int16, error) {
	i, err := result.getInt(column, 16)

	return int16(i), err
}

// Returns value as int32
func (result Result) Int32(column string) (int32, error) {
	i, err := result.getInt(column, 32)

	return int32(i), err
}

// Returns value as int64
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
		err := errors.New(fmt.Sprintf("Can't convert value %s to float%d: %s", value, bitSize, err.Error()))

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return 0, err
	}

	return f, nil
}

// Returns value as float32
func (result Result) Float32(column string) (float32, error) {
	f, err := result.getFloat(column, 32)

	return float32(f), err
}

// Returns value as float64
func (result Result) Float64(column string) (float64, error) {
	f, err := result.getFloat(column, 64)

	return float64(f), err
}

// Returns value as date
func (result Result) Date(column string) (time.Time, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02", value)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to date: %s", value, err.Error()))

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	return t, nil
}

// Returns value as datetime
func (result Result) DateTime(column string) (time.Time, error) {
	value, err := result.String(column)
	if err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	t, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		err := errors.New(fmt.Sprintf("Can't convert value %s to datetime: %s", value, err.Error()))

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return time.Time{}, err
	}

	return t, nil
}

func (conn *Conn) getFQDN(toConnect bool) string {
	if conn.fqnd == "" {
		pass := conn.pass
		masked := strings.Repeat("*", len(conn.pass))

		if !toConnect {
			pass = masked
		}

		conn.fqnd = fmt.Sprintf("%s:%s@%s:%d", conn.user, pass, conn.host, conn.port)

		message := fmt.Sprintf("Connection FQDN is %s:%s@%s:%d", conn.user, masked, conn.host, conn.port)
		cfg.logger.info(message)
	}

	return conn.fqnd
}

func (conn *Conn) doQuery(query string) (io.ReadCloser, error) {
	timeout := conn.connectTimeout +
		conn.sendTimeout +
		conn.receiveTimeout

	client := http.Client{
		Timeout: time.Duration(timeout) * time.Second}

	options := url.Values{}
	options.Set("max_memory_usage", fmt.Sprintf("%d", conn.maxMemoryUsage))
	options.Set("connect_timeout", fmt.Sprintf("%d", conn.connectTimeout))
	options.Set("max_memory_usage", fmt.Sprintf("%d", conn.maxMemoryUsage))
	options.Set("send_timeout", fmt.Sprintf("%d", conn.sendTimeout))

	urlStr := "http://" + conn.getFQDN(true) + "/?" + options.Encode()

	req, err := http.NewRequest("POST", urlStr, strings.NewReader(query))
	if err != nil {
		message := fmt.Sprintf("Can't connect to host %s: %s", conn.getFQDN(false), err.Error())
		cfg.logger.fatal(message)

		return nil, errors.New(message)
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Cache-Control", "no-cache")

	isDone := false
	attempts := 0
	var res *http.Response

	for !isDone && attempts < conn.attemptsAmount {
		if attempts > 0 {
			time.Sleep(time.Duration(conn.attemptWait) * time.Second)
		}

		if conn.attemptsAmount > 1 {
			message := fmt.Sprintf("Try to attempt #%s", conn.attemptsAmount)
			cfg.logger.debug(message)
		}

		attempts++
		res, err = client.Do(req)

		if err == nil {
			if res.StatusCode != 200 {
				bytes, _ := ioutil.ReadAll(res.Body)

				text := string(bytes)

				if text[0] == '<' {
					re := regexp.MustCompile("<title>([^<]+)</title>")
					list := re.FindAllString(text, -1)

					err = errors.New(list[0])
				} else {
					err = errors.New(text)
				}

				if conn.attemptsAmount > 1 {
					message := fmt.Sprintf("Catch warning %s", err.Error())
					cfg.logger.warn(message)
				} else {
					message := fmt.Sprintf("Catch error %s", err.Error())
					cfg.logger.error(message)
				}
			} else {
				if conn.attemptsAmount > 1 {
					cfg.logger.debug("Attempt is success")
				}

				isDone = true
			}
		}
	}

	if err != nil {
		message := fmt.Sprintf("Can't do request to host %s: %s", conn.getFQDN(false), err.Error())
		cfg.logger.fatal(message)

		return nil, errors.New(message)
	} else if res.StatusCode != 200 {
		bytes, _ := ioutil.ReadAll(res.Body)

		message := string(bytes)
		cfg.logger.error(message)

		return nil, errors.New(message)
	}

	return res.Body, nil
}
