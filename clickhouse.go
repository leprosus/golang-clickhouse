package clickhouse

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"log"
	"compress/gzip"
	"sync/atomic"
)

const (
	MegaByte = 1024 * 1024
	GigaByte = 1024 * MegaByte
)

type Conn struct {
	Limiter

	host           string
	port           int
	user           string
	pass           string
	maxMemoryUsage uint32
	connectTimeout uint32
	sendTimeout    uint32
	receiveTimeout uint32
	attemptsAmount uint32
	attemptWait    uint32
	compression    uint32
}

type Iter struct {
	conn       *Conn
	columns    map[string]int
	readCloser io.ReadCloser
	reader     *bufio.Reader
	err        error
	Result     Result
	isClosed   bool
}

type Result struct {
	data map[string]string
}

type config struct {
	sync.Once
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
		debug: func(message string) {
			log.Printf("DEBUG: %s\n", message)
		},
		info: func(message string) {
			log.Printf("INFO: %s\n", message)
		},
		warn: func(message string) {
			log.Printf("WARN: %s\n", message)
		},
		error: func(message string) {
			log.Printf("ERROR: %s\n", message)
		},
		fatal: func(message string) {
			log.Printf("FATAL: %s\n", message)
		}}}

func New(host string, port int, user string, pass string) *Conn {
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
		attemptWait:    0,
		compression:    0}
}

// Sets logger for debug
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
	atomic.StoreUint32(&conn.attemptsAmount, uint32(amount))
	atomic.StoreUint32(&conn.attemptWait, uint32(wait))

	message := fmt.Sprintf("Set attempts amount (%d) and wait (%d seconds)", amount, wait)
	cfg.logger.debug(message)
}

// Sets new maximum memory usage value
func (conn *Conn) MaxMemoryUsage(limit int) {
	atomic.StoreUint32(&conn.maxMemoryUsage, uint32(limit))

	message := fmt.Sprintf("Set max_memory_usage = %d", limit)
	cfg.logger.debug(message)
}

// Sets new connection timeout
func (conn *Conn) ConnectTimeout(timeout int) {
	atomic.StoreUint32(&conn.connectTimeout, uint32(timeout))

	message := fmt.Sprintf("Set connect_timeout = %d s", timeout)
	cfg.logger.debug(message)
}

// Sets new send timeout
func (conn *Conn) SendTimeout(timeout int) {
	atomic.StoreUint32(&conn.sendTimeout, uint32(timeout))

	message := fmt.Sprintf("Set send_timeout = %d s", timeout)
	cfg.logger.debug(message)
}

// Sets new send timeout
func (conn *Conn) Compression(compression bool) {
	var compInt uint32 = 0
	if compression {
		compInt = 1
	}

	atomic.StoreUint32(&conn.compression, compInt)

	message := fmt.Sprintf("Set compression = %d", conn.compression)
	cfg.logger.debug(message)
}

// Sets new receive timeout
func (conn *Conn) ReceiveTimeout(timeout int) {
	atomic.StoreUint32(&conn.receiveTimeout, uint32(timeout))

	message := fmt.Sprintf("Set receive_timeout = %d s", timeout)
	cfg.logger.debug(message)
}

// Executes new query
func (conn *Conn) Exec(query string) error {
	conn.increaseRequests()
	conn.waitRequests()
	defer conn.reduceRequests()

	return conn.ForceExec(query)
}

// Executes new query without requests limits
func (conn *Conn) ForceExec(query string) error {
	message := fmt.Sprintf("Try to execute: %s", cutOffQuery(query, 500))
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
	conn.increaseRequests()
	conn.waitRequests()
	defer conn.reduceRequests()

	return conn.ForceFetch(query)
}

// Executes new query and fetches all data without requests limits
func (conn *Conn) ForceFetch(query string) (Iter, error) {
	message := fmt.Sprintf("Try to execute: %s", cutOffQuery(query, 500))
	cfg.logger.debug(message)

	re := regexp.MustCompile("(FORMAT [A-Za-z0-9]+)? *;? *$")
	query = re.ReplaceAllString(query, " FORMAT TabSeparatedWithNames")

	iter := Iter{conn: conn}

	var err error
	iter.readCloser, err = conn.doQuery(query)

	if err != nil {
		return iter, err
	}

	iter.columns = make(map[string]int)

	cfg.logger.debug("Open stream to fetch")

	iter.reader = bufio.NewReader(iter.readCloser)
	bytes, hasMore := iter.read()
	if !hasMore {
		err := errors.New("Can't get columns names")

		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.fatal(message)

		return iter, err
	}

	line := string(bytes)

	matches := strings.Split(line, "\t")
	for index, column := range matches {
		iter.columns[column] = index
	}

	cfg.logger.debug("Load fields names")

	return iter, nil
}

// Executes new query and fetches one row
func (conn *Conn) FetchOne(query string) (Result, error) {
	conn.increaseRequests()
	conn.waitRequests()
	defer conn.reduceRequests()

	return conn.ForceFetchOne(query)
}

// Executes new query and fetches one row without requests limits
func (conn *Conn) ForceFetchOne(query string) (Result, error) {
	iter, err := conn.ForceFetch(query)
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
func (iter *Iter) Next() bool {
	cfg.logger.debug("Check if has more data")

	bytes, hasMore := iter.read()
	if !hasMore {
		return false
	}

	line := string(bytes)

	matches := strings.Split(line, "\t")

	iter.Result = Result{}
	iter.Result.data = make(map[string]string)
	for column, index := range iter.columns {
		iter.Result.data[column] = matches[index]
	}

	cfg.logger.debug("Load new data")

	return true
}

func (iter *Iter) read() ([]byte, bool) {
	var bytes []byte
	bytes, iter.err = iter.reader.ReadBytes('\n')

	l := len(bytes)
	if l > 0 {
		bytes = bytes[0:len(bytes)-1]
	}

	if iter.err == io.EOF {
		iter.Close()

		return bytes, false
	} else if iter.err != nil {
		message := fmt.Sprintf("Catch error %s", iter.err.Error())
		cfg.logger.fatal(message)

		return bytes, false
	}

	return bytes, true
}

// Returns error of iterator
func (iter Iter) Err() error {
	return iter.err
}

// Closes stream
func (iter Iter) Close() {
	if !iter.isClosed {
		iter.readCloser.Close()

		cfg.logger.debug("The query is fetched")
	}
}

func (conn *Conn) getFQDN(toConnect bool) string {
	pass := conn.pass
	masked := strings.Repeat("*", len(conn.pass))

	if !toConnect {
		pass = masked
	}

	fqnd := fmt.Sprintf("%s:%s@%s:%d", conn.user, pass, conn.host, conn.port)

	cfg.Do(func() {
		message := fmt.Sprintf("Connection FQDN is %s:%s@%s:%d", conn.user, masked, conn.host, conn.port)
		cfg.logger.info(message)
	})

	return fqnd
}

func (conn *Conn) doQuery(query string) (io.ReadCloser, error) {
	var (
		attempts uint32 = 0
		req      *http.Request
		res      *http.Response
		err      error
	)

	for attempts < atomic.LoadUint32(&conn.attemptsAmount) {
		timeout := atomic.LoadUint32(&conn.connectTimeout) +
			atomic.LoadUint32(&conn.sendTimeout) +
			atomic.LoadUint32(&conn.receiveTimeout)

		client := http.Client{Timeout: time.Duration(timeout) * time.Second}

		options := url.Values{}
		options.Set("max_memory_usage", fmt.Sprintf("%d", atomic.LoadUint32(&conn.maxMemoryUsage)))
		options.Set("connect_timeout", fmt.Sprintf("%d", atomic.LoadUint32(&conn.connectTimeout)))
		options.Set("max_memory_usage", fmt.Sprintf("%d", atomic.LoadUint32(&conn.maxMemoryUsage)))
		options.Set("send_timeout", fmt.Sprintf("%d", atomic.LoadUint32(&conn.sendTimeout)))
		options.Set("enable_http_compression", fmt.Sprintf("%d", atomic.LoadUint32(&conn.compression)))

		urlStr := "http://" + conn.getFQDN(true) + "/?" + options.Encode()

		req, err = http.NewRequest("POST", urlStr, strings.NewReader(query))
		if err != nil {
			message := fmt.Sprintf("Can't connect to host %s: %s", conn.getFQDN(false), err.Error())
			cfg.logger.fatal(message)

			return nil, errors.New(message)
		}

		if atomic.LoadUint32(&conn.compression) == 1 {
			req.Header.Add("Accept-Encoding", "gzip")
		}
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("Pragma", "no-cache")
		req.Header.Set("Cache-Control", "no-cache")

		req.Close = true

		if attempts > 0 {
			exponentialTime := attempts * conn.attemptWait

			time.Sleep(time.Duration(exponentialTime) * time.Second)
		}

		attempts++

		res, err = client.Do(req)

		if atomic.LoadUint32(&conn.attemptsAmount) > 1 {
			if err != nil {
				message := fmt.Sprintf("Catch warning %s", err.Error())
				cfg.logger.warn(message)

				if strings.Contains(err.Error(), "Memory limit") {
					return nil, errors.New(message)
				}
			} else if err = handleErrStatus(res); err != nil {
				message := fmt.Sprintf("Catch warning %s", err.Error())
				cfg.logger.warn(message)
			} else {
				return getReader(res)
			}
		}
	}

	if err != nil {
		message := fmt.Sprintf("Can't do request to host %s: %s", conn.getFQDN(false), err.Error())
		cfg.logger.error(message)

		return nil, errors.New(message)
	} else if err = handleErrStatus(res); err != nil {
		message := fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return nil, errors.New(message)
	}

	return res.Body, nil
}

func getReader(res *http.Response) (io.ReadCloser, error) {
	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err := gzip.NewReader(res.Body)
		if err != nil {
			return nil, err
		}
		reader.Close()

		return reader, nil
	default:
		return res.Body, nil
	}
}

func handleErrStatus(res *http.Response) error {
	if res.StatusCode != 200 {
		reader, err := getReader(res)
		if err != nil {
			return err
		}

		bytes, _ := ioutil.ReadAll(reader)

		text := string(bytes)

		if text[0] == '<' {
			re := regexp.MustCompile("<title>([^<]+)</title>")
			list := re.FindAllString(text, -1)

			return errors.New(list[0])
		} else {
			return errors.New(text)
		}
	}

	return nil
}

func cutOffQuery(query string, length int) string {
	if len(query) > length {
		return query[0:length] + " ..."
	}

	return query
}
