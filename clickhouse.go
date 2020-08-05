package clickhouse

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	maxMemoryUsage int32
	connectTimeout int32
	sendTimeout    int32
	receiveTimeout int32
	compression    int32
	attemptsAmount uint32
	attemptWait    uint32
	protocol	   string
	mux sync.Mutex
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

type Format string

const (
	TSV          Format = "TabSeparated"
	TSVWithNames Format = "TabSeparatedWithNames"
	CSV          Format = "CSV"
	CSVWithNames Format = "CSVWithNames"
)

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

var cfg = config{
	logger: logger{
		debug: func(message string) {},
		info:  func(message string) {},
		warn:  func(message string) {},
		error: func(message string) {},
		fatal: func(message string) {}}}

func New(host string, port int, user string, pass string) *Conn {
	cfg.logger.info("Clickhouse is initialized")

	return &Conn{
		host:           host,
		port:           port,
		user:           user,
		pass:           pass,
		protocol: 		"https",
		connectTimeout: -1,
		receiveTimeout: -1,
		sendTimeout:    -1,
		maxMemoryUsage: -1,
		compression:    -1,
		attemptsAmount: 1,
		attemptWait:    0}
}

// Debug sets logger for debug
func Debug(callback func(message string)) {
	cfg.logger.debug = callback
	cfg.logger.debug("Set custom debug logger")
}

// Info sets logger for into
func Info(callback func(message string)) {
	cfg.logger.info = callback
	cfg.logger.debug("Set custom info logger")
}

// Warn sets logger for warning
func Warn(callback func(message string)) {
	cfg.logger.warn = callback
	cfg.logger.debug("Set custom warning logger")
}

// Error sets logger for error
func Error(callback func(message string)) {
	cfg.logger.error = callback
	cfg.logger.debug("Set custom error logger")
}

// Fatal sets logger for fatal
func Fatal(callback func(message string)) {
	cfg.logger.fatal = callback
	cfg.logger.debug("Set custom fatal logger")
}

// Attempts sets amount of attempt query execution
func (conn *Conn) Attempts(amount int, wait int) {
	atomic.StoreUint32(&conn.attemptsAmount, uint32(amount))
	atomic.StoreUint32(&conn.attemptWait, uint32(wait))

	message := fmt.Sprintf("Set attempts amount (%d) and wait (%d seconds)", amount, wait)
	cfg.logger.debug(message)
}

// Protocol sets new protocol value
func (conn *Conn) Protocol(protocol string) {
	conn.mux.Lock()
	conn.protocol = protocol
	conn.mux.Unlock()
	message := fmt.Sprintf("Set protocol = %s", protocol)
	cfg.logger.debug(message)
}

// MaxMemoryUsage sets new maximum memory usage value
func (conn *Conn) MaxMemoryUsage(limit int) {
	if limit < 0 {
		return
	}

	atomic.StoreInt32(&conn.maxMemoryUsage, int32(limit))

	message := fmt.Sprintf("Set max_memory_usage = %d", limit)
	cfg.logger.debug(message)
}

// ConnectTimeout sets new connection timeout
func (conn *Conn) ConnectTimeout(timeout int) {
	if timeout < 0 {
		return
	}

	atomic.StoreInt32(&conn.connectTimeout, int32(timeout))

	message := fmt.Sprintf("Set connect_timeout = %d s", timeout)
	cfg.logger.debug(message)
}

// SendTimeout sets new send timeout
func (conn *Conn) SendTimeout(timeout int) {
	if timeout < 0 {
		return
	}

	atomic.StoreInt32(&conn.sendTimeout, int32(timeout))

	message := fmt.Sprintf("Set send_timeout = %d s", timeout)
	cfg.logger.debug(message)
}

// Compression sets new send timeout
func (conn *Conn) Compression(compression bool) {
	var compInt int32 = 0
	if compression {
		compInt = 1
	}

	atomic.StoreInt32(&conn.compression, compInt)

	message := fmt.Sprintf("Set compression = %d", conn.compression)
	cfg.logger.debug(message)
}

// ReceiveTimeout sets new receive timeout
func (conn *Conn) ReceiveTimeout(timeout int) {
	atomic.StoreInt32(&conn.receiveTimeout, int32(timeout))

	message := fmt.Sprintf("Set receive_timeout = %d s", timeout)
	cfg.logger.debug(message)
}

// Exec executes new query
func (conn *Conn) Exec(query string) error {
	conn.waitForRest()
	conn.increase()
	defer conn.reduce()

	return conn.ForcedExec(query)
}

// ForcedExec executes new query without requests limits
func (conn *Conn) ForcedExec(query string) error {
	message := fmt.Sprintf("Try to execute: %s", cutOffQuery(query, 500))
	cfg.logger.debug(message)

	reader, err := conn.doQuery(query)
	if err != nil {
		message = fmt.Sprintf("Catch error %s", err.Error())
		cfg.logger.error(message)

		return err
	}

	defer reader.Close()

	_, err = ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	message = fmt.Sprintf("The query is executed %s", cutOffQuery(query, 500))
	cfg.logger.debug(message)

	return nil
}

// InsertBatch inserts TSV data into `database.table` table
func (conn *Conn) InsertBatch(database, table string, columns []string, format Format, tsvReader io.Reader) error {
	var query string
	if len(columns) == 0 {
		query = fmt.Sprintf("INSERT INTO %s.%s FORMAT %s\n", database, table, format)
	} else {
		query = fmt.Sprintf("INSERT INTO %s.%s (%s) FORMAT %s\n", database, table, strings.Join(columns, ", "), format)
	}

	reader := bufio.NewReader(tsvReader)

	var (
		bs  []byte
		err error
	)

	for {
		bs, err = reader.ReadBytes('\b')
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		query += string(bs)
	}

	query += "\n"

	err = conn.Exec(query)

	return err
}

// Fetch executes new query and fetches all data
func (conn *Conn) Fetch(query string) (Iter, error) {
	conn.waitForRest()
	conn.increase()
	defer conn.reduce()

	return conn.ForcedFetch(query)
}

// ForcedFetch executes new query and fetches all data without requests limits
func (conn *Conn) ForcedFetch(query string) (Iter, error) {
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
		err := errors.New("can't get columns names")

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

// FetchOne executes new query and fetches one row
func (conn *Conn) FetchOne(query string) (Result, error) {
	conn.waitForRest()
	conn.increase()
	defer conn.reduce()

	return conn.ForcedFetchOne(query)
}

// ForcedFetchOne executes new query and fetches one row without requests limits
func (conn *Conn) ForcedFetchOne(query string) (Result, error) {
	iter, err := conn.ForcedFetch(query)
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

// Next returns next row of data
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
		bytes = bytes[0 : len(bytes)-1]
	}

	if iter.err == io.EOF {
		iter.err = nil
		iter.Close()

		return bytes, false
	} else if iter.err != nil {
		message := fmt.Sprintf("Catch error %s", iter.err.Error())
		cfg.logger.fatal(message)

		return bytes, false
	}

	return bytes, true
}

// Err returns error of iterator
func (iter Iter) Err() error {
	return iter.err
}

// Close closes stream
func (iter Iter) Close() {
	if !iter.isClosed {
		iter.readCloser.Close()

		iter.isClosed = true

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
		maxMemoryUsage := atomic.LoadInt32(&conn.maxMemoryUsage)
		connectTimeout := atomic.LoadInt32(&conn.connectTimeout)
		sendTimeout := atomic.LoadInt32(&conn.sendTimeout)
		receiveTimeout := atomic.LoadInt32(&conn.receiveTimeout)
		compression := atomic.LoadInt32(&conn.compression)

		var timeout int32 = 0

		if connectTimeout > 0 {
			timeout += connectTimeout
		}

		if sendTimeout > 0 {
			timeout += sendTimeout
		}

		if receiveTimeout > 0 {
			timeout += receiveTimeout
		}

		client := http.Client{}
		if timeout > 0 {
			client.Timeout = time.Duration(timeout) * time.Second
		}

		options := url.Values{}
		if maxMemoryUsage > 0 {
			options.Set("max_memory_usage", fmt.Sprintf("%d", maxMemoryUsage))
		}

		if connectTimeout > 0 {
			options.Set("connect_timeout", fmt.Sprintf("%d", connectTimeout))
		}

		if sendTimeout > 0 {
			options.Set("send_timeout", fmt.Sprintf("%d", sendTimeout))
		}

		if receiveTimeout > 0 {
			options.Set("receive_timeout", fmt.Sprintf("%d", receiveTimeout))
		}

		if compression == 1 {
			options.Set("enable_http_compression", fmt.Sprintf("%d", compression))
		}

		urlStr := conn.protocol + "://" + conn.getFQDN(true) + "/?" + options.Encode()

		req, err = http.NewRequest("POST", urlStr, strings.NewReader(query))
		if err != nil {
			message := fmt.Sprintf("Can't connect to host %s: %s", conn.getFQDN(false), err.Error())
			cfg.logger.fatal(message)

			return nil, errors.New(message)
		}

		if compression == 1 {
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
		defer reader.Close()

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
