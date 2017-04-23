# Golang Clickhouse connector

## Create new connection

```go
user := "clickhouse.user"
pass := "clickhouse.pass"
host := "clickhouse.host"
port := 8123

conn, _ := clickhouse.New(host, port, user, pass)

// Also you can preset maximum memory usage limit to execute one query
conn.MaxMemoryUsage(4 * clickhouse.GigaByte)
```

## Query rows

```go
conn, _ := ch.New(host, port, user, pass)

iter, err := conn.Fetch("SELECT `database`, `name`, `engine` FROM system.tables")
if err != nil {
    panic(err)
}

for iter.Next() {
    result := iter.Result

    database, _ := result.String("database")
    name, _ := result.String("name")
    engine, _ := result.String("engine")

    println(database, country, engine)
}
```

## Execute insert

```go
conn, _ := ch.New(host, port, user, pass)

query := fmt.Sprintf("INSERT INTO db.table (SomeFiled) VALUES ('%s')", "Some value")
conn.Exec(query)
```

## List all methods

### Connection

* clickhouse.New(host, port, user, pass) - creates connection
* clickhouse.Debug(func(message string)) - sets custom logger for debug
* clickhouse.Info(func(message string)) - sets custom logger for info
* clickhouse.Warn(func(message string)) - sets custom logger for warn
* clickhouse.Error(func(message string)) - sets custom logger for error
* clickhouse.Fatal(func(message string)) - sets custom logger for fatal
* conn.Attempts(attempts, wait) - sets amount of attempts and time awaiting after fail request (wait in seconds)
* conn.MaxMemoryUsage(limit) - sets maximum memory usage per query (limit in bytes)
* conn.ConnectTimeout(timeout) - sets connection timeout (timeout in seconds)
* conn.SendTimeout(timeout) - sets send timeout (timeout in seconds)
* conn.ReceiveTimeout(timeout) - sets receive timeout (timeout in seconds)
* conn.Fetch(query) - executes, fetches query and returns iterator and error
* conn.FetchOne(query) - executes, fetches query and returns first result and error
* conn.Exec(query) - executes query and returns error

### Iterator

* iter.Next() - checks if has more data
* iter.Err() - returns error if exist or nil
* iter.Result() - returns result
* iter.Close() - closes data stream

### Result

* result.Exist("FieldName") - returns true if field is exist or false
* result.String("FieldName") - returns string value and error
* result.Bytes("FieldName") - returns bytes slice value and error
* result.UInt8("FieldName") - return unsigned int8 value and error
* result.UInt16("FieldName") - return unsigned int16 value and error
* result.UInt32("FieldName") - return unsigned int32 value and error
* result.UInt64("FieldName") - return unsigned int64 value and error
* result.Int8("FieldName") - return int8 value and error
* result.Int16("FieldName") - return int16 value and error
* result.Int32("FieldName") - return int32 value and error
* result.Int64("FieldName") - return int64 value and error
* result.Float32("FieldName") - return float32 value and error
* result.Float64("FieldName") - return float64 value and error
* result.Date("FieldName") - parse data YYYY-MM-DD and returns time value and error
* result.DateTime("FieldName") - parse data YYYY-MM-DD HH:MM:SS and returns time value and error