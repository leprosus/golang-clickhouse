#TODO & FIX

## Fix i/o timeout err

```
Can't fetch data. Catch error Can't do request to host
panic: runtime error: invalid memory address or nil pointer dereference

goroutine 1 [running]:
panic(0x669840, 0xc42000c0f0)
        /usr/lib/golang/src/runtime/panic.go:500 +0x1a1
bufio.(*Scanner).Scan(0x0, 0xc4203df930)
        /usr/lib/golang/src/bufio/scan.go:129 +0x3a
bitbucket.org/tcjwo/clickhouse.(*Iter).Next(0xc4203dfd98, 0x5a)
        /home/leprosus/go/src/bitbucket.org/tcjwo/clickhouse/clickhouse.go:158 +0x5e
```

Need to realise:
* conn.ConncetTimeout (connect_timeout = 10)
* conn.ReceiveTimeout (receive_timeout = 300)
* conn.SendTimeout (send_timeout = 300)

## Add method to setting custom logger

conn.Logger(logger func(message string))

## Add method to setup attempts to do success query

* conn.Attempts(attempts int) - 1 as default
* conn.SleepAfterAttempt(time time.Duration) - 1 s as default