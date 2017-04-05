#TODO & FIX

## Add method to setting custom logger

conn.Logger(logger func(message string))

## Add method to setup attempts to do success query

* conn.Attempts(attempts int) - 1 as default
* conn.SleepAfterAttempt(time time.Duration) - 1 s as default