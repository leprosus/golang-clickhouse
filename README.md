# Usage example

``` go
package main

import "bitbucket.org/tcjwo/clickhouse"

func main() {
    user := "clickhouse.user"
    pass := "clickhouse.pass"
    host := "clickhouse.host"
    port := 8123
    
    conn, _ := ch.New(host, port, user, pass)
    conn.MaxMemoryUsage(4 * clickhouse.GigaByte)
    
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
}
```