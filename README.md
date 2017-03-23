# Usage example

``` go
package main

import "bitbucket.org/tcjwo/clickhouse"

func main() {
    user := "clickhouse.user"
    pass := "clickhouse.pass"
    host := "clickhouse.host"
    port := 8123
    
    conn, _ := ch.Connect(host, port, user, pass)
    iter, err := conn.Fetch("SELECT `database`, `name`, `engine` FROM system.tables")
    if err != nil {
        panic(err)
    }
    
    for iter.Next() {
        result := iter.Result
    
        database, _ := result.GetString("database")
        name, _ := result.GetString("name")
        engine, _ := result.GetString("engine")
    
        println(database, country, engine)
    }
}
```