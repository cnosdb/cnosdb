# CnosDB go-client

This is the current Go client API for CnosDB.

# Usage

To import into your Go project, run the following command in your terminal: ```go get github.com/cnosdb/cnosdb/client```
. Then, in your import declaration section of your Go file, paste the
following: ```import "github.com/cnosdb/cnosdb/client"```.

# Example

The following example creates a new client to the CnosDB host on localhost:8086 and runs a query for the measurement
cpu_load from the mydb database.

```go
func ExampleClient_query() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		fmt.Println("Error creating CnosDB Client: ", err.Error())
	}
	defer func() {
		_ = c.Close()
	}()

	q := client.NewQuery("SELECT count(value) FROM cpu_load", "mydb", "")
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		fmt.Println(response.Results)
	}
}
```