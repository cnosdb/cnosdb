# How to use flight sql

> 100% compatible with arrow flight sql or arrow flight rpc

## Support

- java
  * jdbc
  * flight sql
- golang
  * flight sql
- c++
  * flight sql
- rust
  * flight rpc

[Apache Arrow](https://arrow.apache.org/docs/index.html)

[Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) 

[Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) 

## java

### jdbc

- properties

| camelName                        | defaultValue | required | desc     |
| -------------------------------- |--------------| -------  | -------- |
|   user                           |              | false     | user name |
|   password                       |              | false     | user password        |
|   useEncryption                  | true         | false    | whether to use TLS encryption        |
|   disableCertificateVerification | false        | false    | whether to disable certificate verification        |
|   trustStore                     | false        | false    | the KeyStore path        |
|   trustStorePassword             |              | false    | the KeyStore password        |
|   useSystemTrustStore            | true         | false    | whether to use the system operating certificates        |
|   threadPoolSize                 | 1            | false    | thread pool size of flight stream queue        |
|   token                          |              | false    | the bearer token used in the token authetication(used when no user is specified)        |

- example

```java
    final Properties properties = new Properties();
    properties.put("user", "root");
    properties.put("password", "password");
    properties.put("tenant", "cnosdb");
    properties.put("useEncryption", false);

    try (Connection connection = DriverManager.getConnection(
            "jdbc:arrow-flight-sql://localhost:8901", properties);
            Statement stmt = connection.createStatement()) {
        stmt.execute("CREATE TABLE IF NOT EXISTS air\n" +
            "(\n" +
            "    visibility  DOUBLE,\n" +
            "    temperature DOUBLE,\n" +
            "    pressure    DOUBLE,\n" +
            "    TAGS(station)\n" +
            ");");

        stmt.executeUpdate("INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES\n" +
            "    (1666165200290401000, 'XiaoMaiDao', 56, 69, 77);");

        ResultSet resultSet = stmt.executeQuery("select * from air limit 1;");

        while (resultSet.next()) {
            // Do something
        }
    }
```

### flight sql

- example

```java
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    // specify the address of the server according to the specific situation
    final Location clientLocation = Location.forGrpcInsecure("localhost", 8901);
    FlightClient client = FlightClient.builder(allocator, clientLocation).build();
    // basic authenticate， is authenticateBasicToken instead of authenticateBasic
    Optional<CredentialCallOption> credentialCallOption = client.authenticateBasicToken("root", "password");
    FlightSqlClient sqlClient = new FlightSqlClient(client);

    final CallHeaders headers = new FlightCallHeaders();
    headers.insert("tenant", "cnosdb");
    Set<CallOption> options = new HashSet<>();
    credentialCallOption.ifPresent(options::add);
    options.add(new HeaderCallOption(headers));
    CallOption[] callOptions = options.toArray(new CallOption[0]);

    // simple prepared statement schema
    try (final FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare("SELECT 1;", callOptions)) {
        final Schema actualSchema = preparedStatement.getResultSetSchema();
        final FlightInfo info = preparedStatement.execute();
        final Ticket tkt = info.getEndpoints().get(0).getTicket();

        try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
            while (stream.next()) {
                // Do something
            }
        }
    }

    // ad-hoc statement
    final FlightInfo info = sqlClient.execute("SELECT now()", callOptions);
    // Consume statement to close connection before cache eviction
    try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
        while (stream.next()) {
            // Do something
        }
    }
```

## golang

### flight sql

- example

```go
    var dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	cl, err := flightsql.NewClient("localhost:8904", nil, nil, dialOpts...)
    if err != nil {
        return err
    }
    // 1. Perform basic authentication and get the authenticated context
	ctx, err := cl.Client.AuthenticateBasicToken(context.Background(), "root", "password")
	if err != nil {
        // do something
        return err
	}
    // 2. Perform operations using an authenticated context
    // only get result metadata, not result set
    info, err := s.cl.Execute(ctx, "SELECT now();")
    if err != nil {
        // do something
        // st, ok := status.FromError(err)
        return err
	}
    // 3. Get the data of the first Endpoint, 
    // the current cnosdb implementation has only one Endpoint
    rdr, err := s.cl.DoGet(ctx, info.Endpoint[0].Ticket)
    if err != nil {
        // do something
        return err
	}
    // read result set
	defer rdr.Release()
    for rdr.Next() {
		// do something
	}
```

## cpp

[Using Arrow C++ in your own project](https://arrow.apache.org/docs/cpp/build_system.html)

### flight sql

- example

```cpp
  ARROW_ASSIGN_OR_RAISE(auto location, Location::ForGrpcTcp("localhost", 8904));
  ARROW_ASSIGN_OR_RAISE(auto client, FlightClient::Connect(location));

  FlightCallOptions call_options;
  char user[] = "root";
  char password[] = "password";

  // 1. Perform basic authentication and get the authenticated bearer token
  Result<std::pair<std::string, std::string>> bearer_result =
      client->AuthenticateBasicToken({}, user, password);
  ARROW_RETURN_NOT_OK(bearer_result);

  call_options.headers.push_back(bearer_result.ValueOrDie());

  FlightSqlClient sql_client(std::move(client));

  // 2. Perform operations using an authenticated options
  // only get result metadata, not result set
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, "select now();"));
  
  const std::vector<FlightEndpoint>& endpoints = info->endpoints();
  for (size_t i = 0; i < endpoints.size(); i++) {
    std::cout << "Results from endpoint " << i + 1 << " of " << endpoints.size()
              << std::endl;
              
    // 3. Get the data of the first Endpoint, 
    // the current cnosdb implementation has only one Endpoint
    ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet(call_options, endpoints[i].ticket));
    const arrow::Result<std::shared_ptr<Schema>>& schema = stream->GetSchema();
    ARROW_RETURN_NOT_OK(schema);

    std::cout << "Schema:" << std::endl;
    std::cout << schema->get()->ToString() << std::endl << std::endl;

    std::cout << "Results:" << std::endl;

    int64_t num_rows = 0;

    while (true) {
      ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, stream->Next());
      if (chunk.data == nullptr) {
        break;
      }
      std::cout << chunk.data->ToString() << std::endl;
      num_rows += chunk.data->num_rows();
    }

    std::cout << "Total: " << num_rows << std::endl;
  }
```

<!-- ## python

> see also

[PyArrow - Apache Arrow Python bindings](https://arrow.apache.org/docs/python/index.html)
[Arrow Flight RPC](https://arrow.apache.org/docs/python/flight.html)
[Flight protocol documentation](https://arrow.apache.org/docs/format/Flight.html)
[Flight API documentation](https://arrow.apache.org/docs/python/api/flight.html)
[Python Cookbook](https://arrow.apache.org/cookbook/py/flight.html)

### flight rpc

- requirements
  * pyarrow

- example

```py
    import pyarrow as pa
    import pyarrow.flight

    client = pa.flight.connect("grpc://localhost:8904")

    # 1. Perform basic authentication and get the authenticated context
    token_pair = client.authenticate_basic_token(b'root', b'password')
    print(token_pair)

    options = pa.flight.FlightCallOptions(headers=[token_pair])

    # 2. Perform operations using an authenticated context, only get result metadata, not result set
    descriptor = pyarrow.flight.FlightDescriptor.for_command("select now();")
    info = client.get_flight_info(descriptor, options)
    for endpoint in info.endpoints:
        print('Ticket:', endpoint.ticket)
        for location in endpoint.locations:

            reader = client.do_get(endpoint.ticket)
            df = reader.read_pandas()
            print(df)
``` -->

## rust

[arrow-flight docs](https://docs.rs/arrow-flight/26.0.0/arrow_flight/index.html)

```
arrow-flight = { version = "26.0.0", features = ["flight-sql-experimental"]}
```

### flight rpc

- example

```rust
    let endpoint = Endpoint::from_static("http://localhost:8904");
    let mut client = FlightServiceClient::connect(endpoint)
        .await
        .expect("connect");

    // 1. handshake, basic authentication
    let mut req = Request::new(futures::stream::iter(vec![HandshakeRequest::default()]));
    req.metadata_mut().insert(
        AUTHORIZATION.as_str(),
        MetadataValue::from_static("Basic eHg6eHgK"),
    );
    let resp = client.handshake(req).await.expect("handshake");
    println!("handshake resp: {:?}", resp.metadata());

    // 2. execute query, get result metadata
    let cmd = CommandStatementQuery {
        query: "select 1;".to_string(),
    };
    let any = prost_types::Any::pack(&cmd).expect("pack");
    let fd = FlightDescriptor::new_cmd(any.encode_to_vec());
    let mut req = Request::new(fd);
    req.metadata_mut().insert(
        AUTHORIZATION.as_str(),
        resp.metadata().get(AUTHORIZATION.as_str()).unwrap().clone(),
    );
    let resp = client.get_flight_info(req).await.expect("get_flight_info");

    // 3. get result set
    let flight_info = resp.into_inner();
    let schema_ref = Arc::new(Schema::try_from(IpcMessage(flight_info.schema)).expect("Schema::try_from"));

    for ep in flight_info.endpoint {
        if let Some(tkt) = ep.ticket {
            let resp = client.do_get(tkt).await.expect("do_get");
            let mut stream = resp.into_inner();
            // do something
        }
    }
```