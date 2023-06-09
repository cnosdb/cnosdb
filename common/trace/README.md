# Trace Collection

## Description

The trace information collection module allow to gather and export trace data from CnosDB. 
This README provides instructions on how to configure and use the available exporters: log file or Jaeger.

## Exporters

### Log File Exporter

The log file exporter writes trace information to a specified log file. Follow the steps below to enable this exporter:

1. Open the `config.toml` file.
2. Locate the `[trace]` section.
3. Under `[trace]`, find the `[trace.log]` subsection.
4. Set the path parameter to the desired log file `path`. For example:
```toml
[trace]
[trace.log]
path = '/tmp/cnosdb'
```

### Jaeger Exporter

The Jaeger exporter sends trace information to a Jaeger instance for analysis and visualization. To use the Jaeger exporter, follow these instructions:

1. Start a `Jaeger` container by running the following command in your terminal:
```bash
docker run -d --name jaeger \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest
```
2. Open the `config.toml` file.
3. Locate the `[trace]` section.
4. Under `[trace]`, find the `[trace.jaeger]` subsection.
5. Set the `jaeger_agent_endpoint` parameter to the Jaeger agent endpoint. By default, it is set to `http://localhost:14268/api/traces`.
```toml
[trace]
[trace.jaeger]
jaeger_agent_endpoint = 'http://localhost:14268/api/traces'
max_concurrent_exports = 2
max_queue_size = 4096
```

## Conclusion
You have successfully configured and enabled trace information collection using the available exporters: log file and Jaeger. Ensure that you have the necessary permissions and access to the relevant configuration files and tools.
