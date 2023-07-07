# CnosDB2.0 Road Map

## Design Objectives of CnosDB 2.4

CnosDB is designed to develop a high-performance, high-compression ratio, high-availability, distributed cloud-native temporal database, and CnosDB 2.4 will achieve the following goals.

### Functions
Common functions (first, last, max, min), date conversion class, monitoring class (gauges calculation).
### Lossy Compression
- Deadband Compression ：A data compression algorithm used to reduce the frequency of sensor data updates and reduce data transmission and storage costs.
- Swinging Door Trending (SDT)  Algorithm ：A real-time data stream processing algorithm that can be used to process dynamic data sets, maintaining the number of elements in the data set by constantly adjusting the size of the gate.
### Support Schema change 
Support Update、Delete operations.
### Add Master-Slave replicaset 
Implements exactly once semantics for processing stream data.

## CnosDB Architecture

![Overall Structure](../source/_static/img/arch.jpg)

## CnosDB Timeline

| Product Features      | Time Points  |
|-----------------------| ----  |
| 2.0 Stand-alone  Beta | 2023.10 |



