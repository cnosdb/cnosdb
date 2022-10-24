# CnosDB2.0 Road Map

## Design Objectives of CnosDB2.0

To design and develop a high performance, high compression ratio, highly available, distributed cloud native time series
database, which meets the following objectives.

> Time Series Database

1. Extensibility, theoretically support time series without upper limit, completely solve the problem of time series
   inflation, support horizontal/vertical expansion.
2. Separate storage and computation. Compute nodes and storage nodes can expand and shrink independently.
3. High-performance storage and low cost, high-performance I/O stacks, cloud disk and object storage for storage tiering
4. Query engine supports vectorized queries.
5. Supports multiple timing protocols to write and query, and provides external components to import data.

> Cloud Native

1. Supports cloud native, making full use of the convenience brought by cloud infrastructure and integrating into cloud
   native ecology.
2. High availability, second-level fault recovery, multi-cloud, and multi-zone disaster recovery and preparedness.
3. Native support multi-tenant, pay-as-you-go.
4. CDC, logs can be subscribed to and distributed to other nodes.
5. More configurable items are provided to meet the complex requirements of public cloud users in multiple scenarios.
6. Cloud edge - end collaboration provides the edge - end integration capability with the public cloud
7. Converged OLAP/CloudAI data Ecosystem on the cloud.

## CnosDB Architecture

![整体架构](../source/_static/img/arch.jpg)

## CnosDB Timeline

| 产品特性                  | 时间节点  |
|-----------------------| ----  |
| 2.0 Stand-alone  Beta | 2022.10 |
| 2.0 Distribution Beta | 2022.12 |
| Cloud Trial on AWS    | 2023.Q1 |
| Enterprise Service    | 2023.Q2 |


