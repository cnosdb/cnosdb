
<img src="docs/source/_static/img/cnosdb_logo_white.svg" width="360"/>

<a href="https://codebeat.co/projects/github-com-cnosdatabase-cnosdb-main"><img alt="codebeat badge" src="https://codebeat.co/badges/23007af1-7b99-419c-81a8-7bfb6dac31b9" /></a>
![GitHub](https://img.shields.io/github/license/cnosdb/cnosdb)

English | [简体中文](./README_CN.md)

CnosDB is An Open Source Distributed Time Series Database with high performance, high compression ratio and high usability.
CnosDB Isipho is a original new version of CnosDB which use Rust, [Apache Arrow](https://arrow.apache.org/) and [DataFusion](https://github.com/apache/arrow-datafusion) to build.


## Design Objectives of CnosDB Isipho

To design and develop a high performance, high compression ratio, highly available, distributed cloud native time series database, which meets the following objectives.
### Storage
- Seperate storage and computation; theoretically uncapped support time series expansion; support horizontal/vertical scaling. 
- Focus performance and cost balance; high performance io, Run-to-Completion scheduling model, support for hierarchical storage using object storage.
- Lossy compression with reduced precision at the user's option.
### Query
- Implemented query engine by using Apache Arrow and Datafusion.
- Support for vectorized execution of the query engine to execute complex query statements.
- Support for standard SQL, Flux, rich aggregate queries and arithmetic.
### Ecology
- Design for multi-tenant-oriented, providing more configuration parameters, able to provide more flexible configuration of resources.
- CDC, WAL can provide subscription and distribution to other nodes, more flexible deploy and support.
- Support Ecological compatibility with the K8s ecosystem, reducing shared memory.
- Integration with other data ecosystems to support import/export of parquet files.
- Be compatible with major international and domestic public cloud ecosystems.

## Roadmap
- Click to view [Roadmap](docs/roadmap/ROADMAP.md)

## Join the community
All developers/users who love time series databases are welcome to participate in the CnosDB User Group. Scan the QR code below and add CC to join the group.

Please check [Instructions for joining the group](./docs/guidelines/CnosDBWeChatUserGroupGuidelines.md) beforehand.

<img src="docs/source/_static/img/u.jpg" width="300"/>

## Contributing

* Please refer to [Contribution Guide](./CONTRIBUTING_EN.md) to contribute to CnosDB.

## Contact

* [Home page](https://cnosdb.com)

* [Stack Overflow](https://stackoverflow.com/questions/tagged/cnosdb)

* [Twitter:@CnosDB](https://twitter.com/CnosDB)

* [LinkedIn Page](https://www.linkedin.com/company/cnosdb)

* [Bilibili](https://space.bilibili.com/36231559)

* [Tiktok CN](https://www.douyin.com/user/MS4wLjABAAAA6ua1UPmYWCcTl0AT0Lf1asILf9ogmj7J257KEq812csox9FBrAkxxKcok1GIzPMv)

* [Zhihu](https://www.zhihu.com/org/cnosdb)

* [CSDN](https://blog.csdn.net/CnosDB)

* [Jianshu](https://www.jianshu.com/u/745811688e9e)

## We are hiring
* If you want to get a job of full-time/part-time/intern, please send us resume by email hr@cnosdb.com

## License

* [AGPL-3.0 License](./LICENSE.md)