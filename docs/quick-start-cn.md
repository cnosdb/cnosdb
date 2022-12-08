# 使用CnosDB

>[Docker](#Docker)
>
>[Kubernetes](#Kubernetes)
>
>[源码安装](#源码安装)
>
>[基本操作](#基本操作)

## Docker

1. 安装 [Docker](https://www.docker.com/products/docker-desktop/)

2. 使用 Docker 启动容器
   ```shell
   docker run -itd  --env cpu=2 --env memory=4 -p 31007:31007 cnosdb/cnosdb:v2.0.0
   ```

3. 进入容器运行`cnosdb-cli`
   ```shell
   docker exec -it <container_id> sh
   ```
   ```shell
   $ cnosdb-cli
   CnosDB CLI v2.0.0
   Input arguments: Args { host: "0.0.0.0", port: 31007, user: "cnosdb", password: None, database: "public", target_partitions: Some(1), data_path: None, file: [], rc: None, format: Table, quiet: false }
   public ❯
   ```
   Tips
   >
   >退出请输入`\q`
   >
   >查看帮助请输入`\?`
   >
   > 更多内容请查看[基本操作](#基本操作)

## Kubernetes

### Helm

1. 准备好Kubernetes环境
2. 执行如下命令
   ```shell
   git clone https://github.com/cnosdb/cloud-deploy.git
   cd helm-chart
   helm install cnosdb .
   ```

### Terraform

1. 克隆部署仓库
   ```shell
   git clone https://github.com/cnosdb/cloud-deploy.git
   cd terraform
   ```
2. 创建
   ```shell 
   terraform init
   ```
3. 部署
   ```shell 
   terraform apply
   ```
4. 登陆
   第三步会给出 CnosDB 的公共IP

5. ```shell
   chmod 400 /root/.ssh/id_rsa
   ssh ubuntu@<cnosdb-public-ip>
   ```
   端口22和31007以开放，你可以添加IP白名单在main.tf文件
   Port 22 and 31007 are opened, you can add whitelisted IP into the main.tf file.

## 源码安装

### **支持平台**

我们支持以下平台，如果发现可以在列表以外的平台上运行，
请[报告](https://github.com/cnosdb/cnosdb/issues)给我们。

- Linux x86(`x86_64-unknown-linux-gnu`)
- Darwin arm(`aarch64-apple-darwin`)

### **编译环境**

1. 安装`Rust`，可前往[官网](https://www.rust-lang.org/learn/get-started)下载安装
2. 安装Cmake
   ```shell
   # Debian or Ubuntu
   apt-get install cmake
   # Arch Linux
   pacman -S cmake
   # CentOS
   yum install cmake
   # Fedora
   dnf install cmake
   # macOS
   brew install cmake
   ```
   对于 Windows，你也可以在[Cmake官网](https://cmake.org/download/)下载和安装 Cmake
3. 安装FlatBuffers

   ```shell
   # Arch Linux
   pacman -S flatbuffers
   # Fedora
   dnf install flatbuffers
   # Ubuntu
   snap install flatbuffers
   # macOS
   brew install flatbuffers
   ```

   如果您的系统不在此列，需要自行编译

   ```shell
   $ git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git && cd flatbuffers

   # 根据操作系统选择以下命令之一
   $ cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
   $ cmake -G "Visual Studio 10" -DCMAKE_BUILD_TYPE=Release
   $ cmake -G "Xcode" -DCMAKE_BUILD_TYPE=Release

   $ sudo make install
   ```

### **编译**

```shell
git clone https://github.com/cnosdb/cnosdb.git && cd cnosdb
cargo build
```

### **运行**

#### **运行数据库服务**

```shell
cargo run -- run --cpu 4 --memory 64
```
#### **运行CLI**
在另一个终端，相同目录下运行如下命令
```shell
cargo run --package client --bin client
```

## 基本操作

### 创建数据库

```sql
CREATE DATABASE oceanic_station;
```

正确执行，会返回类似如下内容：

    Query took 0.080 seconds.

CLI中输入如下命令，切换连接的数据库

```
\c oceanic_station
```

### 创建表

```sql
CREATE TABLE air
(
    visibility  DOUBLE,
    temperature DOUBLE,
    pressure    DOUBLE,
    TAGS(station)
);
```

正确执行，会返回类似如下内容：

    Query took 0.032 seconds.

### 写入您的第一条数据
```sql
INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES
    (1666165200290401000, 'XiaoMaiDao', 56, 69, 77);
```

正确执行，会返回以下内容：

    +------+
    | rows |
    +------+
    | 1    |
    +------+

### 使用 SQL 查询数据

```sql
SELECT * FROM air;
```

正确执行，会返回以下内容：

    +-----------+------------+-------------+----------------------------+------------+
    | pressure | station    | temperature | time                       | visibility |
    +-----------+------------+-------------+----------------------------+------------+
    | 77        | XiaoMaiDao | 69          | 2022-10-19 07:40:00.290401 | 56         |
    +-----------+------------+-------------+----------------------------+------------+

> 更多关于数据库的操作请查看[文档](https://docs.cnosdb.com)：


