# Start Use CnosDB

> [Docker](#Docker)
>
> [Kubernetes](#Kubernetes)
>
> [Source Installation](#Source-Installation)
>
> [Basic Operation](#Basic-Operation)

## Docker
1. Install [Docker](https://www.docker.com/products/docker-desktop/)

2. Run container 
    ```shell
    docker run -itd --env cpu=2 --env memory=4 -p 31007:31007 cnosdb/cnosdb:v2.0.0
    ```
3. Enter the container and run cnosdb-cli
    ```shell
    docker exec -it <container_id> sh
    ```

    ```
    $ cnosdb-cli
    CnosDB CLI v2.0.0
    Input arguments: Args { host: "0.0.0.0", port: 31007, user: "cnosdb", password: None, database: "public", target_partitions: Some(1), data_path: None, file: [], rc : None, format: Table, quiet: false }
    public ❯
    ```

   Tips
   >
   > Enter \q to exit
   >
   > Enter \? to get help
   >
   > For more information, see [Basic operation](#Basic operation)

## Kubernetes

### Helm

1. Prepare your Kubernetes environment
2. Execute the following command
   ```shell
   git clone https://github.com/cnosdb/cloud-deploy.git
   cd helm-chart
   helm install cnosdb .
   ```

### Terraform

1. Clone the repository
   ```shell
   git clone https://github.com/cnosdb/cloud-deploy.git
   cd terraform
   ```
2. Create
   ```shell 
   terraform init
   ```

3. Deploy
   ```shell
   terraform apply
   ```  
4. Login
 
   step 3 will give CnosDB public IP.
   ```shell
   chmod 400 /root/.ssh/id_rsa
   ssh ubuntu@<cnosdb-public-ip>
   ```

   Port 22 and 31007 are opened, you can add whitelisted IP into the main.tf file.

# Source Installation

### Support Platform

We support the following platforms, please report to us if you find it works on a platform outside the list.

- Linux x86(`x86_64-unknown-linux-gnu`)
- Darwin arm(`aarch64-apple-darwin`)

### Compiler Environment
1. Install [Rust](https://www.rust-lang.org/learn/get-started)

2. Install [Cmake](https://cmake.org/download/)

    ```shell
    # Debian or Ubuntu
    apt-get install cmake
    # Arch Linux
    pacman -S cmake
    # CentOS
    yum install cmake
    #Fedora
    dnf install cmake
    #macOS
    brew install cmake
    ```
3. Install FlatBuffers

    ```shell
    # Arch Linux
    pacman -S flatbuffers
    #Fedora
    dnf install flatbuffers
    # Ubuntu
    snap install flatbuffers
    #macOS
    brew install flatbuffers
    ```

    If your system is not listed here, you can install FlatBuffers as follows

    ```shell
    git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git && cd flatbuffers
    ```

    ```shell
    # Choose one of the following commands depending on the operating system
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
    cmake -G "Visual Studio 10" -DCMAKE_BUILD_TYPE=Release
    cmake -G "Xcode" -DCMAKE_BUILD_TYPE=Release

    sudo make install
    ```

### Compile

```shell
git clone https://github.com/cnosdb/cnosdb.git && cd cnosdb
cargo build
```

### Run

#### Run Database Service

```shell
cargo run -- run --cpu 4 --memory 64
```

#### Run CLI

In another terminal, run the following command in the same directory

```shell
cargo run --package client --bin client
```

## Basic Operation

### Create Database
```sql
CREATE DATABASE oceanic_station;
```
When executed correctly, it will return something similar to the following:

    Query took 0.080 seconds.

Enter the following command in the CLI to switch the connected database

```
\c oceanic_station
```

### Create Table
```sql
CREATE TABLE air (
    visibility DOUBLE,
    temperature DOUBLE,
    pressure DOUBLE,
    TAGS(station)
);
```

When executed correctly, it will return something similar to the following:

    Query took 0.032 seconds.

### Write Data 

```sql
INSERT INTO air (TIME, station, visibility, temperature, pressure) VALUES
(1666165200290401000, 'XiaoMaiDao', 56, 69, 77);
```
When executed correctly, the following is returned:

    +------+
    | rows |
    +------+
    | 1    |
    +------+

### Query Data 

```sql
SELECT * FROM air;
```
When executed correctly, the following is returned:

    +-----------+------------+-------------+----------------------------+------------+
    | pressure  | station    | temperature | time                       | visibility |
    +-----------+------------+-------------+----------------------------+------------+
    | 77        | XiaoMaiDao | 69          | 2022-10-19 07:40:00.290401 | 56         |
    +-----------+------------+-------------+----------------------------+------------+
