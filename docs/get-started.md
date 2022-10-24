# Get Started

## Requirements

* Rust

    1. Install rust and rustup following the instruments from this [page](https://www.rust-lang.org/tools/install).

    2. Nightly version of rust is needed because we use it to format code. To install
       it:

        ```shell
        rustup default nightly
        ```

       or

        ```
        rustup toolchain install nightly
        rustup component add rustfmt --toolchain nightly
        ```

* Cmake

    * Fetch it from your packager manager

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
    * Use the pre-built binary from the [Downloads](https://cmake.org/download/) page
    * Build it from source following the instruments from [this page](https://cmake.org/install/)

* FlatBuffers
    * Build it from source

        ```shell
        $ git clone https://github.com/google/flatbuffers.git && cd flatbuffers && git checkout -b v2.0.6 v2.0.6

        # Execute one of the following commands depending on your OS
        $ cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release
        $ cmake -G "Visual Studio 10" -DCMAKE_BUILD_TYPE=Release
        $ cmake -G "Xcode" -DCMAKE_BUILD_TYPE=Release
        
        $ sudo make install
        ```

    * Fetch it from your package manager
      On some OS, you can install `FlatBuffers` using its package manager
      instead of building it from source, for example:

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

## Compile

```sh
$ git clone https://github.com/cnosdb/cnosdb.git && cd cnosdb 
$ cargo build
```

## Run

```shell
$ cargo run -- tskv --cpu 1 --memory 64 debug
```
