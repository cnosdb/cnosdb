if [[ $TARGETPLATFORM = "linux/amd64" ]]; then
    curl -o protoc.zip -sL https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip
    unzip protoc.zip -d protoc && mv protoc/bin/protoc /usr/local/bin &&
        mv protoc/include/* /usr/local/include/ && rm -rf protoc protoc.zip
    curl -o flatbuffers.zip -sL https://github.com/google/flatbuffers/releases/download/v22.9.29/Linux.flatc.binary.clang++-12.zip
    unzip flatbuffers.zip && mv flatc /usr/local/bin && rm -rf flatbuffers.zip
elif [[ $TARGETPLATFORM = "linux/arm64" ]]; then
    curl -o protoc.zip -sL https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-linux-aarch_64.zip
    unzip protoc.zip -d protoc && mv protoc/bin/protoc /usr/local/bin &&
        mv protoc/include/* /usr/local/include/ && rm -rf protoc protoc.zip
    git clone -b v22.9.29 --depth 1 https://github.com/google/flatbuffers.git && cd flatbuffers &&
        cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release &&
        make install && cd .. && rm -rf flatbuffers
elif [[ $TARGETPLATFORM = "windows/amd64" ]]; then
    curl -o protoc.zip -sL https://github.com/protocolbuffers/protobuf/releases/download/v3.15.8/protoc-3.15.8-win64.zip
    unzip protoc.zip -d protoc && mv protoc/bin/protoc.exe /c/Windows/system32 && rm -rf protoc protoc.zip
    curl -o flatbuffers.zip -sL https://github.com/google/flatbuffers/releases/download/v22.9.29/Windows.flatc.binary.zip
    unzip flatbuffers.zip && mv flatc.exe /c/Windows/system32 && rm -rf flatbuffers.zip
elif [[ $TARGETPLATFORM = "darwin" ]]; then
    brew install protobuf flatbuffers
else
    echo "unspportted platform: $TARGETPLATFORM"
    exit 1
fi
