install Go:
```bash
wget https://studygolang.com/dl/golang/go1.15.4.linux-amd64.tar.gz
sudo tar -zxvf go1.15.4.linux-amd64.tar.gz -C ~/
mkdir ~/Go
echo "export GOPATH=~/Go" >>  ~/.bashrc 
echo "export GOROOT=~/go" >> ~/.bashrc 
echo "export GOTOOLS=\$GOROOT/pkg/tool" >> ~/.bashrc
echo "export PATH=\$PATH:\$GOROOT/bin:\$GOPATH/bin" >> ~/.bashrc
source ~/.bashrc    # 重新加载.bashrc
go env -w GOPROXY=https://goproxy.cn
```
git push fail:
```bash
# 设置Git的全局代理，通过http.proxy配置项，将HTTP协议的请求流量转发到代理服务器（127.0.0.1:1080）。表示将所有HTTP请求都转发到监听在本地1080端口上的代理服务器。
git config --global http.proxy http://127.0.0.1:1080
# 取消Git的全局代理，将之前设置的http.proxy配置项移除，从而取消Git的代理设置。
git config --global --unset http.proxy
```

start kvserver cluster: 
`go run kvstore/kvserver/kvserver_pro.go -address 192.168.1.72:3088 -internalAddress 192.168.1.72:30881 -peers 192.168.1.72:30881,192.168.1.100:30881,192.168.1.74:30881`
`go run kvstore/kvserver/kvserver_pro.go -address 192.168.1.74:3088 -internalAddress 192.168.1.74:30881 -peers 192.168.1.72:30881,192.168.1.100:30881,192.168.1.74:30881`
`go run kvstore/kvserver/kvserver_pro.go -address 192.168.1.100:3088 -internalAddress 192.168.1.100:30881 -peers 192.168.1.72:30881,192.168.1.100:30881,192.168.1.74:30881`
<!-- 在一个集群中启动一个键值存储服务器。通过设置监听地址、内部地址和对等节点，实现了服务器之间的通信和数据共享。 -->
<!-- kvserver with tcp and rpc:
`go run kvstore/kvserver/kvserver.go -address 192.168.10.129:3088 -tcpAddress 192.168.10.129:50000 -internalAddress 192.168.10.129:30881 -peers 192.168.1.74:30881` -->
<!-- 启动一个基于 TCP 协议的分布式键值存储服务器，用于在分布式集群中存储键值对，并且能够与其他节点进行数据同步。通过设置监听地址、TCP 监听地址、内部地址和对等节点，实现了服务器之间的通信、数据共享、以及基于 TCP 的数据同步。 -->

start kvclient:
* RequestRatio benchmark: 
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 15625000 -vsize 64 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088`
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 3906250 -vsize 256 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088` 
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 976563 -vsize 1000 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088`
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 244140 -vsize 4000 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088`
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 61035 -vsize 16000 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088`
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 15258 -vsize 64000 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088`
    `go run ./benchmark/hydis/benchmark_pro.go -cnums 1500 -dnums 3815 -vsize 256000 -servers 192.168.1.72:3088,192.168.1.74:3088,192.168.1.100:3088`
    <!-- 模拟的客户端个数为1，客户端程序将会在一个goroutine中运行 -->
    * cnums: number of clients, goroutines simulate 
    <!-- 操作次数，便于测试 -->
    * onums: number of operations, each client will do onums operations
    <!-- 设定基准测试模式，设置为RequestRatio表示在测试中会测试请求比例，即PUT和GET的比例。 -->
    * mode: only support RequestRatio (put/get ratio is changeable)
    <!-- 设置GET操作执行次数与PUT的比率为4：1，即每执行4次GET操作后，会执行一次PUT操作。 -->
    * getRatio: get times per put time
    <!-- 设置服务器的地址为三个IP地址的3088端口，用，分隔。即客户端将会连接到这三个地址的服务器上。 -->
    * servers: kvserver address
    operation times = cnums * onums * (1+getRatio)

* benchmark from csv:
    <!-- 模拟5个客户端，客户端程序将在5个goroutine中运行，模拟多个并发客户端与服务器进行交互。
    基准测试模式为BenchmarkFromCSV，表示使用CSV文件进行基准测试。
    设置服务器的地址为五个主机名加上：3088端口号。
    这个命令的作用是通过读取 CSV 文件中的操作记录，模拟多个客户端与服务器进行交互，以进行基准测试。通过指定客户端数目、服务器地址和 CSV 文件路径，实现对分布式键值存储系统的性能测试和评估。 -->
    `go run ./benchmark/hydis/benchmark.go -cnums 5 -mode BenchmarkFromCSV -servers benchmark001:3088,benchmark002:3088,benchmark003:3088,benchmark004:3088,benchmark005:3088`

* WASM Client by Rust:
    <!-- 需要一个WASM运行时环境，这里提供了一个叫做“wasmedge”的WASM运行时环境作为参考。因为WASM是一种跨平台的字节码格式，在不同的平台上都需要相应的运行时环境来执行。 -->
    Need WASM Runtime(wasmedge...) 
    `cd ./benchmark/wasm_client/rust-client`
    compile: `cargo build --example=tcpclient --target=wasm32-wasi`
    run: `wasmedge ./target/wasm32-wasi/debug/examples/tcpclient.wasm`