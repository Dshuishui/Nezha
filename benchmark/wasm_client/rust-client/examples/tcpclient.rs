use rust_client::client::HydisTcpClient;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::io::{Read, Write};
use wasmedge_wasi_socket::{Shutdown, TcpStream};

#[derive(Serialize)]
struct Message {
    consistency: String,
    operation: String,
    key: String,
    value: String,
    vector_clock: HashMap<String, u64>,
}

fn main() -> std::io::Result<()> {
    let kvs = format!("192.168.10.120:50000");
    // 创建客户端
    let mut client = HydisTcpClient::new();
    let mut vc_map = HashMap::new();
    vc_map.insert("192.168.10.120:30881".to_string(), 0);
    client.set_vc(vc_map);
    println!("test!");
    // 创建消息体，并序列化为JSON
    let put_writelesscausal_message = Message {
        consistency: "PutInWritelessCausal".to_string(),
        operation: "Put".to_string(),
        key: "k1".to_string(),
        value: "v1".to_string(),
        vector_clock: client.get_vc(),
    };

    let get_writelesscausal_message = Message {
        consistency: "GetInWritelessCausal".to_string(),
        operation: "Get".to_string(),
        key: "k1".to_string(),
        value: "".to_string(),
        vector_clock: client.get_vc(),
    };

    let json = serde_json::to_string(&get_writelesscausal_message).unwrap();

    let mut stream = TcpStream::connect(kvs)?;
    stream.set_nonblocking(true)?;
    stream.write(json.as_bytes())?;
    // 持续等待响应
    loop {
        let mut buf = [0; 128];
        match stream.read(&mut buf) {
            Ok(0) => {
                println!("server closed connection");
                break;
            }
            Ok(size) => {
                let buf = &mut buf[..size];
                println!("response: {}", String::from_utf8_lossy(buf));
                break;
            }
            Err(e) => {
                // WouldBlock: The operation needs to block to complete, but the blocking operation was requested to not occur.
                // 无视这个错误，继续循环
                if e.kind() == std::io::ErrorKind::WouldBlock {
                } else {
                    return Err(e);
                }
            }
        };
    }
    stream.shutdown(Shutdown::Both)?;
    Ok(())
}
