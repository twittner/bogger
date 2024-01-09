use bogger::{Record, Handshake, BlockInfo, HandshakeResponse, Ack};
use minicbor_io::{AsyncReader, AsyncWriter};
use rand::Rng;
use tokio::net::TcpListener;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[ignore]
#[tokio::test]
async fn test_server() {
    let listener = TcpListener::bind("127.0.0.1:4000").await.unwrap();
    let mut state = (String::new(), BlockInfo::zero(), Ack::zero());

    let mut gen = rand::thread_rng();
    while let Ok((sock, _)) = listener.accept().await {
        let (r, w) = sock.into_split();
        let mut reader = AsyncReader::new(r.compat());
        let mut writer = AsyncWriter::new(w.compat_write());
        let hs: Handshake = reader.read().await.unwrap().unwrap();
        println!("received handshake from {}", hs.id());
        if state.0 != hs.id() {
            state = (hs.id().to_string(), BlockInfo::zero(), Ack::zero());
        }
        writer.write(HandshakeResponse::go(state.1)).await.unwrap();
        while let Ok(Some(r)) = reader.read::<Record>().await {
            state.1 = r.info();
            println!("{} {}", r.info(), r.item().as_ref().len());
            if gen.gen_range(0 .. 10) % 3 == 0 {
                state.2 = Ack::new(state.1)
            }
            if gen.gen_range(0 .. 100) % 10 == 0 {
                println!("sending ack: {:?}", state.2);
                let _ = writer.write(state.2).await;
            }
        }
    }
}
