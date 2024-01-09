use std::path::Path;

use bogger::{Logger, Config};
use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;
use tokio::fs;

#[tokio::test]
async fn log_some_records() {
    let dir = Path::new("/tmp/logs-test-log-some-records");
    if !dir.is_dir() {
        fs::create_dir(dir).await.unwrap();
    }
    let cfg = Config::default();
    let log = Logger::new(dir, cfg).await.unwrap();

    for _ in 0 .. 10000 {
        let mut g = rand::thread_rng();
        let len = g.gen_range(60 .. 150);
        let msg = Alphanumeric.sample_string(&mut g, len);
        log.add(msg).await.unwrap();
        //tokio::time::sleep(std::time::Duration::from_millis(100)).await
    }

    log.close().await.unwrap()
}
