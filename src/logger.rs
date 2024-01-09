use std::{path::Path, time::Duration};

use minicbor::Encode;
use tokio::{sync::{mpsc::{self, error::TryRecvError}, oneshot}, select};
use tokio::time::sleep;

use crate::{EntryWriter, Config, WriteError};

#[derive(Debug)]
pub struct Logger<T> {
    sender: mpsc::Sender<Command<T>>
}

enum Command<T> {
    Add(T),
    Sync,
    Close(oneshot::Sender<()>)
}

impl<T> Clone for Logger<T> {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone() }
    }
}

impl<T: Encode<()> + Send + 'static> Logger<T> {
    pub async fn new<P: AsRef<Path>>(dir: P, cfg: Config) -> Result<Self, LogError> {
        let mut writer = EntryWriter::open(dir, cfg).await?;
        let (tx, mut rx) = mpsc::channel(100);
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let mut closers = Vec::new();

            'main: loop {
                // Try to process all immediately available items.
                loop {
                    match rx.try_recv() {
                        Ok(it) => on_item(it, &mut writer, &mut buf, &mut closers, &mut rx).await,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break 'main
                    }
                }
                // Once the channel is empty, wait for the next item or sync the writer
                // after a short amount of time if no command shows up.
                select! {
                    it = rx.recv() =>
                        if let Some(it) = it {
                            on_item(it, &mut writer, &mut buf, &mut closers, &mut rx).await
                        } else {
                            break
                        },
                    () = sleep(Duration::from_secs(3)) =>
                        if let Err(err) = writer.sync().await {
                            tracing::error!(%err, "failed to sync log writer")
                        }
                }
                // To not repeat the syncing over and over again in case no item appears for
                // some time we now wait indefinitely for the next one before starting over.
                if let Some(it) = rx.recv().await {
                    on_item(it, &mut writer, &mut buf, &mut closers, &mut rx).await
                } else {
                    break
                }
            }

            // A final sync after the channel is closed.
            if let Err(err) = writer.sync().await {
                tracing::error!(%err, "failed to sync log writer")
            }

            // Unblock all parties that closed the logger and exit.
            for tx in closers {
                let _ = tx.send(());
            }
        });
        Ok(Self { sender: tx })
    }

    pub async fn add(&self, val: T) -> Result<(), LogError> {
        self.sender.send(Command::Add(val)).await.map_err(|_| LogError::Closed)
    }

    pub async fn sync(&self) -> Result<(), LogError> {
        self.sender.send(Command::Sync).await.map_err(|_| LogError::Closed)
    }

    pub async fn close(&self) -> Result<(), LogError> {
        let (tx, rx) = oneshot::channel();
        self.sender.send(Command::Close(tx)).await.map_err(|_| LogError::Closed)?;
        rx.await.map_err(|_| LogError::Closed)?;
        Ok(())
    }
}

async fn on_item<T>
    ( item: Command<T>
    , writer: &mut EntryWriter
    , buf: &mut Vec<u8>
    , closers: &mut Vec<oneshot::Sender<()>>
    , rx: &mut mpsc::Receiver<Command<T>>
    )
where
    T: Encode<()>
{
    match item {
        Command::Add(v) => {
            buf.clear();
            if let Err(err) = minicbor::encode(v, &mut *buf) {
                tracing::error!(%err, "failed to encode log entry");
                return
            }
            if let Err(err) = writer.append(buf).await {
                tracing::error!(%err, "failed to append log entry")
            }
        }
        Command::Sync => {
            if let Err(err) = writer.sync().await {
                tracing::error!(%err, "failed to sync log writer")
            }
        }
        Command::Close(tx) => {
            if closers.is_empty() {
                rx.close();
            }
            closers.push(tx)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("storage error: {0}")]
    Write(#[from] WriteError),

    #[error("logger closed")]
    Closed
}
