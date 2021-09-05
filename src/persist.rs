use std::fs::OpenOptions;
use std::io::{prelude::*, BufWriter};
use std::sync::Arc;

// use bytes::{BufMut, BytesMut};
use log::{debug, info, warn};
use tokio::{
    // io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::JoinHandle,
};

use crate::command::Command;
use crate::models::{Db, CHAT_DB_FILE};
use crate::server::{self, Server};
use crate::{Result, BUFSIZE};

// pub static CAP: usize = 10;

pub struct Persist;

impl Persist {
    pub async fn init(
        db: Db,
        cmd_rx: mpsc::Receiver<Command>,
    ) -> Result<(mpsc::UnboundedSender<()>, mpsc::UnboundedReceiver<()>)> {
        // let buffer = vec![BytesMut::with_capacity(CAP); BUF];
        // let buffer = Arc::new(Mutex::new(buffer));

        let (buf_tx, buf_rx) = mpsc::channel(BUFSIZE);
        let (cluster_quit_tx, cluster_quit_rx) = mpsc::unbounded_channel();
        let (sync_tx, sync_rx) = mpsc::channel(BUFSIZE);

        Server::new(Arc::clone(&db), buf_tx.clone())
            .await?
            .persist(cluster_quit_rx)
            .await?;
        server::notify(Arc::clone(&db), sync_rx).await?;

        Self::write_buf(cmd_rx, sync_tx, buf_tx).await;
        let (complete_tx, complete_rx) = mpsc::unbounded_channel();
        Self::write_file(buf_rx, complete_tx).await?;

        Ok((cluster_quit_tx, complete_rx))
    }

    async fn write_file(
        mut buf_rx: mpsc::Receiver<Command>,
        complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<JoinHandle<()>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(CHAT_DB_FILE)?;
        let t = tokio::spawn(async move {
            let mut file = BufWriter::new(file);
            let tick = crossbeam_channel::tick(std::time::Duration::from_secs(1));

            loop {
                crossbeam_channel::select! {
                    default => {
                        if let Some(cmd) = buf_rx.recv().await {
                            debug!("---> {:?}", cmd);
                            if cmd.is_quit() {
                                // debug!("got = {:?}", cmd);
                                debug!("receive write_file `quit` command");
                                break;
                            }

                            debug!("---> write to file");
                            let _ = file.write(cmd.encode().as_bytes());
                        }
                    },
                    recv(tick) -> _ => {
                        let _ = file.flush();
                        // debug!("<--- flushed");
                    },
                }
            }

            let _ = file.flush();
            let _ = complete_tx.send(());
            info!("exit write_file safely");
        });

        Ok(t)
    }

    //仅master会执行这个
    async fn write_buf(
        mut cmd_rx: mpsc::Receiver<Command>,
        sync_tx: mpsc::Sender<Command>,
        buf_tx: mpsc::Sender<Command>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                debug!("===> write_buf {:?}", cmd);
                if let Err(e) = sync_tx.send(cmd.clone()).await {
                    warn!("sync error occured: {:?}", e);
                }
                if cmd.is_quit() {
                    debug!("receive write_buf `quit` command");
                    let _ = buf_tx.send(Command::quit()).await;
                    break;
                }

                let _ = buf_tx.send(cmd).await;
            }

            info!("exit write_buf safely");
        })
    }
}
