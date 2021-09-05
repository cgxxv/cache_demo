use log::info;
use log4rs::init_file;
use signal_hook::{consts::*, iterator::Signals};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use warp::Filter;

mod client;
mod cluster;
mod command;
mod conn;
mod error;
mod handlers;
mod models;
mod persist;
mod server;
mod services;
mod shutdown;
use crate::command::Command;
use crate::error::Error;
use crate::persist::Persist;
use crate::services::cluster_service;
use crate::services::message_service;
use crate::services::room_service;
use crate::services::user_service;

pub type Result<T> = std::result::Result<T, Error>;

pub static PERSIST_PORT: u16 = 8081;
pub static HTTP_PORT: u16 = 8080;
pub static BUFSIZE: usize = 1024;

#[tokio::main]
async fn main() -> Result<()> {
    init_file("./log4rs.yml", Default::default())?;

    let (db, cmd_rx) = models::init_db()?;

    let rooms = room_service::init(db.clone());
    let users = user_service::init(db.clone());
    let messages = message_service::init(db.clone());
    let cluster = cluster_service::init(db.clone());

    let ping = warp::path!("ping").map(|| "PONG");

    let routes = rooms.or(users).or(messages).or(cluster).or(ping);

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (_, server) =
        warp::serve(routes).bind_with_graceful_shutdown(([0, 0, 0, 0], HTTP_PORT), async move {
            info!("rust server started");
            rx.recv().await;
            sleep(Duration::from_millis(1000)).await;
            info!("rust servert receive close signal");
        });
    // Spawn the server into a runtime
    let thread = tokio::spawn(server);
    let mut signals = Signals::new(&[
        SIGTERM, SIGQUIT, SIGINT, SIGTSTP, SIGWINCH, SIGHUP, SIGCHLD, SIGCONT,
    ])?;
    let tx_signal = tx.clone();
    std::thread::spawn(move || {
        for _ in signals.forever() {
            let _ = tx_signal.send(()).unwrap();
            info!("start to close server");
            break;
        }
    });

    let (cluster_quit_tx, mut complete_rx) = Persist::init(db.clone(), cmd_rx).await?;
    info!("===============>");
    thread.await?;

    //wait until cluster_persist quit
    let _ = cluster_quit_tx.send(());

    db.cmd_tx.send(Command::quit()).await?;
    complete_rx.recv().await;
    info!("<===============");

    Ok(())
}
