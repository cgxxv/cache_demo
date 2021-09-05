#![allow(unused_imports)]
use std::collections::HashMap;
use std::time::Duration;

// use log;
use tokio::time;
use warp::{self, http::StatusCode};

use crate::models::Db;
use crate::models::Message;

pub async fn send(
    mut message: Message,
    token: String,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    let succ = warp::reply::with_status(super::SUCCESSFUL_OPERATION, StatusCode::OK);
    let fail = warp::reply::with_status(super::INVALID_INPUT, StatusCode::BAD_REQUEST);

    // let mut backoff = 1;
    // loop {
    //     match message.create(&db, &token).await {
    //         Ok(()) => return Ok(succ),
    //         Err(_) => {
    //             if backoff > 2048 {
    //                 return Ok(fail);
    //             }
    //         }
    //     }

    //     time::sleep(Duration::from_millis(backoff)).await;
    //     backoff *= 2;
    // }

    Ok(match message.create(&db, &token).await {
        Ok(()) => succ,
        Err(_) => fail,
    })
}

pub async fn retrieve(
    params: HashMap<String, isize>,
    token: String,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    let index = params.get("pageIndex").map_or(-1, |index| index.to_owned());

    let r = warp::reply::with_status(
        warp::reply::with_header(
            super::INVALID_INPUT.to_string(),
            "content-type",
            "text/plain",
        ),
        StatusCode::BAD_REQUEST,
    );
    if index >= 0 {
        return Ok(r);
    }

    let size = params.get("pageSize").map_or(0, |size| size.to_owned());

    Ok(match Message::query(db, index, size, token).await {
        Ok(messages) => warp::reply::with_status(
            warp::reply::with_header(messages, "content-type", "application/json"),
            StatusCode::OK,
        ),
        Err(_) => r,
    })
}
