#![allow(unused_imports)]
use std::collections::HashMap;
use std::time::Duration;

// use log;
use tokio::time;
use warp::{self, http::StatusCode};

use crate::models::Db;
use crate::models::Room;

pub async fn create_room(
    params: HashMap<String, String>,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    let name = params
        .get("name")
        .map_or("".to_string(), |name| name.to_string());

    let r = warp::reply::with_status(super::INVALID_INPUT.to_string(), StatusCode::BAD_REQUEST);
    if name.is_empty() {
        return Ok(r);
    }

    // let mut backoff = 1;
    // loop {
    //     match Room::create(&db, &name).await {
    //         Ok(id) => return Ok(warp::reply::with_status(id, StatusCode::OK)),
    //         Err(_) => {
    //             if backoff > 2048 {
    //                 return Ok(r);
    //             }
    //         }
    //     }

    //     time::sleep(Duration::from_millis(backoff)).await;
    //     backoff *= 2;
    // }

    Ok(match Room::create(&db, &name).await {
        Ok(id) => warp::reply::with_status(id, StatusCode::OK),
        Err(_) => r,
    })
}

pub async fn enter_room(
    roomid: String,
    token: String,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    //log::debug!("roomid = {}, token = {}", roomid, token);
    let succ = warp::reply::with_status(super::ENTER_THE_ROOM, StatusCode::OK);
    let fail = warp::reply::with_status(super::INVALID_ROOMID, StatusCode::BAD_REQUEST);

    // let mut backoff = 1;
    // loop {
    //     match Room::enter(&db, &roomid, &token).await {
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

    Ok(match Room::enter(&db, &roomid, &token).await {
        Ok(()) => succ,
        Err(_) => fail,
    })
}

pub async fn leave_room(token: String, db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let succ = warp::reply::with_status(super::LEFT_THE_ROOM, StatusCode::OK);
    let fail = warp::reply::with_status(super::ERROR, StatusCode::BAD_REQUEST);

    // let mut backoff = 1;
    // loop {
    //     match Room::leave(&db, &token).await {
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

    Ok(match Room::leave(&db, &token).await {
        Ok(()) => succ,
        Err(_) => fail,
    })
}

pub async fn get_room_info(roomid: String, db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(match Room::get_roominfo(db, roomid).await {
        Ok(name) => warp::reply::with_status(name, StatusCode::OK),
        Err(_) => {
            warp::reply::with_status(super::INVALID_ROOMID.to_owned(), StatusCode::BAD_REQUEST)
        }
    })
}

pub async fn get_room_users(roomid: String, db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(match Room::get_users(db, roomid).await {
        Ok(users) => warp::reply::with_status(
            warp::reply::with_header(users, "content-type", "application/json"),
            StatusCode::OK,
        ),
        Err(_) => warp::reply::with_status(
            warp::reply::with_header(
                super::INVALID_ROOMID.to_string(),
                "content-type",
                "text/plain",
            ),
            StatusCode::BAD_REQUEST,
        ),
    })
}

pub async fn get_room_list(
    params: HashMap<String, isize>,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    let index = params.get("pageIndex").map_or(-1, |index| index.to_owned());

    let r = warp::reply::with_status(
        warp::reply::with_header(super::ERROR.to_string(), "content-type", "text/plain"),
        StatusCode::BAD_REQUEST,
    );
    if index < 0 {
        return Ok(r);
    }

    let size = params.get("pageSize").map_or(0, |size| size.to_owned());

    Ok(match Room::query(db, index, size).await {
        Ok(rooms) => warp::reply::with_status(
            warp::reply::with_header(rooms, "content-type", "application/json"),
            StatusCode::OK,
        ),
        Err(_) => r,
    })
}
