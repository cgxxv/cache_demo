#![allow(unused_imports)]
use std::collections::HashMap;
use std::time::Duration;

// use log;
use tokio::time;
use warp::http::StatusCode;

use crate::models::Db;
use crate::models::User;

pub async fn register(mut user: User, db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let succ = warp::reply::with_status(super::SUCCESSFUL_OPERATION, StatusCode::OK);
    let fail = warp::reply::with_status(super::INVALID_INPUT, StatusCode::BAD_REQUEST);

    // let mut backoff = 1;
    // loop {
    //     match user.create(&db).await {
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

    Ok(match user.create(&db).await {
        Ok(()) => succ,
        Err(_) => fail,
    })
}

pub async fn login(
    params: HashMap<String, String>,
    db: Db,
) -> Result<impl warp::Reply, warp::Rejection> {
    let username = params
        .get("username")
        .map(|username| username.to_owned())
        .map_or("".to_string(), |v| v);

    let r = warp::reply::with_status(
        super::INVALID_USERNAME_OR_PASSWORD.to_string(),
        StatusCode::BAD_REQUEST,
    );
    if username.is_empty() {
        return Ok(r);
    }

    let password = params
        .get("password")
        .map_or("".to_string(), |v| v.to_string());
    // let mut backoff = 1;
    // loop {
    //     match User::login(&db, &username, &password).await {
    //         Ok(token) => return Ok(warp::reply::with_status(token, StatusCode::OK)),
    //         Err(_) => {
    //             if backoff > 2048 {
    //                 return Ok(r);
    //             }
    //         }
    //     }

    //     time::sleep(Duration::from_millis(backoff)).await;
    //     backoff *= 2;
    // }

    Ok(match User::login(&db, &username, &password).await {
        Ok(token) => warp::reply::with_status(token, StatusCode::OK),
        Err(_) => r,
    })
}

pub async fn userinfo(username: String, db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    Ok(match User::get_userinfo(db, username).await {
        Ok(user) => warp::reply::with_status(
            warp::reply::with_header(user, "content-type", "application/json"),
            StatusCode::OK,
        ),
        Err(_) => warp::reply::with_status(
            warp::reply::with_header(
                super::INVALID_USERNAME_SUPPLIED.to_string(),
                "content-type",
                "text/plain",
            ),
            StatusCode::BAD_REQUEST,
        ),
    })
}
