use std::collections::HashMap;

use warp::Filter;

use crate::handlers::user_handler;
use crate::models::Db;
use crate::services::with_db;

pub fn init(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    register(db.clone()).or(login(db.clone())).or(userinfo(db))
}

pub fn register(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("user")
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .and(with_db(db))
        .and_then(user_handler::register)
}

pub fn login(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("userLogin")
        .and(warp::get())
        .and(warp::query::<HashMap<String, String>>())
        .and(with_db(db))
        .and_then(user_handler::login)
}

pub fn userinfo(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("user" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(user_handler::userinfo)
}
