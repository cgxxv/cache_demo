use warp::Filter;

use super::{with_db, with_token};
use crate::handlers::message_handler;
use crate::models::Db;

pub fn init(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    send(db.clone()).or(retrieve(db))
}

pub fn send(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("message" / "send")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_token())
        .and(with_db(db))
        .and_then(message_handler::send)
}

pub fn retrieve(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("message" / "retrieve")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_token())
        .and(with_db(db))
        .and_then(message_handler::retrieve)
}
