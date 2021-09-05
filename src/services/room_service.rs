use warp::Filter;

use super::{with_db, with_token};
use crate::handlers::room_handler;
use crate::models::Db;

pub fn init(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    create_room(db.clone())
        .or(enter_room(db.clone()))
        .or(leave_room(db.clone()))
        .or(get_room_info(db.clone()))
        .or(get_room_users(db.clone()))
        .or(get_room_list(db))
}

pub fn create_room(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("room")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_db(db))
        .and_then(room_handler::create_room)
}

pub fn enter_room(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("room" / String / "enter")
        .and(warp::put())
        .and(with_token())
        .and(with_db(db))
        .and_then(room_handler::enter_room)
}

pub fn leave_room(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("roomLeave")
        .and(warp::put())
        .and(with_token())
        .and(with_db(db))
        .and_then(room_handler::leave_room)
}

pub fn get_room_info(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("room" / String)
        .and(warp::get())
        .and(with_db(db))
        .and_then(room_handler::get_room_info)
}

pub fn get_room_users(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("room" / String / "users")
        .and(warp::get())
        .and(with_db(db))
        .and_then(room_handler::get_room_users)
}

pub fn get_room_list(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("roomList")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_db(db))
        .and_then(room_handler::get_room_list)
}
