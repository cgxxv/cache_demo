use warp::Filter;

use crate::models::Db;

fn with_db(db: Db) -> impl Filter<Extract = (Db,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || db.clone())
}

fn with_token() -> impl Filter<Extract = (String,), Error = warp::Rejection> + Copy {
    warp::header::<String>("Authorization").map(|v: String| {
        if v.len() <= 7 {
            "".to_string()
        } else {
            v[7..].to_owned()
        }
    })
}

pub mod message_service;
pub mod room_service;
pub mod user_service;
pub mod cluster_service;
