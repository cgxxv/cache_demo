use warp::Filter;

use crate::handlers::cluster_handler;
use crate::models::Db;
use crate::services::with_db;

pub fn init(db: Db) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    update_cluster(db.clone()).or(check_cluster(db))
}

pub fn update_cluster(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("updateCluster")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_db(db))
        .and_then(cluster_handler::update_cluster)
}

pub fn check_cluster(
    db: Db,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("checkCluster")
        .and(warp::get())
        .and(with_db(db))
        .and_then(cluster_handler::check_cluster)
}
