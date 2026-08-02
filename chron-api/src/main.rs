use std::{sync::Arc};

use axum::{
    Router,
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use chron_base::{ChronConfig, load_config, stop_signal};
use chron_db::ChronDb;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    services::ServeDir,
    trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::info;

mod chron_api;

#[derive(Clone)]
pub struct AppState {
    config: Arc<ChronConfig>,
    db: ChronDb,
}

pub struct AppError(anyhow::Error);

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let config = load_config()?;
    let db = ChronDb::new(&config).await?;

    let state = AppState {
        db,
        config: Arc::new(config),
    };

    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    let trace = TraceLayer::new_for_http()
        .on_request(DefaultOnRequest::new())
        .on_response(DefaultOnResponse::new());

    let mut app = Router::new()
        .route("/chron/v0/entities", get(chron_api::get_entities))
        .route("/chron/v0/versions", get(chron_api::get_versions));

    if let Some(dir) = &state.config.export_path {
        dbg!(dir);
        app = app.nest_service("/export", ServeDir::new(dir));
    }

    let app = app
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(trace)
        .with_state(state);

    let addr = "0.0.0.0:3001";
    info!("starting api at {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let serve_fut = axum::serve(listener, app);
    let ctrl_c_fut = stop_signal();

    tokio::select! {
        res = serve_fut => res?,
        _ = ctrl_c_fut => {}
    }

    Ok(())
}
