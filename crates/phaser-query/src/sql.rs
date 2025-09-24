use crate::catalog::RocksDbCatalog;
use anyhow::Result;
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

pub struct SqlServer {
    catalog: Arc<RocksDbCatalog>,
    port: u16,
}

#[derive(Debug, Deserialize)]
struct SqlQuery {
    query: String,
}

#[derive(Debug, Serialize)]
struct SqlResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

impl SqlServer {
    pub async fn new(catalog: Arc<RocksDbCatalog>, port: u16) -> Result<Self> {
        Ok(Self { catalog, port })
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting SQL server on port {}", self.port);

        let app = Router::new()
            .route("/query", post(handle_sql_query))
            .with_state(self.catalog.clone());

        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", self.port)).await?;

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn handle_sql_query(
    State(_catalog): State<Arc<RocksDbCatalog>>,
    Json(_query): Json<SqlQuery>,
) -> impl IntoResponse {
    // TODO: Parse SQL using DataFusion
    // TODO: Execute query against Parquet files using indexes
    // TODO: Return results

    let response = SqlResponse {
        columns: vec!["placeholder".to_string()],
        rows: vec![vec![serde_json::json!("TODO: Implement SQL execution")]],
    };

    (StatusCode::OK, Json(response))
}
