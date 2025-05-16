mod scylladb;

use crate::scylladb::ScyllaDb;
use actix_cors::Cors;
use actix_web::http::header;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use dotenv::dotenv;
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use fastnear_primitives::types::ChainId;
use std::env;
use std::sync::Arc;

const PROJECT_ID: &str = "fastfs-server";

#[derive(Clone)]
pub struct AppState {
    pub hostname: String,
    pub scylladb: Arc<ScyllaDb>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter("scylladb=info,fastfs-server=info")
        .init();

    let hostname = env::var("HOSTNAME").expect("Missing HOSTNAME env var");

    let chain_id: ChainId = env::var("CHAIN_ID")
        .expect("CHAIN_ID required")
        .try_into()
        .expect("Invalid chain id");

    let scylla_session = ScyllaDb::new_scylla_session()
        .await
        .expect("Can't create scylla session");

    ScyllaDb::test_connection(&scylla_session)
        .await
        .expect("Can't connect to scylla");

    tracing::info!(target: PROJECT_ID, "Connected to Scylla");

    let scylladb = Arc::new(
        ScyllaDb::new(chain_id, scylla_session)
            .await
            .expect("Can't create scylla db"),
    );

    HttpServer::new(move || {
        // Configure CORS middleware
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET"])
            .allowed_headers(vec![
                header::CONTENT_TYPE,
                header::AUTHORIZATION,
                header::ACCEPT,
            ])
            .max_age(3600)
            .supports_credentials();

        App::new()
            .app_data(web::Data::new(AppState {
                hostname: hostname.clone(),
                scylladb: Arc::clone(&scylladb),
            }))
            .wrap(cors)
            .wrap(middleware::Logger::new(
                "%{r}a \"%r\"	%s %b \"%{Referer}i\" \"%{User-Agent}i\" %T",
            ))
            .wrap(tracing_actix_web::TracingLogger::default())
            .service(get_file)
    })
    .bind(format!("127.0.0.1:{}", env::var("PORT").unwrap()))?
    .run()
    .await?;

    Ok(())
}

#[get("/{account_id}/{path:.*}")]
async fn get_file(request: HttpRequest, app_state: web::Data<AppState>) -> impl Responder {
    let namespace_account_id = request.match_info().get("account_id").unwrap();
    let mut path = request.match_info().get("path").unwrap().to_string();
    if path.is_empty() || path.ends_with('/') {
        path += "index.html";
    }
    let hostname = request.connection_info().host().to_string();
    let predecessor_account_id: Option<AccountId> = hostname
        .strip_suffix(&app_state.hostname)
        .and_then(|s| s.parse().ok());
    if predecessor_account_id.is_none() {
        return HttpResponse::InternalServerError().finish();
    }
    let predecessor_account_id = predecessor_account_id.unwrap();
    tracing::info!(target: PROJECT_ID, "GET {} {} {}", predecessor_account_id, namespace_account_id, path);

    let res = app_state
        .scylladb
        .get_fastfs(predecessor_account_id.as_str(), namespace_account_id, &path)
        .await;
    let data = match res {
        Ok(data) => data,
        Err(e) => {
            tracing::error!(target: PROJECT_ID, "Error getting fastfs data, {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    if let Some(data) = data {
        if let (Some(mime_type), Some(content)) = (data.mime_type, data.content) {
            return HttpResponse::Ok()
                .append_header((header::CONTENT_TYPE, mime_type))
                .append_header(("x-fastdata-receipt-id", data.receipt_id.to_string()))
                .append_header((
                    "x-fastdata-tx-hash",
                    data.tx_hash.map(|h| h.to_string()).unwrap_or_default(),
                ))
                .append_header(("x-fastdata-block-height", data.block_height.to_string()))
                .append_header((
                    "x-fastdata-block-timestamp",
                    data.block_timestamp.to_string(),
                ))
                .body(content);
        }
    }
    HttpResponse::NotFound().finish()
}
