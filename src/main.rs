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
const OFFSET_ALIGNMENT: u32 = 1 << 20; // 1Mb

#[derive(Clone)]
pub struct AppState {
    pub hostname: String,
    pub fallback_subdomain: Option<String>,
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

    let fallback_subdomain = env::var("FALLBACK_SUBDOMAIN").ok();

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
                fallback_subdomain: fallback_subdomain.clone(),
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

#[get("/{first_segment}/{path:.*}")]
async fn get_file(request: HttpRequest, app_state: web::Data<AppState>) -> impl Responder {
    let first_segment = request.match_info().get("first_segment").unwrap();
    let raw_path = request.match_info().get("path").unwrap();
    let hostname = request.connection_info().host().to_string();

    // Check if this is a fallback subdomain request
    let is_fallback = app_state.fallback_subdomain.as_ref().is_some_and(|fb| {
        hostname
            .strip_suffix(&app_state.hostname)
            .map_or(false, |s| s == fb.trim_end_matches('.'))
    });

    let (predecessor_account_id, namespace_account_id, mut path) = if is_fallback {
        // fallback.fastfs.io/root.near/fastfs.near/file.jpg
        // first_segment = predecessor_id, path starts with account_id/...
        let predecessor: AccountId = match first_segment.parse() {
            Ok(id) => id,
            Err(_) => return HttpResponse::BadRequest().body("Invalid predecessor account id"),
        };
        let (account_id, file_path) = match raw_path.split_once('/') {
            Some((acc, p)) => (acc.to_string(), p.to_string()),
            None => (raw_path.to_string(), String::new()),
        };
        (predecessor, account_id, file_path)
    } else {
        // root.near.fastfs.io/fastfs.near/file.jpg
        let predecessor: Option<AccountId> = hostname
            .strip_suffix(&app_state.hostname)
            .and_then(|s| s.parse().ok());
        if predecessor.is_none() {
            return HttpResponse::InternalServerError().finish();
        }
        (
            predecessor.unwrap(),
            first_segment.to_string(),
            raw_path.to_string(),
        )
    };

    if path.is_empty() || path.ends_with('/') {
        path += "index.html";
    }
    tracing::info!(target: PROJECT_ID, "GET {} {} {}", predecessor_account_id, namespace_account_id, path);

    let res = app_state
        .scylladb
        .get_fastfs(
            predecessor_account_id.as_str(),
            &namespace_account_id,
            &path,
        )
        .await;
    let data = match res {
        Ok(data) => data,
        Err(e) => {
            tracing::error!(target: PROJECT_ID, "Error getting fastfs data, {}", e);
            return HttpResponse::InternalServerError().finish();
        }
    };

    let (first, rest) = {
        let mut iter = data.into_iter();
        let first = iter.next();
        let rest: Vec<_> = iter.collect();
        (first, rest)
    };

    if let Some(first) = first {
        if first.offset == 0 {
            if let (Some(mime_type), Some(content)) = (first.mime_type, first.content) {
                let mut response = HttpResponse::Ok();
                let response = response
                    .append_header((header::CONTENT_TYPE, mime_type))
                    .append_header(("x-fastdata-receipt-id", first.receipt_id.to_string()))
                    .append_header((
                        "x-fastdata-tx-hash",
                        first.tx_hash.map(|h| h.to_string()).unwrap_or_default(),
                    ))
                    .append_header(("x-fastdata-block-height", first.block_height.to_string()))
                    .append_header((
                        "x-fastdata-block-timestamp",
                        first.block_timestamp.to_string(),
                    ))
                    .append_header(("x-fastdata-nonce", first.nonce.to_string()));
                if content.len() as u32 == first.full_size {
                    return response
                        .append_header(("x-fastdata-parts", "1"))
                        .body(content);
                }
                let expected_parts = (first.full_size + OFFSET_ALIGNMENT - 1) / OFFSET_ALIGNMENT;
                let mut num_parts: usize = 1;
                let mut full_content = vec![0u8; first.full_size as usize];
                full_content[first.offset as usize..first.offset as usize + content.len()]
                    .copy_from_slice(&content);
                for part in rest {
                    if part.nonce == first.nonce {
                        if let Some(part_content) = part.content {
                            if part.offset >= first.full_size {
                                break;
                            }
                            let right_bound = std::cmp::min(
                                full_content.len(),
                                part.offset as usize + part_content.len(),
                            );
                            full_content[part.offset as usize..right_bound]
                                .copy_from_slice(&part_content);
                            num_parts += 1;
                        }
                    }
                }
                if num_parts as u32 == expected_parts {
                    return response
                        .append_header(("x-fastdata-parts", num_parts.to_string()))
                        .body(full_content);
                }
            }
        }
    }
    HttpResponse::NotFound().finish()
}
