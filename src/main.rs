use dotenv::dotenv;
use std::env;

use actix_cors::Cors;
use actix_web::http::header;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use fastnear_primitives::near_indexer_primitives::types::AccountId;
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;

#[derive(Clone)]
pub struct AppState {
    pub redis_client: redis::Client,
    pub hostname: String,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FastfsFileContent {
    pub mime_type: String,
    #[serde_as(as = "Base64")]
    pub content: Vec<u8>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    openssl_probe::init_ssl_cert_env_vars();
    dotenv().ok();

    let redis_client =
        redis::Client::open(env::var("REDIS_URL").expect("Missing REDIS_URL env var"))
            .expect("Failed to connect to Redis");

    let hostname = env::var("HOSTNAME").expect("Missing HOSTNAME env var");

    tracing_subscriber::fmt()
        .with_env_filter("redis=info")
        .with_env_filter("api=info")
        .init();

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
                redis_client: redis_client.clone(),
                hostname: hostname.clone(),
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
    let path = request.match_info().get("path").unwrap();
    let hostname = request.connection_info().host().to_string();
    let predecessor_account_id: Option<AccountId> = hostname
        .strip_suffix(&app_state.hostname)
        .and_then(|s| s.parse().ok());
    if predecessor_account_id.is_none() {
        return HttpResponse::InternalServerError().finish();
    }

    let key = format!(
        "fastfs:{}:{}:{}",
        predecessor_account_id.unwrap(),
        namespace_account_id,
        path
    );
    tracing::info!(target: "api", "GET {}", key);

    let data: Option<String> = redis::cmd("GET")
        .arg(key)
        .query_async(
            &mut app_state
                .redis_client
                .get_multiplexed_async_connection()
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    if let Some(data) = data {
        if let Ok(FastfsFileContent { mime_type, content }) = serde_json::from_str(&data) {
            HttpResponse::Ok()
                .append_header((header::CONTENT_TYPE, mime_type))
                .body(content)
        } else {
            HttpResponse::InternalServerError().finish()
        }
    } else {
        HttpResponse::NotFound().finish()
    }
}
