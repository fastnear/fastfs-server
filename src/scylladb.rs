use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;

use fastnear_primitives::near_indexer_primitives::types::BlockHeight;
use fastnear_primitives::near_indexer_primitives::CryptoHash;
use fastnear_primitives::types::ChainId;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConfig, RootCertStore};
use scylla::DeserializeRow;
use std::env;
use std::sync::Arc;

#[derive(DeserializeRow)]
pub struct FastfsRow {
    pub receipt_id: String,
    pub tx_hash: Option<String>,
    pub block_height: i64,
    pub block_timestamp: i64,
    pub mime_type: Option<String>,
    pub content: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct FastfsParsedRow {
    pub receipt_id: CryptoHash,
    pub tx_hash: Option<CryptoHash>,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub mime_type: Option<String>,
    pub content: Option<Vec<u8>>,
}

impl From<FastfsRow> for FastfsParsedRow {
    fn from(row: FastfsRow) -> Self {
        Self {
            receipt_id: row.receipt_id.parse().unwrap(),
            tx_hash: row.tx_hash.map(|h| h.parse().unwrap()),
            block_height: row.block_height as u64,
            block_timestamp: row.block_timestamp as u64,
            mime_type: row.mime_type,
            content: row.content,
        }
    }
}

pub struct ScyllaDb {
    select_fastfs: PreparedStatement,

    pub scylla_session: Session,
}

pub fn create_rustls_client_config() -> Arc<ClientConfig> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("Failed to install default provider");
    }
    let ca_cert_path =
        env::var("SCYLLA_SSL_CA").expect("SCYLLA_SSL_CA environment variable not set");
    let client_cert_path =
        env::var("SCYLLA_SSL_CERT").expect("SCYLLA_SSL_CERT environment variable not set");
    let client_key_path =
        env::var("SCYLLA_SSL_KEY").expect("SCYLLA_SSL_KEY environment variable not set");

    let ca_certs = rustls::pki_types::CertificateDer::from_pem_file(ca_cert_path)
        .expect("Failed to load CA certs");
    let client_certs = rustls::pki_types::CertificateDer::from_pem_file(client_cert_path)
        .expect("Failed to load client certs");
    let client_key = rustls::pki_types::PrivateKeyDer::from_pem_file(client_key_path)
        .expect("Failed to load client key");

    let mut root_store = RootCertStore::empty();
    root_store.add(ca_certs).expect("Failed to add CA certs");

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(vec![client_certs], client_key)
        .expect("Failed to create client config");

    Arc::new(config)
}

impl ScyllaDb {
    pub async fn new_scylla_session() -> anyhow::Result<Session> {
        let scylla_url = env::var("SCYLLA_URL").expect("SCYLLA_DB_URL must be set");
        let scylla_username = env::var("SCYLLA_USERNAME").expect("SCYLLA_USERNAME must be set");
        let scylla_password = env::var("SCYLLA_PASSWORD").expect("SCYLLA_PASSWORD must be set");

        let session: Session = SessionBuilder::new()
            .known_node(scylla_url)
            .tls_context(Some(create_rustls_client_config()))
            .authenticator_provider(Arc::new(
                scylla::authentication::PlainTextAuthenticator::new(
                    scylla_username,
                    scylla_password,
                ),
            ))
            .build()
            .await?;

        Ok(session)
    }

    pub async fn test_connection(scylla_session: &Session) -> anyhow::Result<()> {
        scylla_session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await?;
        Ok(())
    }

    pub async fn new(chain_id: ChainId, scylla_session: Session) -> anyhow::Result<Self> {
        scylla_session
            .use_keyspace(format!("fastdata_{chain_id}"), false)
            .await?;

        Ok(Self {
            select_fastfs: Self::prepare_query(
                &scylla_session,
                "SELECT receipt_id, tx_hash, block_height, block_timestamp, mime_type, content FROM s_fastfs WHERE predecessor_id = ? AND current_account_id = ? AND relative_path = ? LIMIT 1",
                scylla::frame::types::Consistency::LocalOne,
            ).await?,
            scylla_session,
        })
    }

    pub async fn prepare_query(
        scylla_db_session: &Session,
        query_text: &str,
        consistency: scylla::frame::types::Consistency,
    ) -> anyhow::Result<PreparedStatement> {
        let mut query = scylla::statement::Statement::new(query_text);
        query.set_consistency(consistency);
        Ok(scylla_db_session.prepare(query).await?)
    }

    pub async fn get_fastfs(
        &self,
        predecessor_id: &str,
        current_account_id: &str,
        relative_path: &str,
    ) -> anyhow::Result<Option<FastfsParsedRow>> {
        let rows = self
            .scylla_session
            .execute_unpaged(
                &self.select_fastfs,
                (
                    predecessor_id.to_string(),
                    current_account_id.to_string(),
                    relative_path.to_string(),
                ),
            )
            .await?
            .into_rows_result()?;
        Ok(rows.single_row::<FastfsRow>().ok().map(|v| v.into()))
    }
}
