use anyhow::anyhow;
use config::Config;
use serde::Deserialize;
use time::OffsetDateTime;
use unicode_normalization::UnicodeNormalization;


#[derive(Deserialize)]
pub struct ChronConfig {
    pub database_uri: String,
    pub export_path: Option<String>,

    #[serde(default)]
    pub jitter: bool,
}

pub fn normalize_location(s: &str) -> String {
    s.to_lowercase().nfkc().to_string()
}

pub fn load_config() -> anyhow::Result<ChronConfig> {
    // maybe we shouldn't do this here idk
    // tracing_subscriber::fmt::init();
    tracing_subscriber::fmt().compact().without_time().init();

    let settings = Config::builder()
        .add_source(config::File::with_name("config"))
        .add_source(config::Environment::with_prefix("CHRON"))
        .build()?
        .try_deserialize()?;
    Ok(settings)
}

pub fn objectid_to_timestamp(id: &str) -> anyhow::Result<OffsetDateTime> {
    if id.len() != 24 {
        return Err(anyhow!("not a valid objectid"));
    }

    let mut data = [0u8; 12];
    hex::decode_to_slice(id, &mut data)?;

    let unix_timestamp = u32::from_be_bytes(data[0..4].try_into()?);
    Ok(OffsetDateTime::from_unix_timestamp(unix_timestamp as i64)?)
}

pub async fn stop_signal() -> tokio::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::{self, unix::SignalKind};

        let mut int_fut = signal::unix::signal(SignalKind::interrupt())?;
        let mut term_fut = signal::unix::signal(SignalKind::terminate())?;

        tokio::select! {
            _ = int_fut.recv() => {},
            _ = term_fut.recv() => {}
        }

        Ok(())
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await
    }
}
