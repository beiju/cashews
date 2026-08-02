use std::{sync::Arc, time::Duration};

use chron_db::models::{EntityKind, NewObject};
use reqwest::{Client, ClientBuilder, IntoUrl, StatusCode, Url};
use serde::Deserialize;
use serde::de::{Deserializer, DeserializeOwned};
use time::OffsetDateTime;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct DataClient {
    client: Client,
    semaphore: Arc<Semaphore>, // cached_responses: Arc<DashMap<String, ClientResponse>>,
}

#[derive(Debug, Clone)]
pub struct ClientResponse {
    pub _url: Url,
    pub timestamp_before: OffsetDateTime,
    pub timestamp_after: OffsetDateTime,
    // pub etag: Option<String>,
    // pub content_type: Option<String>,
    // pub last_modified: Option<String>,
    pub data: Vec<u8>,
    pub _status_code: StatusCode,
    pub _was_cached: bool,
}

// I got this from https://stackoverflow.com/a/44331646/522118
fn deserialize_optional_field<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::deserialize(deserializer)?))
}

#[derive(Deserialize)]
struct FeedHolder {
    feed: Vec<serde_json::Value>,

    // this field was added in the season 11 preseason
    // outer option: is the field present, inner option: is the field null
    #[serde(deserialize_with = "deserialize_optional_field")]
    next_cursor: Option<Option<String>>,
}

impl ClientResponse {
    pub fn parse<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(&self.data)?)
    }

    pub fn to_chron(&self, kind: EntityKind, entity_id: &str) -> anyhow::Result<NewObject> {
        let parsed = serde_json::from_slice(&self.data)?;

        Ok(NewObject {
            data: parsed,
            kind,
            entity_id: entity_id.to_string(),
            request_time: self.request_time(),
            timestamp: self.timestamp(),
        })
    }

    pub fn request_time(&self) -> time::Duration {
        self.timestamp_after - self.timestamp_before
    }

    pub fn timestamp(&self) -> OffsetDateTime {
        self.timestamp_before
    }
}

impl DataClient {
    pub fn new() -> anyhow::Result<DataClient> {
        let client = ClientBuilder::new()
            .deflate(true)
            .zstd(true)
            .brotli(true)
            .gzip(true)
            .use_rustls_tls()
            .build()?;

        let semaphore = Arc::new(Semaphore::new(20));

        Ok(DataClient { client, semaphore })
    }

    pub async fn fetch(&self, orig_url: impl IntoUrl) -> anyhow::Result<ClientResponse> {
        let _permit = self.semaphore.acquire().await?;

        let request = self.client.get(orig_url);
        // if let Some(cached_etag) = self
        //     .cached_responses
        //     .get(orig_url)
        //     .and_then(|x| x.etag.clone())
        // {
        //     request = request.header(header::IF_NONE_MATCH, cached_etag);
        // }

        let timestamp_before = OffsetDateTime::now_utc();
        let response = request.send().await?;
        let timestamp_after = OffsetDateTime::now_utc();
        debug!(
            "{} {} ({}s)",
            response.status(),
            response.url(),
            (timestamp_after - timestamp_before).as_seconds_f64()
        );

        let url = response.url().clone();
        // let last_modified = response
        //     .headers()
        //     .get(header::LAST_MODIFIED)
        //     .and_then(|x| x.to_str().ok())
        //     .map(|x| x.to_owned());
        // let content_type = response
        //     .headers()
        //     .get(header::CONTENT_TYPE)
        //     .and_then(|x| x.to_str().ok())
        //     .map(|x| x.to_owned());
        // let etag = response
        //     .headers()
        //     .get(header::ETAG)
        //     .and_then(|x| x.to_str().ok())
        //     .map(|x| x.to_owned());
        let status_code = response.status();
        if status_code == StatusCode::BAD_GATEWAY {
            // if we get a 502 from the server, sleep for a second
            // because we're still within the semaphore, this basically functions as a light "circuit breaker"
            // and will slow down at least one "slot" of the available permits
            warn!("received 502 response, sleeping for a bit");
            let _cb_permit = self
                .semaphore
                .acquire_many(self.semaphore.available_permits() as u32)
                .await?;
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        let response = response.error_for_status()?;

        // if response.status() == StatusCode::NOT_MODIFIED {
        //     if let Some(resp) = self.cached_responses.get(orig_url) {
        //         if resp.etag == etag {
        //             let mut cached_resp = resp.clone();
        //             cached_resp.status_code = response.status();
        //             cached_resp.was_cached = true;
        //             cached_resp.timestamp_before = timestamp_before;
        //             cached_resp.timestamp_after = timestamp_after;
        //             return Ok(cached_resp);
        //         }
        //     }
        // }

        let data = response.bytes().await?.to_vec();

        let sr = ClientResponse {
            _url: url,
            timestamp_before,
            timestamp_after,
            // etag,
            data,
            // content_type,
            // last_modified,
            _status_code: status_code,
            _was_cached: false,
        };

        // if sr.etag.is_some() {
        //     self.cached_responses
        //         .insert(orig_url.to_string(), sr.clone());
        // }

        Ok(sr)
    }

    pub async fn fetch_paginated_feed(&self, base_url: String) -> anyhow::Result<ClientResponse> {
        let _permit = self.semaphore.acquire().await?;

        let mut cursor = None;
        let mut fetched_events_reverse = Vec::new();
        let timestamp_before = OffsetDateTime::now_utc();
        // For now, we pretend that this all happens in a single fetch
        let (last_status_code, fetched_events_forward) = loop {
            let url = if let Some(cursor) = cursor {
                format!("{base_url}&limit=100&cursor={cursor}")
            } else {
                format!("{base_url}&limit=100")
            };
            let request = self.client.get(url);
            let page_timestamp_before = OffsetDateTime::now_utc();
            let response = request.send().await?;
            let page_timestamp_after = OffsetDateTime::now_utc();
            debug!(
                "{} {} ({}s)",
                response.status(),
                response.url(),
                (page_timestamp_after - page_timestamp_before).as_seconds_f64()
            );

            let status_code = response.status();
            if status_code == StatusCode::BAD_GATEWAY {
                // if we get a 502 from the server, sleep for a second
                // because we're still within the semaphore, this basically functions as a light "circuit breaker"
                // and will slow down at least one "slot" of the available permits
                warn!("received 502 response, sleeping for a bit");
                let _cb_permit = self
                    .semaphore
                    .acquire_many(self.semaphore.available_permits() as u32)
                    .await?;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            let response = response.error_for_status()?;

            let obj: FeedHolder = response.json().await?;
            match obj {
                // New format
                FeedHolder {
                    feed,
                    next_cursor: Some(next_cursor),
                } => {
                    fetched_events_reverse.extend(feed);
                    // Yes, it does make sense to unwrap the option and then re-wrap it
                    if let Some(next_cursor) = next_cursor {
                        cursor = Some(next_cursor);
                    } else {
                        fetched_events_reverse.reverse();
                        break (status_code, fetched_events_reverse);
                    }
                }
                // Old format
                FeedHolder {
                    feed,
                    next_cursor: _,  // must be None thanks to the prior pattern matching Some
                } => {
                    break (status_code, feed);
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let timestamp_after = OffsetDateTime::now_utc();
        debug!(
            "All pages for {}: {}s",
            base_url,
            (timestamp_after - timestamp_before).as_seconds_f64()
        );

        let object = serde_json::json!({
            "feed": fetched_events_forward,
        });
        let data = serde_json::to_vec(&object)?;

        let sr = ClientResponse {
            _url: Url::parse(&base_url)?,
            timestamp_before,
            timestamp_after,
            data,
            _status_code: last_status_code,
            _was_cached: false,
        };

        Ok(sr)
    }
}
