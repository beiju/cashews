use crate::workers::{IntervalWorker, WorkerContext};
use anyhow::anyhow;
use chron_db::models::EntityKind;
use serde::Deserialize;
use std::time::Duration;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::time::{Interval, interval};
use tracing::{info, warn};

pub struct PollPlayerFeeds;
pub struct PollTeamFeeds;
// This is a replacement for PollPlayerFeeds and PollTeamFeeds.
// Those two will be deleted once this is confirmed working.
pub struct PollFeeds;

#[derive(Deserialize)]
struct FeedEvents {
    events: Vec<serde_json::Value>,
    has_more: bool,
    limit: Option<serde_json::Value>, // Be fully error tolerant
    next_cursor: Option<String>,
}

impl IntervalWorker for PollPlayerFeeds {
    fn interval() -> Interval {
        interval(Duration::from_secs(5 * 60 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let player_ids = ctx.db.get_all_entity_ids(EntityKind::Player).await?;

        ctx.process_many_with_progress(player_ids, 10, "player feeds", fetch_player_feed)
            .await;
        Ok(())
    }
}

impl IntervalWorker for PollTeamFeeds {
    fn interval() -> Interval {
        interval(Duration::from_secs(60 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let team_ids = ctx.db.get_all_entity_ids(EntityKind::Team).await?;

        ctx.process_many_with_progress(team_ids, 10, "team feeds", fetch_team_feed)
            .await;
        Ok(())
    }
}

async fn fetch_player_feed(ctx: &WorkerContext, player_id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/feed?player={}", &player_id);
    let _ = ctx
        .fetch_and_save_paginated_feed(url, EntityKind::PlayerFeed, player_id)
        .await?;

    // todo: do anything immediately, or wait for ProcessFeeds to come around?
    Ok(())
}

async fn fetch_team_feed(ctx: &WorkerContext, team_id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/feed?team={}", &team_id);
    let _ = ctx
        .fetch_and_save_paginated_feed(url, EntityKind::TeamFeed, team_id)
        .await?;

    // todo: do anything immediately, or wait for ProcessFeeds to come around?
    Ok(())
}

fn str_from_json_value<'a>(events: impl IntoIterator<Item = &'a serde_json::Value>, key: &str) -> anyhow::Result<Vec<&'a str>> {
    events.into_iter()
        .map(|event| {
            event[key].as_str()
                .ok_or_else(|| anyhow!("Event's {} is missing or not a string", key))
        })
        .collect()
}

const FEED_EVENTS_API_URL: &str = "https://mmolb.com/api/feed/events";

// The max, per Danny, as of Aug 1 2026
const FEED_EVENTS_REQUEST_LIMIT: i64 = 1000;

impl IntervalWorker for PollFeeds {
    fn interval() -> Interval {
        interval(Duration::from_secs(15 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let mut cursor = ctx.db.get_feed_cursor().await?
            .map(|cursor| format!("{}|{}", cursor.timestamp_str, cursor.event_id));
        info!("Starting from feed cursor {:?}", cursor);

        loop {
            let url = if let Some(cursor) = &cursor {
                format!("{FEED_EVENTS_API_URL}?limit={FEED_EVENTS_REQUEST_LIMIT}&cursor={}", cursor)
            } else {
                format!("{FEED_EVENTS_API_URL}?limit={FEED_EVENTS_REQUEST_LIMIT}")
            };

            let response = ctx.client.fetch(url).await?;

            let container: FeedEvents = response.parse()?;

            if let Some(limit_value) = &container.limit {
                if let Some(limit) = limit_value.as_i64() {
                    if limit != FEED_EVENTS_REQUEST_LIMIT {
                        warn!(
                            "Feed API response `limit` ({limit}) did not match the requested limit \
                            ({FEED_EVENTS_REQUEST_LIMIT})",
                        );
                    }
                } else {
                    warn!("Feed API response `limit` value was not an integer");
                }
            } else {
                warn!("Feed API response had no `limit` field");
            }

            if container.events.is_empty() {
                info!("Feed event ingest finished after a fetch with 0 events");
                break;
            }

            let event_ids = str_from_json_value(&container.events, "_id")?;
            let subject_types = str_from_json_value(&container.events, "subject_type")?;
            let subject_ids = str_from_json_value(&container.events, "subject_id")?;
            let timestamp_strs = str_from_json_value(&container.events, "ts")?;
            let request_starts = event_ids.iter()
                .map(|_| response.timestamp_before)
                .collect::<Vec<_>>();
            let request_ends = event_ids.iter()
                .map(|_| response.timestamp_after)
                .collect::<Vec<_>>();
            let timestamps = timestamp_strs.iter()
                .map(|timestamp_str| {
                    OffsetDateTime::parse(timestamp_str, &Rfc3339)
                })
                .collect::<Result<Vec<_>, _>>()?;

            info!("Saving {} new feed events", event_ids.len());
            ctx.db.save_feed_events(
                &event_ids,
                &subject_types,
                &subject_ids,
                &timestamps,
                &timestamp_strs,
                &request_starts,
                &request_ends,
                &container.events,
            ).await?;

            if !container.has_more {
                info!("Feed event ingest finished due to has_more == false");
                break;
            }
            if let Some(next_cursor) = container.next_cursor {
                cursor = Some(next_cursor);
            } else {
                info!("Feed event ingest finished due to next_cursor == null");
                break;
            }
        }

        Ok(())
    }
}
