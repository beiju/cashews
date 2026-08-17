use std::collections::HashSet;
use std::time::Duration;

use chron_db::models::EntityKind;
use crate::models::MmolbSeason;
use crate::workers::{IntervalWorker, WorkerContext};

pub struct PollElection;

impl IntervalWorker for PollElection {
    fn interval() -> tokio::time::Interval {
        // This barely changes, 6hrs is probably more than necessary
        tokio::time::interval(Duration::from_secs(6 * 60 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        ctx.fetch_and_save(
            "https://mmolb.com/api/election",
            EntityKind::Election,
            "election",
        )
            .await?;

        let seasons = ctx.db.get_all_latest(EntityKind::Season).await?;
        let season_num = seasons.iter()
            .map(|season| {
                let season_parsed: MmolbSeason = season.parse()?;
                Ok::<_, anyhow::Error>(season_parsed.season)
            })
            .try_fold(0, |a, b| Ok::<_, anyhow::Error>(a.max(b?)))?;
        let last_season_num = season_num - 1;

        ctx.fetch_and_save(
            format!("https://mmolb.com/api/election_history?season={last_season_num}"),
            EntityKind::ElectionHistory,
            format!("season-{last_season_num}-election"),
        )
            .await?;

        let existing_histories = ctx.db.get_all_latest(EntityKind::ElectionHistory).await?;
        let existing_history_ids: HashSet<_> = existing_histories.into_iter()
            .map(|v| v.entity_id)
            .collect();
        for season in seasons {
            let season_parsed: MmolbSeason = season.parse()?;
            let id = format!("season-{}-election", season_parsed.season);
            if !existing_history_ids.contains(&id) {
                ctx.fetch_and_save(
                    format!("https://mmolb.com/api/election_history?season={}", season_parsed.season),
                    EntityKind::ElectionHistory,
                    id,
                )
                    .await?;
            }
        }

        Ok(())
    }
}
