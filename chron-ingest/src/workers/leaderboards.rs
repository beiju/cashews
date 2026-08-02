use std::time::Duration;

use chron_db::models::EntityKind;

use crate::workers::{IntervalWorker, WorkerContext};

pub struct PollLeaderboards;

impl IntervalWorker for PollLeaderboards {
    fn interval() -> tokio::time::Interval {
        // Word of Danny is that these are only updated once an hour.
        // It's also a huge object, so I don't want to poll it more than necessary
        tokio::time::interval(Duration::from_secs(60 * 60))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        ctx
            .fetch_and_save(
                "https://mmolb.com/api/leaderboards",
                EntityKind::Leaderboard,
                "leaderboards",
            )
            .await?;

        Ok(())
    }
}
