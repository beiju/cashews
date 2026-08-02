use std::time::{Duration, Instant};

use tracing::{info, warn};

use super::IntervalWorker;

pub struct RefreshMatviews;

impl IntervalWorker for RefreshMatviews {
    fn interval() -> tokio::time::Interval {
        tokio::time::interval(Duration::from_secs(10 * 60))
    }

    async fn tick(&mut self, ctx: &mut super::WorkerContext) -> anyhow::Result<()> {
        // I have left this code here in case I ever reintroduce any matviews, 
        // but as I write this there are no matviews left
        let matviews: [&str; _] = [];
        for matview in matviews {
            info!("refreshing matview {}...", matview);

            let mut tx = ctx.db.pool.begin().await?;
            let lock: bool = sqlx::query_scalar("select pg_try_advisory_xact_lock(0x13371337)")
                .fetch_one(&mut *tx)
                .await?;
            if !lock {
                warn!("failed to claim advisory xact lock for matview refresh");
                break;
            }

            let time_before = Instant::now();
            sqlx::query(&format!(
                "refresh materialized view concurrently {}",
                matview
            ))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            let time_after = Instant::now();

            let delta = time_after.duration_since(time_before);
            info!(
                "refreshed matview {} (took {}s)",
                matview,
                delta.as_secs_f32()
            );
        }

        Ok(())
    }
}
