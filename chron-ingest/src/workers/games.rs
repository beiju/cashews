use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    time::Duration,
};

use chron_db::models::EntityKind;
use serde::Deserialize;
use tokio::time::interval;
use tracing::{error, info};

use crate::{
    models::{MmolbDay, MmolbGame, MmolbSeason},
    workers::{IntervalWorker, WorkerContext},
};
use futures::TryStreamExt;
use chron_db::DbGameSaveModel;

pub struct PollGameDays;

pub struct HandleEventGames;

impl IntervalWorker for PollGameDays {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        info!("Start PollGameDays tick");
        let state = ctx.try_update_state().await?;
        info!("PollGameDays updated state");

        // todo: loop multiple seasons?
        let season_id = state.season_id;
        handle_season(ctx, season_id.clone()).await?;
        info!("PollGameDays updated season");

        // ok, now that we've saved all the days, query all the unfinished games
        // todo: only run this for current season?
        let mut game_ids_to_poll: HashSet<String> =
            HashSet::from_iter(get_all_game_ids_from_days(ctx, Some(&season_id)).await?);
        info!("PollGameDays got all {} game ids from season {season_id}", game_ids_to_poll.len());
        for known_complete in query_completed_game_ids(&ctx).await? {
            game_ids_to_poll.remove(&known_complete);
        }
        info!("PollGameDays filtered to {} non-known-complete games in {season_id}", game_ids_to_poll.len());

        ctx.process_many_with_progress(
            game_ids_to_poll,
            25,
            "games",
            // I changed this from fetch_game_if_not_known_completed because these games should never be known completed
            poll_game_by_id,
        )
        .await;

        Ok(())
    }
}

pub struct HandleSuperstarGames;

#[derive(Deserialize)]
struct SuperstarGamesResponse {
    games: Vec<SuperstarGame>,
}

#[derive(Deserialize)]
// i swear every endpoint that returns games has a different schema...
struct SuperstarGame {
    #[serde(default)]
    game_id: Option<String>,
}

impl IntervalWorker for HandleSuperstarGames {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let resp = ctx
            .fetch_and_save(
                "https://mmolb.com/api/superstar-games",
                EntityKind::SuperstarGames,
                "superstar-games",
            )
            .await?;

        let game_ids = resp
            .parse::<SuperstarGamesResponse>()?
            .games
            .into_iter()
            .flat_map(|x| x.game_id)
            .collect::<Vec<_>>();

        let resp = ctx
            .fetch_and_save(
                "https://mmolb.com/api/super16-bracket",
                EntityKind::Super16Bracket,
                "super16-bracket",
            )
            .await?;

        poll_games(&ctx, &game_ids).await?;
        Ok(())
    }
}

// mostly just a quick hack to make sure we get the game IDs from the state object in as well
// for eg. exhibition games
// TODO(beiju) figure out if this is necessary still
impl IntervalWorker for HandleEventGames {
    fn interval() -> tokio::time::Interval {
        interval(Duration::from_secs(60 * 5))
    }

    async fn tick(&mut self, ctx: &mut WorkerContext) -> anyhow::Result<()> {
        let state = ctx.try_update_state().await?;

        poll_games(ctx, &state.event_game_ids).await?;
        Ok(())
    }
}

// mostly used for events/superstars
async fn poll_games(ctx: &WorkerContext, ids: &[String]) -> anyhow::Result<()> {
    // maybe should only poll if incomplete, but eh, there's not many going at once usually
    ctx.process_many(ids.to_vec(), 3, poll_game_by_id).await;

    Ok(())
}

async fn get_all_game_ids_from_days(
    ctx: &WorkerContext,
    season_filter: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    let mut season_map = HashMap::new();
    let seasons = ctx.db.get_all_latest(EntityKind::Season).await?;
    for season in seasons {
        let season_parsed: MmolbSeason = season.parse()?;
        for day in season_parsed.days {
            season_map.insert(day, season.entity_id.clone());
        }
    }

    let mut game_ids = Vec::new();
    let mut stream = ctx.db.get_all_latest_stream(EntityKind::Day);
    while let Some(v) = stream.try_next().await? {
        if season_filter.is_none()
            || season_map.get(&v.entity_id).map(|x| x.deref()) == season_filter
        {
            match v.parse::<MmolbDay>() {
                Ok(day) => {
                    game_ids.extend(day.games.into_iter().map(|g| g.game_id).flatten());
                }
                Err(e) => {
                    error!("error parsing day {}: {:?}", v.entity_id, e);
                }
            }
        }
    }

    Ok(game_ids)
}

async fn handle_season(ctx: &WorkerContext, season_id: String) -> anyhow::Result<()> {
    let season = ctx
        .fetch_and_save(
            format!("https://mmolb.com/api/season/{}", &season_id),
            EntityKind::Season,
            &season_id,
        )
        .await?;
    let season_parsed: MmolbSeason = season.parse()?;

    let mut season_day_ids = season_parsed.days;
    season_day_ids.extend(
        season_parsed.other_fields.into_iter()
            .flat_map(|(key, val)| {
                (key.starts_with("SuperstarDay") && key["SuperstarDay".len()..].parse::<i64>().is_ok())
                    .then(|| val.as_str().map(str::to_string))
                    .flatten()
            })
    );

    ctx.process_many_with_progress(
        season_day_ids,
        10,
        &format!("season {} days", season_parsed.season),
        handle_day,
    )
    .await;
    Ok(())
}

async fn handle_day(ctx: &WorkerContext, day_id: String) -> anyhow::Result<()> {
    ctx.fetch_and_save(
        format!("https://mmolb.com/api/day/{}", &day_id),
        EntityKind::Day,
        &day_id,
    )
    .await?;
    Ok(())
}

async fn fetch_game_if_not_known_completed(
    ctx: &WorkerContext,
    game_id: String,
) -> anyhow::Result<()> {
    let known_game = ctx.db.get_latest(EntityKind::Game, &game_id).await?;
    let should_poll = if let Some(game) = known_game {
        let game: MmolbGame = game.parse()?;
        game.state != "Complete"
    } else {
        true
    };

    if should_poll {
        poll_game_by_id(ctx, game_id).await?;
    }

    Ok(())
}

async fn poll_game_by_id(ctx: &WorkerContext, id: String) -> anyhow::Result<()> {
    let url = format!("https://mmolb.com/api/game/{}", id);
    let resp = ctx.fetch_and_save(url, EntityKind::Game, &id).await?;

    let game: MmolbGame = resp.parse()?;
    process_game_data(ctx, &id, &game).await?;
    info!("poll_game_by_id saved processed game {id}");

    Ok(())
}

// This is the only derived data that i deemed necessary to still include
async fn process_game_data(
    ctx: &WorkerContext,
    id: &str,
    game: &MmolbGame,
) -> anyhow::Result<()> {
    ctx.db
        .update_game(DbGameSaveModel {
            game_id: &id,
            season: game.season,
            day: game.day.to_int(),
            day_special: game.day.get_special(),
            home_team_id: &game.home_team_id,
            away_team_id: &game.away_team_id,
            state: &game.state,
            event_count: game.event_log.len() as i32,
            last_update: game.event_log.last(),
        })
        .await?;
    info!("process_game_data updated games table with game {id}");

    // Disabling game events and player stats processing because it was
    // slowing down my ingest to the point where it couldn't keep up with
    // live games --beiju
    Ok(())
}

async fn get_all_known_game_ids(ctx: &WorkerContext) -> anyhow::Result<HashSet<String>> {
    let preset_game_ids = include_str!("./game_ids.txt");

    let mut game_ids: HashSet<String> = preset_game_ids
        .split("\n")
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();

    game_ids.extend(ctx.db.get_all_entity_ids(EntityKind::Game).await?);
    game_ids.extend(get_all_game_ids_from_days(ctx, None).await?);
    Ok(game_ids)
}

pub async fn fetch_all_games(ctx: &WorkerContext) -> anyhow::Result<()> {
    let game_ids = get_all_known_game_ids(ctx).await?;
    ctx.process_many_with_progress(game_ids, 50, "fetch all games", poll_game_by_id)
        .await;
    Ok(())
}

pub async fn fetch_all_new_or_incomplete_games(ctx: &WorkerContext) -> anyhow::Result<()> {
    ctx.process_many_with_progress(
        get_known_incomplete_game_ids(ctx).await?,
        50,
        "fetch all new/incomplete games",
        fetch_game_if_not_known_completed,
    )
    .await;
    Ok(())
}

pub async fn fetch_all_seasons(ctx: &WorkerContext) -> anyhow::Result<()> {
    let mut season_ids: HashSet<String> = ctx
        .db
        .get_all_entity_ids(EntityKind::Season)
        .await?
        .into_iter()
        .collect();

    // we really don't wanna load up all game objects rn so do this the dumb way
    // TODO This is clearly meant to be updated manually but it hasn't been updated in like. a year
    season_ids.insert("6805db0fac48194de3cd42d1".to_string()); // season 0
    season_ids.insert("6846ba011b7a53d888cdef49".to_string()); // season 1
    season_ids.insert("6858e7be2d94a56ec8d460ea".to_string()); // season 2

    ctx.process_many(season_ids, 1, handle_season).await;

    Ok(())
}

pub async fn query_completed_game_ids(ctx: &WorkerContext) -> anyhow::Result<Vec<String>> {
    // lol inline sql
    Ok(
        sqlx::query_scalar("select game_id from games where state = 'Complete'")
            .fetch_all(&ctx.db.pool)
            .await?,
    )
}

async fn get_known_incomplete_game_ids(ctx: &WorkerContext) -> anyhow::Result<HashSet<String>> {
    let mut game_ids = get_all_known_game_ids(ctx).await?;

    let completed_games = query_completed_game_ids(ctx).await?;
    for completed_game in &completed_games {
        game_ids.remove(completed_game);
    }

    Ok(game_ids)
}
