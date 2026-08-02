create table feed_events (
    event_id text not null,
    subject_type text not null,
    subject_id text not null,
    timestamp timestamptz not null,
    timestamp_str text not null, -- needed because mmolb timestamps are fragile
    request_start timestamptz not null,
    request_end timestamptz not null,
    data jsonb not null
);

create index if not exists idx_feed_events_by_timestamp_id
    ON feed_events (timestamp, event_id);

create index if not exists idx_feed_events_by_subject_timestamp
    ON feed_events (subject_type, subject_id, timestamp);

