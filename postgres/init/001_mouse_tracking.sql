CREATE TABLE IF NOT EXISTS event_raw (
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    session_id TEXT NOT NULL,
    user_id TEXT,
    page_url TEXT NOT NULL,
    event_type TEXT NOT NULL,
    x INTEGER,
    y INTEGER,
    scroll_y INTEGER,
    viewport_width INTEGER,
    viewport_height INTEGER,
    event_timestamp TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_value TEXT NOT NULL,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_event_raw_event_timestamp
    ON event_raw (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_event_raw_page_event_type
    ON event_raw (page_url, event_type);

CREATE INDEX IF NOT EXISTS idx_event_raw_session_id
    ON event_raw (session_id);

CREATE TABLE IF NOT EXISTS mouse_heatmap (
    page_url TEXT NOT NULL,
    grid_x INTEGER NOT NULL,
    grid_y INTEGER NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (page_url, grid_x, grid_y)
);

CREATE INDEX IF NOT EXISTS idx_mouse_heatmap_page_count
    ON mouse_heatmap (page_url, event_count DESC);
