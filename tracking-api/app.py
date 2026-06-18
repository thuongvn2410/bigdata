from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import uuid
import os
import psycopg2
import psycopg2.extras

app = FastAPI(title="Mouse Tracking API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

TOPIC = os.getenv("KAFKA_TOPIC", "mouse-events")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@postgres:5432/mousetracking")


def get_pg():
    return psycopg2.connect(POSTGRES_URL)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all",
    linger_ms=10,
    request_timeout_ms=15000,
)


class MouseEvent(BaseModel):
    session_id: str | None = None
    user_id: str | None = None
    page_url: str
    event_type: str
    x: int | None = None
    y: int | None = None
    scroll_y: int | None = None
    viewport_width: int | None = None
    viewport_height: int | None = None
    timestamp: float | str | None = None


@app.get("/")
def health_check():
    return {
        "status": "ok",
        "service": "tracking-api",
        "kafka": KAFKA_BOOTSTRAP
    }


@app.post("/track")
def track_event(event: MouseEvent):
    data = event.model_dump()

    if data["session_id"] is None:
        data["session_id"] = str(uuid.uuid4())

    if data["timestamp"] is None:
        data["timestamp"] = time.time()

    try:
        future = producer.send(TOPIC, data)
        result = future.get(timeout=10)

        return {
            "status": "sent",
            "topic": TOPIC,
            "partition": result.partition,
            "offset": result.offset,
            "event": data
        }

    except KafkaError as e:
        raise HTTPException(status_code=503, detail=f"Kafka send failed: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/pages")
def get_pages():
    try:
        with get_pg() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT page_url FROM event_raw ORDER BY page_url"
                )
                rows = cur.fetchall()
        return {"pages": [r[0] for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


SCROLL_Y_SQL = (
    "SELECT (ROUND(scroll_y / 50.0) * 50)::int AS y, "
    "COALESCE(MAX(viewport_width), 1200)::int AS vw, COUNT(*) AS value "
    "FROM event_raw "
    "WHERE page_url = %s AND event_type = 'scroll' AND scroll_y IS NOT NULL "
    "GROUP BY (ROUND(scroll_y / 50.0) * 50)::int "
    "ORDER BY value DESC LIMIT 1000"
)


def _scroll_points(rows):
    data = []
    for y, vw, value in rows:
        width = max(int(vw or 1200), 1)
        for ratio in (0.2, 0.4, 0.6, 0.8):
            data.append({"x": round(width * ratio), "y": y, "value": value,
                         "event_type": "scroll", "vw": width})
    return data


@app.get("/heatmap")
def get_heatmap(
    page_url: str = Query("/"),
    event_type: str = Query("all"),
):
    try:
        with get_pg() as conn:
            with conn.cursor() as cur:
                if event_type == "scroll":
                    cur.execute(SCROLL_Y_SQL, (page_url,))
                    data = _scroll_points(cur.fetchall())
                    max_val = max((d["value"] for d in data), default=0)
                    return {"data": data, "max": max_val}

                if event_type == "all":
                    cur.execute(
                        "SELECT x, y, COUNT(*) AS value, "
                        "COALESCE(MAX(viewport_width), 1200)::int AS vw "
                        "FROM event_raw "
                        "WHERE page_url = %s AND x IS NOT NULL AND y IS NOT NULL "
                        "GROUP BY x, y ORDER BY value DESC LIMIT 5000",
                        (page_url,),
                    )
                    cm_rows = cur.fetchall()
                    cm_max = max((r[2] for r in cm_rows), default=0)

                    cur.execute(SCROLL_Y_SQL, (page_url,))
                    sc_rows = cur.fetchall()
                    sc_max = max((r[2] for r in sc_rows), default=0)

                    # Normalise each type to 0-100 so neither drowns the other.
                    norm = 100
                    data = [
                        {"x": r[0], "y": r[1],
                         "value": round(r[2] / max(cm_max, 1) * norm),
                         "vw": int(r[3])}
                        for r in cm_rows
                    ]
                    for d in _scroll_points(sc_rows):
                        d["value"] = round(d["value"] / max(sc_max, 1) * norm)
                        data.append(d)

                    return {"data": data, "max": norm}

                else:
                    cur.execute(
                        "SELECT x, y, COUNT(*) AS value, "
                        "COALESCE(MAX(viewport_width), 1200)::int AS vw "
                        "FROM event_raw "
                        "WHERE page_url = %s AND event_type = %s "
                        "AND x IS NOT NULL AND y IS NOT NULL "
                        "GROUP BY x, y ORDER BY value DESC LIMIT 5000",
                        (page_url, event_type),
                    )
                    rows = cur.fetchall()
                    data = [{"x": r[0], "y": r[1], "value": r[2], "vw": int(r[3])}
                            for r in rows]
                    max_val = max((d["value"] for d in data), default=0)
                    return {"data": data, "max": max_val}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/reset")
def reset_data():
    try:
        with get_pg() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE event_raw, mouse_heatmap")
            conn.commit()
        return {"status": "reset", "message": "event_raw and mouse_heatmap cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
