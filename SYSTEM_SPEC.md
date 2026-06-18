# Mouse Tracking System — Đặc tả hệ thống

---

## 1. Tổng quan

Hệ thống thu thập và phân tích hành vi chuột (mouse tracking) trên website theo thời gian thực.  
Dữ liệu đi qua pipeline: **Frontend → API → Kafka → Spark Streaming → PostgreSQL → Grafana**.

**Mục tiêu:**
- Ghi nhận mọi sự kiện chuột (click, di chuột, scroll) từ người dùng trực tiếp trên trình duyệt.
- Xử lý luồng sự kiện liên tục bằng Spark Structured Streaming.
- Lưu trữ dữ liệu thô và dữ liệu heatmap đã tổng hợp vào PostgreSQL.
- Trực quan hóa qua Grafana dashboard và trang heatmap overlay tùy chỉnh.

---

## 2. Kiến trúc tổng thể

```
┌─────────────────────────────────────────────────────────────────┐
│                         NGƯỜI DÙNG                              │
│                    (Trình duyệt web)                            │
└────────────────────────┬────────────────────────────────────────┘
                         │  HTTP Events (click / mousemove / scroll)
                         ▼
┌────────────────────────────────────────┐
│          Frontend (Nginx)              │  port 8088
│  index.html  ·  heatmap.html          │
└────────────┬────────────┬─────────────┘
             │ POST /track │ GET /heatmap, /pages, /reset
             ▼             ▼
┌────────────────────────────────────────┐
│         Tracking API (FastAPI)         │  port 8000
│  - Nhận sự kiện → produce Kafka       │
│  - Đọc PostgreSQL → trả heatmap data  │
└────────────────┬───────────────────────┘
                 │  Kafka Producer
                 ▼
┌────────────────────────────────────────┐
│         Apache Kafka                   │  port 9092 (external)
│  Topic: mouse-events (3 partitions)   │  port 29092 (internal)
│  Zookeeper: port 2181                 │
└────────────────┬───────────────────────┘
                 │  Kafka Consumer (Structured Streaming)
                 ▼
┌────────────────────────────────────────┐
│      Spark Streaming (PySpark)         │
│  - Parse JSON từ Kafka                 │
│  - Ghi event_raw  (ON CONFLICT IGNORE) │
│  - Tính heatmap delta → upsert         │
│  Trigger: mỗi 5 giây                  │
└────────────────┬───────────────────────┘
                 │  JDBC (PostgreSQL driver)
                 ▼
┌────────────────────────────────────────┐
│         PostgreSQL 16                  │  port 5432
│  DB: mousetracking                     │
│  Tables: event_raw, mouse_heatmap      │
└────────────────┬───────────────────────┘
                 │  SQL datasource
                 ▼
┌────────────────────────────────────────┐
│         Grafana                        │  port 3000
│  Dashboard: Mouse Tracking Overview    │
│  - Events by Type                      │
│  - Heatmap Buckets table               │
│  - Mouse Position Heatmap (ECharts)    │
│  Filter: variable page_url             │
└────────────────────────────────────────┘
```

---

## 3. Các thành phần (Components)

### 3.1 Frontend (Nginx)
| Thuộc tính | Giá trị |
|---|---|
| Image | `nginx:alpine` |
| Port | `8088:80` |
| Phục vụ | `frontend/index.html`, `frontend/heatmap.html` |

**index.html** — Trang demo tracking:
- Gửi sự kiện `click` và ~5% sự kiện `mousemove` lên API.
- Gửi kèm: `session_id`, `page_url`, `x`, `y`, `scroll_y`, `viewport_width`, `viewport_height`, `timestamp`.
- `session_id` được sinh bằng `crypto.randomUUID()`, lưu `localStorage`.

**heatmap.html** — Trang xem heatmap overlay:
- Dropdown chọn `page_url` và `event_type`.
- Render heatmap dùng **heatmap.js 2.0.2** (CDN cdnjs).
- Nút **Reset All Data** — xóa sạch toàn bộ dữ liệu DB.

---

### 3.2 Tracking API (FastAPI)
| Thuộc tính | Giá trị |
|---|---|
| Image | Build từ `python:3.11-slim` |
| Port | `8000:8000` |
| Dependencies | `fastapi`, `uvicorn`, `kafka-python`, `psycopg2-binary` |

**Endpoints:**

| Method | Path | Mô tả |
|---|---|---|
| GET | `/` | Health check |
| POST | `/track` | Nhận sự kiện, produce vào Kafka |
| GET | `/pages` | Danh sách `page_url` phân biệt từ `event_raw` |
| GET | `/heatmap?page_url=&event_type=all` | Tọa độ x,y thô + số lần xuất hiện, trả `{data:[{x,y,value}], max}` |
| POST | `/reset` | TRUNCATE `event_raw` và `mouse_heatmap` |

**CORS:** Cho phép tất cả origin (`*`).

---

### 3.3 Apache Kafka
| Thuộc tính | Giá trị |
|---|---|
| Image | `confluentinc/cp-kafka:7.6.0` |
| Topic | `mouse-events` |
| Partitions | 3 |
| Listener nội bộ | `kafka:29092` |
| Listener bên ngoài | `localhost:9092` |
| Zookeeper | `confluentinc/cp-zookeeper:7.6.0` |

Mỗi sự kiện là một JSON message được produce bởi Tracking API.

**Cấu trúc message:**
```json
{
  "session_id": "uuid",
  "user_id": null,
  "page_url": "/",
  "event_type": "click",
  "x": 450,
  "y": 320,
  "scroll_y": 0,
  "viewport_width": 1920,
  "viewport_height": 1080,
  "timestamp": "2026-06-16T10:00:00Z"
}
```

---

### 3.4 Spark Streaming (PySpark)
| Thuộc tính | Giá trị |
|---|---|
| Image | Custom build từ `apache/spark:3.5.1` |
| Scala version | 2.12 |
| Trigger interval | 5 giây |
| Checkpoint | `/opt/spark/checkpoints/mouse-streaming` |

**Custom Docker image** (`docker/spark/Dockerfile`):  
Pre-baked JARs — không cần internet khi chạy:
- `postgresql-42.7.4.jar`
- `spark-sql-kafka-0-10_2.12-3.5.1.jar`
- `spark-token-provider-kafka-0-10_2.12-3.5.1.jar`
- `kafka-clients-3.4.1.jar`
- `commons-pool2-2.11.1.jar`

**Hai streaming query chạy song song:**

**Query 1 — `event_raw_writer`:**
- Source: `parsed_events` (outputMode `append`)
- Mỗi micro-batch: dedup theo `(kafka_topic, partition, offset)` → ghi vào staging table `event_raw_stage` → INSERT ... ON CONFLICT DO NOTHING vào `event_raw`.

**Query 2 — `mouse_heatmap_writer`:**
- Source: `parsed_events` (outputMode `append`, luồng độc lập)
- Mỗi micro-batch: lọc x/y not null → tính bucket 100px (`grid_x = floor(x/100)*100`) → group by `(page_url, grid_x, grid_y)` → ghi staging → **UPSERT**: `event_count += delta` (không ghi đè toàn bộ).

**Upsert mechanism** (dùng py4j JDBC raw SQL):
```sql
-- event_raw
INSERT INTO event_raw ... FROM event_raw_stage
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING;

-- mouse_heatmap
INSERT INTO mouse_heatmap ...
ON CONFLICT (page_url, grid_x, grid_y)
DO UPDATE SET
  event_count = mouse_heatmap.event_count + EXCLUDED.event_count,
  updated_at  = NOW();
```

---

### 3.5 PostgreSQL 16
| Thuộc tính | Giá trị |
|---|---|
| Image | `postgres:16` |
| Port | `5432:5432` |
| Database | `mousetracking` |
| User/Pass | `postgres / postgres` |

**Schema:**

```sql
-- Dữ liệu thô mỗi sự kiện
CREATE TABLE event_raw (
    kafka_topic       TEXT NOT NULL,
    kafka_partition   INTEGER NOT NULL,
    kafka_offset      BIGINT NOT NULL,
    kafka_timestamp   TIMESTAMPTZ,
    session_id        TEXT NOT NULL,
    user_id           TEXT,
    page_url          TEXT NOT NULL,
    event_type        TEXT NOT NULL,
    x                 INTEGER,
    y                 INTEGER,
    scroll_y          INTEGER,
    viewport_width    INTEGER,
    viewport_height   INTEGER,
    event_timestamp   TIMESTAMPTZ,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_value         TEXT NOT NULL,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

-- Heatmap đã tổng hợp theo bucket 100x100px
CREATE TABLE mouse_heatmap (
    page_url    TEXT NOT NULL,
    grid_x      INTEGER NOT NULL,
    grid_y      INTEGER NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (page_url, grid_x, grid_y)
);
```

**Indexes:** `event_timestamp`, `(page_url, event_type)`, `session_id`, `(page_url, event_count DESC)`.

---

### 3.6 Grafana
| Thuộc tính | Giá trị |
|---|---|
| Image | `grafana/grafana:latest` |
| Port | `3000:3000` |
| Admin | `admin / admin` |
| Plugin | `volkovlabs-echarts-panel` |
| Datasource | PostgreSQL (UID: `postgres_mouse_tracking`) |

**Dashboard: Mouse Tracking Overview**  
Auto-refresh: 5 giây.

| Panel | Type | Query |
|---|---|---|
| Events by Type | Table | COUNT(*) GROUP BY event_type FROM event_raw |
| Mouse Heatmap Buckets | Table | Top 50 rows từ mouse_heatmap |
| Mouse Position Heatmap | ECharts | SUM(event_count) GROUP BY grid_x, grid_y |

**Template variable `page_url`:**
- Type: Query
- SQL: `SELECT DISTINCT page_url FROM mouse_heatmap ORDER BY page_url`
- Include All (value `*`) — tất cả panel đều filter theo `page_url`.

---

## 4. Luồng dữ liệu chi tiết

```
1. User tương tác trên http://localhost:8088
        ↓
2. JavaScript gửi POST /track (JSON event)
        ↓
3. FastAPI validate → KafkaProducer.send("mouse-events")
        ↓
4. Spark đọc Kafka mỗi 5 giây (startingOffsets=earliest)
        ↓
5. Parse JSON, bổ sung kafka metadata
        ↓
   ┌─── Query 1 ──────────────────────────────┐
   │  Dedup → staging → INSERT ON CONFLICT    │
   │  DO NOTHING vào event_raw               │
   └──────────────────────────────────────────┘
        ↓ (song song)
   ┌─── Query 2 ──────────────────────────────┐
   │  Filter x/y → bucket 100px              │
   │  groupBy(page_url, grid_x, grid_y)      │
   │  → staging → UPSERT event_count += Δ   │
   │  vào mouse_heatmap                      │
   └──────────────────────────────────────────┘
        ↓
6. Grafana poll PostgreSQL mỗi 5 giây → hiển thị
7. http://localhost:8088/heatmap.html
        → GET /pages → dropdown page_url
        → GET /heatmap?page_url=... → heatmap.js render
```

---

## 5. Cấu trúc thư mục

```
bigdata/
├── docker-compose.yml          # Toàn bộ stack (7 services)
├── docker/
│   └── spark/
│       └── Dockerfile          # Custom Spark image, JARs pre-baked
├── frontend/
│   ├── index.html              # Trang demo thu thập sự kiện
│   └── heatmap.html            # Trang xem heatmap overlay
├── tracking-api/
│   ├── app.py                  # FastAPI: /track /heatmap /pages /reset
│   ├── Dockerfile
│   └── requirements.txt
├── spark-jobs/
│   └── spark_mouse_streaming.py  # PySpark Structured Streaming job
├── postgres/
│   └── init/
│       └── 001_mouse_tracking.sql  # Schema khởi tạo
└── grafana/
    ├── dashboards/
    │   └── mouse-tracking-overview.json
    └── provisioning/
        ├── dashboards/mouse-tracking.yml
        ├── datasources/postgres.yml
        └── plugins/volkovlabs-echarts.yml
```

---

## 6. Cách khởi động

```bash
# Lần đầu hoặc sau khi reset Kafka/ZooKeeper data:
docker compose up -d --build

# Kiểm tra trạng thái:
docker ps

# Truy cập:
# Tracking page : http://localhost:8088
# Heatmap view  : http://localhost:8088/heatmap.html
# Grafana       : http://localhost:3000  (admin/admin)
# Tracking API  : http://localhost:8000
```

**Lưu ý:** Nếu Kafka báo `InconsistentClusterIdException`, xóa runtime data cũ:
```bash
docker compose down
rm -rf data/runtime/kafka/* data/runtime/zookeeper/*
docker compose up -d
```

---

## 7. Công nghệ sử dụng

| Layer | Công nghệ | Version |
|---|---|---|
| Message Queue | Apache Kafka (Confluent) | 7.6.0 |
| Stream Processing | Apache Spark Structured Streaming | 3.5.1 |
| Database | PostgreSQL | 16 |
| API | FastAPI + Uvicorn | Python 3.11 |
| Frontend | HTML5 + heatmap.js | 2.0.2 |
| Visualization | Grafana + ECharts panel | Latest |
| Container | Docker Compose | v2 |
| Web Server | Nginx Alpine | Latest |

---

## 8. Các điểm kỹ thuật nổi bật

| Vấn đề | Giải pháp |
|---|---|
| Kafka restart làm offset về 0, Spark replay lại data cũ | `ON CONFLICT DO NOTHING` trên PK của event_raw — dữ liệu không bị trùng lặp |
| Heatmap cộng dồn từ nhiều batch | UPSERT `event_count += EXCLUDED.event_count` — không ghi đè |
| Spark cần tải JARs mỗi lần khởi động (cần internet) | Custom Docker image với JARs pre-baked trong `$SPARK_HOME/jars/` |
| Grafana cần lọc theo từng trang | Template variable `page_url` với `Include All` — filter toàn bộ panel |
| Heatmap overlay chồng lên đúng trang thật | `<iframe>` preview + heatmap.js layer tuyệt đối bên trên |
