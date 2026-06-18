# Hướng dẫn run project 
  1. Clone repo

  git clone git@github.com:thuongvn2410/bigdata.git
  cd bigdata

  Nếu máy đó chưa có SSH GitHub thì dùng HTTPS:

  git clone https://github.com/thuongvn2410/bigdata.git
  cd bigdata

  2. Cài yêu cầu
  Cần có:

  - Docker
  - Docker Compose plugin

  Kiểm tra:

  docker --version
  docker compose version

  3. Start toàn bộ hệ thống

  docker compose up -d --build

  Lần đầu sẽ hơi lâu vì build image Spark/API và tải image Kafka/Postgres/Grafana.

  4. Kiểm tra service

  docker compose ps

  Các service chính nên Up:

  - mouse-frontend
  - mouse-tracking-api
  - mouse-postgres
  - mouse-kafka
  - mouse-spark-streaming
  - mouse-grafana

  5. Mở các màn hình
  Tracking page:

  http://localhost:8088/

  Heatmap:

  http://localhost:8088/heatmap.html

  Grafana:

  http://localhost:3000

  Login Grafana:

  admin / admin

  Dashboard:

  http://localhost:3000/d/mouse-tracking-overview/mouse-tracking-overview
  http://localhost:3000/d/mouse-tracking-system-healthcheck/mouse-tracking-system-healthcheck

  6. Test heatmap
  Vào http://localhost:8088/, click, move mouse, scroll vài lần.

  Đợi khoảng 5-10 giây cho Spark ghi dữ liệu vào Postgres, rồi mở:

  http://localhost:8088/heatmap.html

  Nếu chưa thấy dữ liệu, bấm Refresh.

  7. Reset data nếu muốn test lại
  Trên heatmap page bấm Reset dữ liệu.

  Hoặc gọi API:

  curl -X POST http://localhost:8000/reset

  Lưu ý
  Thư mục data/ không có trong Git. Khi chạy docker compose up, Docker sẽ tự tạo dữ liệu runtime mới cho Kafka/Postgres/Spark checkpoint trên máy đó.
