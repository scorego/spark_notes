# Spark部署

## 1. *Local

## 2. All Cluster Manager

| From                         | To                | 默认端口 | 作用               | 配置                    | 说明                               |
| ---------------------------- | ----------------- | -------- | ------------------ | ----------------------- | ---------------------------------- |
| 浏览器                       | Application       | 4040     | Web UI             | spark.ui.port           | Jetty Based.                       |
| 浏览器                       | History Server    | 18080    | Web UI             | spark.history.ui.port   | Jetty Based.                       |
| Executor / Standalone Master | Driver            | 随机     |                    |                         | 设为`0`是随机选择一个端口          |
| Executor / Driver            | Executor / Driver | 随机     | Block Manager port | spark.blockManager.port | Raw socket via ServerSocketChannel |