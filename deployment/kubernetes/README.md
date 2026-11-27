# Kubernetes 单副本部署

默认清单面向 Docker Desktop 的 Linux 集群。命名空间为 `ayanami`，主应用固定一个 Pod、一个 Uvicorn worker，使用 `Recreate` 更新。静态前端与 FastAPI 同源服务。Redis 的数据可重建，工作区、Cron、Milvus、etcd 和 MinIO 分别使用持久卷。

## Docker Desktop

集群启动并有默认 StorageClass 后，从项目根目录运行以下 PowerShell 指令。镜像标签应与清单一致。Docker Desktop 的 kubeadm 模式可使用本机 Docker 镜像。使用与节点隔离的镜像仓库时，改为可拉取的仓库地址，并更新清单中的标签。

```powershell
kubectl --context docker-desktop get nodes
docker build -f deployment/kubernetes/Dockerfile -t ayanami-agent:k8s-local .
docker build -f deployment/cron/Dockerfile --build-arg PYTHON_BASE_IMAGE=mirror.gcr.io/library/python:3.13-slim -t ayanami-cron:k8s-local .
docker build --build-arg PYTHON_BASE_IMAGE=mirror.gcr.io/library/python:3.13-slim -t ayanami-pdf:k8s-local deployment/pdf-layout
.\.venv\Scripts\python.exe deployment/kubernetes/bootstrap.py --context docker-desktop
kubectl --context docker-desktop apply -k deployment/kubernetes
kubectl --context docker-desktop -n ayanami get pods,pvc
kubectl --context docker-desktop -n ayanami rollout status deployment/agent --timeout=180s
```

初始化脚本从本机 `.env` 读取模型配置的白名单字段，并在指定集群中创建 Secret。Secrets 已存在时会复用原值。修改模型设置后，单独更新 `agent-settings` 并重启主 Pod。首次登录用用户名 `owner`。在自己的终端读取密码：

```powershell
$encoded = kubectl --context docker-desktop -n ayanami get secret agent-settings -o 'jsonpath={.data.HARNESS_LOGIN_PASSWORD}'
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($encoded))
kubectl --context docker-desktop -n ayanami port-forward --address=127.0.0.1 svc/agent 18000:8000
```

保持端口转发运行，打开 [本地工作台](http://127.0.0.1:18000/zh/)。登录保护覆盖网页、API 和 SSE。部署版只注册资料库检索、任务、Cron 等受限工具。Shell、任意文件操作、队友线程及动态 MCP 仍可在本机开发模式使用。

## 数据与发布

| 数据 | 工作负载和路径 |
| --- | --- |
| 会话 SQLite、原件、任务、索引元数据 | `agent-workspace` PVC 中的 `/workspace/.runtime/agent-harness` |
| 项目记忆 | 同一 PVC 的 `/workspace/.memory` |
| Cron 计划与待派发记录 | `cron-state` PVC 中的 `/state/cron.sqlite3` |
| 向量、元数据、对象存储 | `milvus-data`、`etcd-data`、`minio-data` PVC |
| 本地 Trace JSONL | `agent-workspace` PVC 中的 `/workspace/.runtime/agent-harness/traces` |
| Jaeger Trace | `jaeger-data` PVC 中的 Badger 数据，Span 保留 72 小时 |

首次部署从空工作区开始。迁移已有数据时，先停写并备份 SQLite 和文件；Cron 数据需要保留旧 scope 或先做 scope 迁移。切换前确认只有一个应用实例会派发同一批计划。重新构建同标签的本地镜像后，可用 `kubectl --context docker-desktop -n ayanami rollout restart deployment/agent` 更新主 Pod。正式发布应使用不可变标签和支持快照的存储。节点上强制删除 Pod 前应确认原进程已停止。

公开部署需要另外配置域名与 HTTPS Gateway，将三组自建镜像推入目标集群可访问的私有仓库，使用支持 NetworkPolicy 的 CNI，并在初始化 Secret 时设置 `--public-origin https://实际域名`。Gateway 保留原始 Host，为 SSE 关闭响应缓冲，超时应覆盖长连接。当前清单只创建内部 Service，外部访问默认走本机端口转发。

## 查询 Agent Trace

应用以 OTLP/HTTP 向 OpenTelemetry Collector 异步发送 Span。Collector 批量转发到 Jaeger。Jaeger 使用独立持久卷，查询 UI 通过本机转发访问。

```powershell
kubectl --context docker-desktop -n ayanami get deployment agent otel-collector jaeger
kubectl --context docker-desktop -n ayanami port-forward --address=127.0.0.1 svc/jaeger 16686:16686
```

打开 [Jaeger UI](http://127.0.0.1:16686)，在 Service 中选择 `agent-harness`，或用运行事件中的 `trace_id` 打开 `/trace/<trace_id>`。
