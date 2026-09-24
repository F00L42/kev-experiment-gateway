# Kev Experiment Gateway

独立的实验数据采集网关：HTTP 透传、完整请求/响应记录、按日期分桶、离线合并与 SystemOne 解析。
只通过 HTTP 连接模型服务；无需安装 vLLM、vLLM-Ascend、PyTorch 或 NPU 驱动。

```text
TypeSafe SDK / curl → gateway → vLLM-Ascend /v1/systemone
                         └── 副本 → 日期 JSONL → 合并 / 离线解析
```

## 安装与启动

Python 3.11+。在仓库根目录执行：

```bash
uv sync --locked
uv run kev-gateway serve --config examples/gateway.toml
```

没有 uv 时：

```bash
python -m pip install .
python -m kev_gateway serve --config examples/gateway.toml
```

默认监听 `127.0.0.1:8080`，示例上游是 `http://127.0.0.1:55733`。部署时替换为实际地址：

```bash
uv run kev-gateway serve --config examples/gateway.toml \
  --upstream http://NPU_SERVER:55733 --host 0.0.0.0 --port 8080 \
  --output /data/kev-captures
```

上游配置只填写 HTTP(S) origin，不带 `/v1`、query 或认证信息。客户端请求的完整路径原样附到该 origin；
gateway 不增加认证策略，原有 Authorization 转发给上游。配置文件相对路径基于 TOML 所在目录，
命令行 `--output` 相对当前工作目录。启动时打印 `run_id` 和数据目录。

`read_timeout` 默认省略，表示不设置响应读取截止时间；连接超时默认 10 秒，写入和连接池等待超时默认 30 秒。
可用 `--read-timeout 300` 设置读取无进展的超时；它不是父请求总 deadline。
配置变更需重启，每次启动生成新的运行 ID。

## curl 与 SDK 接入

现有客户端只需修改 base URL。SystemOne 路径仍为 `/v1/systemone`。

```bash
curl -i http://127.0.0.1:8080/v1/systemone \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer local' \
  --data-binary @examples/request.json
```

Windows PowerShell 使用 `curl.exe`，可将命令写在一行。示例请求包含 Choice、Noul、Score 三类问题，
输入为合成案例，不预设模型应返回的答案。

可选 SDK 示例（SDK 只属于客户端，不是 gateway 依赖）：

```bash
uv run --with typesafe-sdk python examples/sdk_request.py --base-url http://127.0.0.1:8080 --mode async
uv run --with typesafe-sdk python examples/sdk_request.py --base-url http://127.0.0.1:8080 --mode sync
```

请求方式参照 [discussion #18 的客户端评论](https://github.com/singzhou/vllm-ascend/discussions/18#discussioncomment-18576761)：
使用 `extra_body` 传递 `cache_salt` 等字段，通过 `raw_http_response.json()` 读取实际响应，客户端关闭自动重试。
原评论的 `kev_prefix_cache_probe.py` 也可仅替换 `--base-url` 后继续使用。
将其 `--metrics-url` 指向 gateway 的 `/metrics` 可以转发指标请求；采集器不会解释或推断缓存命中。

## 运行信息与实验数据

```text
<output_root>/
├── runtime/
│   └── <run_id>/
│       ├── run.json          # 配置、版本、启停时间、采集计数
│       └── service.log       # gateway 的运行与采集错误日志
├── exchanges/
│   ├── 2026-09-24/
│   │   ├── <run_id_A>.jsonl
│   │   └── <run_id_B>.jsonl
│   └── 2026-09-25/
│       └── <run_id_A>.jsonl
└── exports/                  # 按需创建的导出目录
    └── <export_name>/
        ├── exchanges.jsonl
        └── merge.json
```

- `run_id` 是一次进程启动的 UUID。每个进程独占自己的分片，可共享数据根目录，须使用相同的分桶时区。
- 默认使用 `Asia/Shanghai` 按**请求到达日期**分桶，记录内 `started_at` 为 UTC 时间。
  23:59 到达、次日 00:02 完成的请求与响应仍一起进入前一天的桶。
- 完成时追加一整行，请求文件内是完成顺序；合并时再按开始时间排序。
- 每条记录只以 `run_id` 关联运行信息，不重复存放上游部署或配置。脱离运行目录仍能查看请求与响应。
- 数据根目录自动生成 `.gitignore`，实验正文不进入代码仓库。

一行对应一次 HTTP 交换，schema version 1：

| 字段 | 含义 |
| --- | --- |
| `id` / `run_id` | 采集 ID / 进程运行 ID；不改写客户端请求 ID |
| `started_at` / `duration_ms` | gateway 收到请求的 UTC 时间 / gateway 观察到的生命周期耗时 |
| `request` | method、原编码 path、query、headers 和 body |
| `response` | status、headers 和 body；尚未收到响应时为 null |
| `response_source` | upstream 或 gateway；区分上游错误响应与代理产生的错误 |
| `outcome` / `error` | completed、client_disconnected、upstream_error、gateway_error、cancelled；错误类型与阶段 |
| `capture_complete` | 是否完整保存两边正文与元信息；不表示业务成功或客户端应用已消费响应 |
| `capture_mode` / `capture_reason` | full 或 metadata；不完整/只存元信息的原因 |

请求与响应的 body 包含这些配套字段：

- `body`：原始正文；有效 UTF-8 保存为文本字符串，压缩或其他二进制保存为 base64。
- `body_encoding`：utf8 / base64；metadata 模式为 null。
- `body_bytes`：观察到的字节数，可能大于保存量；不是 token 数。
- `body_complete`：收到 HTTP 正文结束且该方向字节全部保存。

这保留了原始 JSON 的空格、字段顺序和数值文本；转发路径不解析、重写 JSON。
headers 使用列表保留重复项；认证、Cookie、API key 及常见凭据 query 只在保存副本中脱敏，转发值不变。
响应 headers 记录上游原值（脱敏后）；逐跳 headers 在转发时去除。HTTP 分块边界、trailer 不作为采集数据保存。

默认完整采集 `/v1/systemone`；其他路径正常转发，只存元信息和观察到的字节数。设置
`capture_paths = ["*"]` 可以采集全部 HTTP 路径。多题答案、usage、服务端 latency_ms 保留在响应正文中，
不另复制一份派生统计。上游 4xx/5xx 也属于完成的 HTTP 交换，状态码照常保留。

## 合并日期分片

```bash
uv run kev-gateway merge captures \
  --from 2026-09-24 --to 2026-09-25 \
  --output captures/exports/sep24-25
```

`--from` / `--to` 按桶日期过滤，**两端都包含**。省略则读取全部日期。
可重复 `--run-id <UUID>` 筛选运行。导出目录必须不存在，源文件保持不变。

- 输出仍为同一种 JSONL 结构，按 `started_at + id` 排序；使用临时 SQLite 索引，避免把全部正文装进内存。
- 相同请求正文、客户端请求 ID、cache_salt 都保留。只对同一个采集 ID 的相同记录去重；同 ID 内容冲突直接报错。
- 汇总前固定各输入文件的字节边界，只读取该范围内以换行结束的完整行。
  运行中请求、后续追加行、未完成尾行不在本次快照内；跳过尾行字节数明确写入 `merge.json`。
- 坏的完整行明确报告文件和行号，合并失败不会发布成功标记。`merge.json` 最后写入，表示此次导出完成。
- `merge.json` 记录来源、字节边界、哈希、数量、运行元信息状态。导出始终是 snapshot，
  正常关闭只描述进程生命周期，不保证某日期或某次实验的采集完整性。

## 查看和解析

```bash
uv run kev-gateway inspect captures --id <采集ID>
uv run kev-gateway inspect captures/exports/sep24-25/exchanges.jsonl --id <采集ID>
uv run kev-gateway inspect captures --id <采集ID> --raw
```

默认离线解析完整的 UTF-8/base64 JSON 及 gzip/deflate 正文，输出原有 Noul/Choice/Score 答案、
缺失/额外 question 名称、usage、服务端 latency_ms。正文不完整或不可解析时明确显示状态，缺失值保留 null。
`--raw` 显示已保存原记录，适用于坏 JSON、非 JSON 错误或部分正文。解压后的读取上限可用
`--max-decoded-bytes` 调整（默认 16 MiB）。

`duration_ms` 不是纯模型耗时；多题请求只具有整包 HTTP 耗时，不能推导逐题完成时间。
usage 是上游提供的逻辑口径，不是本次重算 token 数；采集器不据时延、salt 或 usage 宣称缓存命中。

## 转发、取消与采集边界

共享异步连接池原样转发原编码路径、重复 query/header 和原始正文。仅处理代理必需的 Host 与逐跳 headers；
不重试、不跟随重定向、不替换 model、不修改请求字段、不跨请求积累上游 Cookie。
WebSocket、CONNECT 和协议升级不支持。

断连时取消上游工作并关闭连接；停机等待活跃请求最多 30 秒，然后取消，等待请求清理，再排空写盘队列。
尚未发送响应时，上游连接错误返回 gateway 502，网络超时返回 504；已经开始的响应若断流就中止连接，
保留部分采集，不补造正常结束。正文采集只证明 gateway 观察到了这些字节，不证明客户端应用已接收。
上传使用有界接收缓冲；完整请求到达后，即使上游仍在等待连接，也能检测后续断连。
未完成的大上传在背压期间，需要随缓冲推进才能读取后续断连事件，连接/连接池/写入超时仍生效。
保存的请求字节表示网关已从客户端收到，不证明上游已经全部消费。

采集正文默认每方向最多 16 MiB；活跃、待写及正在写入的记录共用默认 64 MiB 的原始字节/元信息预算，
另有 4096 个待完成/待写记录上限。Python 对象和单个 writer 的 JSON/base64 序列化临时内存另计，
这不是进程 RSS 限额。达到采集上限时继续转发，停止该请求的正文采集并标记缺口；
连元信息也无法接纳时，只在运行信息中增加丢失计数。

写盘失败记录服务错误并停止接纳后续采集，转发继续。运行计数约每秒更新；正常关闭排空队列并同步文件。
强杀或断电可能留下完整前缀、半行或尚未写入的请求，不能宣称完整归档。

## 开发与验证

```bash
uv sync --locked --group dev
uv run ruff check .
uv run ruff format --check .
uv run pytest -q --basetemp .pytest-manual-test --tb=short
```

`--basetemp` 是 pytest 专用的临时目录，请勿指向业务数据。GitHub Actions 配置了 Windows/Linux、
Python 3.11/3.14 检查。本地测试使用 mock 和真实回环 HTTP 服务，覆盖透传、并发、取消、压缩、错误、
采集上限、写盘失败、跨午夜分桶和合并。真实 Kev/NPU 服务、SDK 与目标环境的组合验收为 `pending_remote`；
本仓库不提供模型准确率、缓存命中率或推理性能结论。

## 模块与参考

| 模块 | 职责 |
| --- | --- |
| `config.py` / `cli.py` | 显式配置、serve/merge/inspect 命令 |
| `transport.py` | HTTP 生命周期、连接与取消 |
| `capture.py` | 请求关联、正文缓冲、日期分片、独立运行信息 |
| `merge.py` | 固定输入快照、排序、去重和导出 |
| `inspect.py` | 离线解码与 SystemOne 答案展示 |

转发与采集分离的结构参考 TB 项目的 `vllm_gateway`，精简了 token 统计、轨迹重建和报告层。
请求/响应字段参考 [vLLM-Ascend SystemOne 源码（80d40e703）](https://github.com/singzhou/vllm-ascend/blob/80d40e7030edf948eddc41db0e98f0ab1f5fb77a/vllm_ascend/entrypoints/systemone/protocol.py)
及 [discussion #18](https://github.com/singzhou/vllm-ascend/discussions/18)。HTTPX 原始流转发和连接关闭遵循
[官方异步文档](https://www.python-httpx.org/async/)。
