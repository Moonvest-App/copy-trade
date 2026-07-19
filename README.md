# Moonvest

这是一个只在本机运行的 macOS 跟单 App。唯一开单信息源是 Moonvest Active Trades SSE；券商侧支持 moomoo OpenD、Robinhood 官方 Trading MCP、IBKR、Webull 与 Charles Schwab。

## Moonvest SSE

固定端点：

```text
GET https://stream.moonvest.app/v1/active_trades/subscribe?follow=<username>
Authorization: Bearer <api-key>
Accept: text/event-stream
```

- 明确配置 1–2 个 `follow` 用户，不读取或推断社交关注关系。
- API key 优先读取 `MOONVEST_API_KEY` 环境变量；界面保存时只写入 macOS 登录钥匙串。
- API key 不进入 `settings.json`、SQLite、日志、App 或分享包。
- cursor 在每个事件完整处理后写入本机 SQLite。
- 默认以 `Last-Event-ID` 恢复，也可切换为 `since=<id>`。
- 可在连接页粘贴一个已知 cursor 立即回放；follow 用户名会规范为 Moonvest 要求的小写，避免大小写不匹配造成静默空流。
- 本机信号表对 `source + event id` 建立唯一约束；重投事件不会再次执行。
- 在券商接口支持自定义订单标识时，订单 remark 使用事件 id 的稳定摘要；本机数据库始终用事件 id 保证持久幂等。
- `: keepalive` 注释会被忽略。
- 断线会关闭执行开关，随后按 1–30 秒退避自动重连；cursor 会补齐断线缺口。
- 收到 `event: resync` 时会清空失效 cursor、把已知来源持仓标为待同步、重新读取当前券商持仓快照、关闭执行，然后继续接收实时流。

## 事件动作

| Moonvest action | 本地动作 | 行为 |
| --- | --- | --- |
| `opened` | `OPEN` | 按事件 `qty` 与统一跟单比例形成开仓计划 |
| `added_to` | `ADD` | 按 `qty_added` 跟随；`entry_price` 是混合成本，不会误当成新增成交价 |
| `partially_closed` | `TRIM` | 按 `qty_closed` 与统一比例缩减本 App 管理的仓位 |
| `closed` | `CLOSE` | 退出该用户、合约和账户下由本 App 管理的剩余数量 |
| `edited` | `EDIT` | 完整留痕并更新来源状态，不产生券商订单 |
| `expired` | `EXPIRE` | 完整留痕并更新来源状态，不产生券商订单 |

股票与单腿期权可以进入 moomoo 执行层；Robinhood 当前只执行美股与 ETF。`vertical` 组合的两条 leg 会完整保存并展示，但当前券商执行层没有原子组合单能力，因此只记录，不拆成可能失真的两张独立订单。

事件中的 `subscriber_only`、`note`、`changes`、`entry_price`、`exit_price`、`realized_pnl`、到期日和 legs 都保存在原始事件 JSON 中。App 不尝试绕过 Moonvest 的订阅者权限；服务端未投递的事件在本机不可见。

## 安全设计

- macOS 红色关闭按钮只把主窗口最小化到程序坞，SSE 与跟单引擎继续在后台运行；只有明确执行“退出 Moonvest”才停止后端。
- macOS 屏幕顶栏提供常驻状态监控，可查看 SSE、券商连接、订单执行和最近事件，并可重新打开主窗口或明确退出。
- 默认配置是 `SIMULATE + observe`，即模拟盘 + 仅观察。
- 订单执行开关只保存在内存；重启、暂停、保存设置、SSE 断线、resync 或未知下单异常都会关闭。券商明确拒绝某一笔订单时只拒绝该笔，后续订单继续运行。
- 执行开关启用 4 小时后自动过期。
- 实盘 moomoo 必须由用户在 OpenD GUI 中手动解锁；项目不调用 `unlock_trade`。
- Robinhood 只通过官方 OAuth 2.1 + PKCE 授权，密码和 2FA 不经过 Moonvest；token 只保存在 macOS 登录钥匙串。
- Robinhood 下单固定先调用官方 `review_equity_order`，成功后才允许调用 `place_equity_order`。
- 默认禁止超出当前可卖数量的卖单；建立空头必须显式开启相应选项。
- 每笔事件仍需通过允许市场/代码、统一跟单比例、滑点、单笔金额、单日金额和期权到期护栏。
- 所有网络状态、游标控制、风控结果、确认动作和订单结果进入本机审计日志。
- 本地页面只绑定 `127.0.0.1`，写请求必须携带应用专用请求头。

## 券商适配器

| 券商 | 端点 | 当前能力 |
| --- | --- | --- |
| moomoo OpenD | 本机 `127.0.0.1:11111` | 账户、持仓、订单、行情、下单 |
| Robinhood | 官方 Trading MCP / OAuth | 全账户只读；独立 Agentic 账户可执行美股与 ETF |
| IBKR | 本机 Client Portal Gateway | 账户、持仓、订单、美股快照；只读 |
| Webull | 官方 HTTPS | 签名认证、账户、余额、持仓、订单；只读 |
| Charles Schwab | 官方 HTTPS / OAuth | 账户、持仓、订单、美股快照；只读 |

moomoo OpenD 与 Robinhood Agentic 账户开放订单执行。Robinhood 普通账户及其他适配器会明确保持只读，引擎不会绕过能力锁。

### Robinhood 连接

在“连接与风控 → 其他券商”选择 Robinhood，点击“连接 Robinhood”。App 会在本机动态注册 OAuth 客户端并打开 Robinhood 官方授权页；完成授权后自动读取官方 MCP 工具列表并发现 Agentic 账户。若只有一个可执行账户，App 会自动选中并保存。

## 期权保护

- 默认在最后常规交易时点前 60 分钟禁止 `OPEN / ADD`，平仓仍允许。
- SPX 与 SPXW 保持不同 root、结算时段和到期保护。
- moomoo 使用 OpenD 合约代码，嘉信使用 OCC；不明确的 IBKR 期权映射会被拒绝。
- SPX/SPXW 单腿限价必须符合对应最小变动单位，不会静默改价。

## 源码运行

默认使用 moomoo 时，先启动 OpenD 并监听 `127.0.0.1:11111`，然后：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 app.py
```

也可以双击 `run.command`。若端口 `8899` 被占用，App 会自动尝试后续本机端口。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖 SSE 帧解析、keepalive、六种 action、cursor 两种传递方式、持久去重、单腿/vertical 映射、resync、钥匙串边界、Robinhood Streamable HTTP、Agentic 账户筛选、预检/下单顺序、执行风控与下游幂等标识。

## 构建 macOS App

```bash
./scripts/build_macos_app.sh
open "dist/Moonvest.app"
```

产物是自包含的 ARM64 Cocoa/WebKit App，内含 Python 服务、`pandas` 与 `moomoo-api`。构建会进行本地 ad-hoc 签名。数据保存在：

```text
~/Library/Application Support/Moonvest/
```

## 生成分享包

```bash
./scripts/build_share_release.sh
```

脚本在 `release/` 生成 DMG、ZIP 和 SHA-256 校验文件，并在打包前拒绝任何设置、凭证或 SQLite 数据进入产物。

## 当前边界

- 可执行层覆盖 moomoo OpenD 股票和单腿期权，以及 Robinhood Agentic 美股/ETF；Robinhood 期权和 vertical 组合仅完整记录。
- `resync` 时给定的 SSE 合约没有来源侧快照端点，因此 App 会把旧来源状态标记为待同步，并以券商持仓快照作为执行安全基线；用户核对前执行保持关闭。
- Moonvest 首次无 cursor 连接只接收连接后的事件，不回放历史。
