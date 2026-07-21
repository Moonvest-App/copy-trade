const LOCAL_HEADERS = { "Content-Type": "application/json", "X-Local-App": "moonvest" };
const state = { dashboard: null, accounts: [], accountRequest: 0, activeView: "dashboard", timer: null, robinhoodConnected: undefined };
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

async function api(path, options = {}) {
  const response = await fetch(path, { cache: "no-store", ...options });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || `请求失败 ${response.status}`);
  return data;
}
async function mutate(path, body = {}, method = "POST") {
  return api(path, { method, headers: LOCAL_HEADERS, body: JSON.stringify(body) });
}
function esc(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[char]));
}
function num(value, digits = 2) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toLocaleString("zh-CN", { maximumFractionDigits: digits, minimumFractionDigits: digits }) : "—";
}
function shortTime(value, withDate = false) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return esc(value);
  const options = withDate ? { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" } : { hour: "2-digit", minute: "2-digit", second: "2-digit" };
  return new Intl.DateTimeFormat("zh-CN", options).format(date);
}
function toast(message, error = false) {
  const target = $("#toast");
  target.textContent = message;
  target.className = `show${error ? " error" : ""}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { target.className = ""; }, 3600);
}

const STATUS_LABELS = { RECEIVED: "已接收", OBSERVED: "已观察", PENDING: "待确认", PAUSED: "暂停中", REJECTED: "已拒绝", PLACED: "已提交", FILLED: "已成交" };
const ACTION_LABELS = { OPEN: "开仓", ADD: "加仓", TRIM: "部分平仓", CLOSE: "平仓", EDIT: "编辑", EXPIRE: "到期" };
const MODE_LABELS = { observe: "仅观察", confirm: "人工确认", auto: "自动跟单" };
const BROKER_LABELS = { moomoo: "moomoo OpenD", robinhood: "Robinhood Agentic", ibkr: "IBKR（盈透）", webull: "Webull", schwab: "嘉信 Schwab" };

function statusTag(status) {
  return `<span class="status ${esc(String(status || "").toLowerCase())}">${esc(STATUS_LABELS[status] || status || "—")}</span>`;
}
function sideTag(side) {
  return `<span class="${side === "BUY" ? "side-buy" : "side-sell"}">${side === "BUY" ? "买入" : "卖出"}</span>`;
}

async function loadDashboard(quiet = false) {
  try {
    const data = await api("/api/dashboard");
    state.dashboard = data;
    renderDashboard(data);
  } catch (error) {
    const chip = $("#connection-chip");
    chip.className = "status-chip offline";
    chip.querySelector("span").textContent = "本机服务不可用";
    if (!quiet) toast(error.message, true);
  }
}

function renderDashboard(data) {
  const { health, settings, engine, moonvest, moonvest_positions: sourcePositions, signals, events, daily_notional: daily } = data;
  const brokerLabel = health.broker_label || BROKER_LABELS[settings.broker] || settings.broker;
  const capabilities = health.capabilities || {};
  const chip = $("#connection-chip");
  chip.className = `status-chip${health.connected ? "" : " offline"}`;
  chip.querySelector("span").textContent = health.connected ? `${brokerLabel} 已连接` : `${brokerLabel} 未连接`;
  $("#stat-broker-label").textContent = brokerLabel;
  $("#stat-broker").textContent = health.connected ? "连接正常" : "连接失败";
  const brokerDetail = [health.endpoint || (health.host && health.port ? `${health.host}:${health.port}` : ""), health.sdk_version ? `SDK ${health.sdk_version}` : "", capabilities.status || ""].filter(Boolean).join(" · ");
  $("#stat-broker-detail").textContent = health.connected ? brokerDetail : (health.error || "请检查券商连接");
  renderMoonvest(moonvest || {});
  $("#stat-env").textContent = settings.trading_env === "REAL" ? "实盘" : "模拟盘";
  const selectedAccount = selectedBrokerAccount(settings);
  $("#stat-account").textContent = selectedAccount ? `账户 ${selectedAccount} · ${brokerLabel}` : "尚未选择账户";
  $("#stat-notional").textContent = num(daily);
  $("#stat-limit").textContent = `单日限额 ${num(settings.max_daily_notional, 0)} ${settings.base_currency}`;
  $("#stat-engine").textContent = engine.paused ? "已暂停" : engine.armed ? "执行已启用" : "执行未启用";
  $("#stat-source").textContent = `唯一来源 · ${(settings.moonvest_follow || []).join(", ") || "未配置 follow"}`;

  const banner = $("#safety-banner");
  banner.className = `safety-banner${settings.trading_env === "REAL" ? " real" : settings.mode === "auto" ? " warning" : ""}`;
  banner.querySelector("strong").textContent = settings.trading_env === "REAL" ? "实盘模式" : settings.mode === "observe" ? "默认安全模式" : "执行保护已开启";
  banner.querySelector("p").textContent = settings.mode === "observe" ? "当前只记录与评估 Moonvest 事件，不会提交订单。" : engine.armed ? "订单执行已临时启用，所有事件仍需通过风控。" : "当前不会下单；启用执行后才可提交订单。";
  $("#mode-label").textContent = MODE_LABELS[settings.mode] || settings.mode;
  $("#pause-button").textContent = engine.paused ? "恢复" : "暂停";
  $("#arm-button").textContent = engine.armed ? "关闭执行" : "启用执行";
  $("#arm-button").className = engine.armed ? "btn danger" : "btn primary";
  $("#arm-button").disabled = !engine.armed && capabilities.execution === false;

  renderSourcePositions(sourcePositions || []);
  renderRecent(signals || []);
  renderPending((signals || []).filter((item) => item.status === "PENDING"));
  renderEvents(events || []);
  fillSettings(settings);
  const visibleBroker = $("#settings-form")?.dataset.dirty === "true" ? (document.querySelector('input[name="broker"]:checked')?.value || settings.broker) : settings.broker;
  renderBrokerConfig(visibleBroker, visibleBroker === settings.broker ? health : null);
  const robinhoodConnected = settings.broker === "robinhood" && Boolean(health.connected);
  const previousRobinhoodState = state.robinhoodConnected;
  state.robinhoodConnected = robinhoodConnected;
  if (previousRobinhoodState === false && robinhoodConnected) {
    loadAccounts(true).catch((error) => toast(error.message, true));
  }
}

function renderMoonvest(status) {
  const connected = Boolean(status.connected);
  const configured = Boolean(status.api_key_configured && (status.follow || []).length);
  const label = status.resync_required ? "待核对" : connected ? "实时流在线" : configured ? "正在重连" : "等待配置";
  $("#stat-moonvest").textContent = label;
  const cursor = status.cursor ? `cursor …${String(status.cursor).slice(-10)}` : "live tail";
  $("#stat-moonvest-detail").textContent = `${(status.follow || []).join(", ") || "未配置 follow"} · ${cursor}`;
  const badge = $("#moonvest-status-badge");
  if (badge) {
    badge.className = `connection-badge${connected ? "" : " offline"}`;
    badge.textContent = label;
  }
  const credential = $("#moonvest-credential-status");
  if (credential) {
    credential.className = status.api_key_configured ? "ready" : "";
    credential.textContent = status.api_key_configured ? `已配置 · ${status.credential_source === "environment" ? "环境变量" : "macOS 钥匙串"}` : "尚未保存 API key";
  }
  const cursorInput = $("#moonvest-cursor-input");
  if (cursorInput && document.activeElement !== cursorInput && cursorInput.dataset.edited !== "true") {
    cursorInput.value = status.cursor || "";
  }
  const detail = $("#moonvest-detail");
  if (detail) {
    const last = status.last_event_at ? `上次事件 ${shortTime(status.last_event_at, true)}` : "尚未收到事件";
    detail.textContent = connected ? `${last} · ${cursor} · 自动重连 ${status.reconnect_count || 0} 次` : `${status.last_error || "等待连接"} · ${cursor}`;
  }
}

function selectedBrokerAccount(settings) {
  if (settings.broker === "ibkr") return settings.ibkr_account_id || "";
  if (settings.broker === "webull") return settings.webull_account_id || "";
  if (settings.broker === "schwab") return settings.schwab_account_hash || "";
  if (settings.broker === "robinhood") return settings.robinhood_account_id || "";
  return settings.account_id || "";
}

function renderSourcePositions(items) {
  $("#source-position-count").textContent = items.length;
  $("#source-positions").innerHTML = items.map((item) => `<tr>
    <td><strong>${esc(item.actor)}</strong></td><td>${esc(item.symbol)}${item.expiry ? `<br><small>${esc(item.expiry)}</small>` : ""}</td>
    <td>${esc(item.asset_type)} · ${esc(item.kind)}</td><td>${sideTag(String(item.side || "").toUpperCase())}</td>
    <td>${num(item.quantity, 0)}</td><td>${item.stale ? '<span class="status stale">待同步</span>' : `<span class="status">${esc(item.status)}</span>`}</td>
    <td title="${esc(item.updated_event_id)}">…${esc(String(item.updated_event_id).slice(-10))}</td>
  </tr>`).join("") || `<tr><td colspan="7">尚无 Moonvest 持仓事件</td></tr>`;
}

function renderRecent(signals) {
  $("#recent-signals").innerHTML = signals.slice(0, 12).map((item) => `<tr>
    <td>${shortTime(item.created_at)}</td><td>${esc(item.leader || "—")}</td><td>${esc(ACTION_LABELS[item.action] || item.action)}</td>
    <td><strong>${esc(item.code)}</strong></td><td>${sideTag(item.side)}</td><td>${num(item.quantity, 0)} / ${num(item.copied_quantity, 0)}</td><td>${statusTag(item.status)}</td>
  </tr>`).join("") || `<tr><td colspan="7">尚无事件</td></tr>`;
}

function renderPending(items) {
  $("#pending-count").textContent = items.length;
  const list = $("#pending-list");
  if (!items.length) {
    list.className = "pending-list empty-state";
    list.innerHTML = `<p>暂无待确认事件</p><small>人工确认模式下，可执行事件会出现在这里。</small>`;
    return;
  }
  list.className = "pending-list";
  list.innerHTML = items.map((item) => `<div class="pending-item">
    <div class="pending-main"><strong>${esc(ACTION_LABELS[item.action] || item.action)} · ${esc(item.code)}</strong><span>${shortTime(item.created_at)}</span></div>
    <div class="pending-meta"><span>${sideTag(item.side)}</span><span>数量 ${num(item.copied_quantity, 0)}</span><span>@ ${num(item.execution_price, 3)}</span></div>
    <div class="pending-actions"><button class="btn ghost reject-signal" data-id="${esc(item.id)}">拒绝</button><button class="btn primary approve-signal" data-id="${esc(item.id)}">确认提交</button></div>
  </div>`).join("");
}

function renderAllSignals(signals) {
  $("#all-signals").innerHTML = signals.map((item) => `<tr>
    <td>${shortTime(item.created_at, true)}</td><td>${esc(item.leader || "—")}<br><small title="${esc(item.external_id)}">…${esc(String(item.external_id).slice(-12))}</small></td>
    <td>${esc(ACTION_LABELS[item.action] || item.action)}</td><td><strong>${esc(item.code)}</strong></td><td>${sideTag(item.side)}</td>
    <td>${num(item.quantity, 0)} → ${num(item.copied_quantity, 0)}</td><td>${num(item.notional)}</td><td>${statusTag(item.status)}</td>
    <td title="${esc(item.reason)}">${esc((item.reason || "").slice(0, 42)) || "—"}</td><td>${esc(item.broker_order_id || "—")}</td>
  </tr>`).join("") || `<tr><td colspan="10">尚无事件</td></tr>`;
}

function renderEvents(events) {
  $("#event-list").innerHTML = events.map((item) => `<div class="event-item ${esc(item.level)}"><time>${shortTime(item.created_at)}</time><i class="event-dot"></i><span class="event-kind">${esc(item.kind)}</span><span class="event-message">${esc(item.message)}</span></div>`).join("") || `<div class="empty-state"><p>尚无审计事件</p></div>`;
}

function fillSettings(settings) {
  const form = $("#settings-form");
  if (!form || form.dataset.dirty === "true") return;
  for (const [key, value] of Object.entries(settings)) {
    const field = form.elements.namedItem(key);
    if (!field) continue;
    if (field.type === "checkbox") field.checked = Boolean(value);
    else if (field.type === "radio") $$(`input[name="${key}"]`).forEach((input) => { input.checked = input.value === value; });
    else if (Array.isArray(value)) field.value = value.join(", ");
    else field.value = value;
  }
  form.elements.namedItem("moonvest_follow").value = (settings.moonvest_follow || []).join(", ");
  form.elements.namedItem("copy_ratio_pct").value = Math.round(Number(settings.copy_ratio || 1) * 100);
  $$('input[name="broker"]').forEach((input) => { input.checked = input.value === settings.broker; });
  setAccountSelection($("#account-select"), settings.broker, settings.broker === "moomoo" ? settings.security_firm : settings.broker.toUpperCase(), selectedBrokerAccount(settings), settings.trading_env);
  renderBrokerConfig(settings.broker, state.dashboard?.health || null);
}

function renderCredentialStatus(broker, status = {}) {
  const target = $(`#${broker}-credential-status`);
  if (!target) return;
  const required = broker === "webull" ? ["app_secret"] : ["access_token"];
  const ready = required.every((key) => Boolean(status[key]));
  const saved = Object.entries(status).filter(([, value]) => value).map(([key]) => key).join("、");
  target.className = ready ? "ready" : "";
  target.textContent = ready ? `钥匙串已保存：${saved}` : saved ? `部分已保存：${saved}` : "尚未保存 API 凭证";
}

function renderBrokerConfig(broker, health = null) {
  $$('[data-broker-config]').forEach((element) => element.classList.toggle("active", element.dataset.brokerConfig === broker));
  const otherBrokers = $("#other-brokers");
  if (otherBrokers) otherBrokers.open = broker !== "moomoo";
  const autoState = $("#opend-auto-state");
  if (autoState) {
    autoState.textContent = broker !== "moomoo" ? "推荐" : health?.connected ? "已自动连接" : "等待 OpenD";
  }
  const capability = $("#broker-capability");
  if (capability) {
    const execution = health?.capabilities?.execution ?? broker === "moomoo";
    capability.className = `connection-badge${execution ? "" : " offline"}`;
    capability.textContent = execution ? "完整接入 · 可执行" : "只读接入 · 执行锁定";
  }
  const transport = $("#api-transport-note span");
  if (transport) transport.textContent = health?.capabilities?.transport || "券商请求使用本地缓存与限频保护。";
  const robinhoodStatus = $("#robinhood-auth-status");
  const robinhoodConnected = broker === "robinhood" && Boolean(health?.connected);
  if (robinhoodStatus) {
    robinhoodStatus.textContent = robinhoodConnected
      ? `官方 OAuth 已连接 · 已载入 ${health.tool_count || 0} 个 MCP 工具 · 仅 Agentic 账户可执行`
      : health?.error || "点击连接后在 Robinhood 官方页面完成授权，无需输入密钥。";
  }
  if ($("#connect-robinhood")) $("#connect-robinhood").disabled = robinhoodConnected;
  if ($("#disconnect-robinhood")) $("#disconnect-robinhood").disabled = !robinhoodConnected;
  renderCredentialStatus("webull", health?.broker === "webull" ? health.credential_status : {});
  renderCredentialStatus("schwab", health?.broker === "schwab" ? health.credential_status : {});
}

function markSettingsDirty() { $("#settings-form").dataset.dirty = "true"; }

async function saveMoonvestKey() {
  const field = $("#moonvest-api-key");
  if (!field.value.trim()) throw new Error("请输入 Moonvest API key");
  const result = await mutate("/api/moonvest/credentials", { api_key: field.value });
  field.value = "";
  toast("Moonvest API key 已保存到 macOS 登录钥匙串");
  renderMoonvest({ ...(state.dashboard?.moonvest || {}), ...result });
  await loadDashboard(true);
}
async function clearMoonvestKey() {
  if (!window.confirm("确定从 macOS 登录钥匙串清除 Moonvest API key 吗？")) return;
  await mutate("/api/moonvest/credentials", { clear: true });
  toast("Moonvest API key 已清除");
  await loadDashboard(true);
}

async function applyMoonvestCursor() {
  const field = $("#moonvest-cursor-input");
  const cursor = field.value.trim();
  if (!cursor) throw new Error("请输入要恢复的 Moonvest cursor；如需 live tail 请点击清空 cursor");
  if ($("#settings-form").dataset.dirty === "true") await persistFormSettings(false);
  await mutate("/api/moonvest/cursor", { cursor });
  field.dataset.edited = "false";
  toast("恢复 cursor 已保存，Moonvest 正在回放并重连");
  await loadDashboard(true);
}

async function clearMoonvestCursor() {
  if (!window.confirm("确定清空 Moonvest cursor 并从 live tail 继续吗？")) return;
  await mutate("/api/moonvest/cursor", { cursor: "" });
  const field = $("#moonvest-cursor-input");
  field.value = "";
  field.dataset.edited = "false";
  toast("Moonvest cursor 已清空，正在连接 live tail");
  await loadDashboard(true);
}

async function saveBrokerCredentials(broker) {
  const payload = { broker };
  if (broker === "webull") {
    payload.app_secret = $("#webull-app-secret").value;
    payload.access_token = $("#webull-access-token").value;
  } else {
    payload.client_secret = $("#schwab-client-secret").value;
    payload.access_token = $("#schwab-access-token").value;
    payload.refresh_token = $("#schwab-refresh-token").value;
  }
  const result = await mutate("/api/broker/credentials", payload);
  $$(`[data-broker-config="${broker}"] .secret-input`).forEach((field) => { field.value = ""; });
  renderCredentialStatus(broker, result.status || {});
  toast(`${BROKER_LABELS[broker]} API 凭证已保存到 macOS 钥匙串`);
  await loadDashboard(true);
}
async function clearBrokerCredentials(broker) {
  if (!window.confirm(`确定清除 ${BROKER_LABELS[broker]} 的全部 API 凭证吗？`)) return;
  const result = await mutate("/api/broker/credentials", { broker, clear: true });
  renderCredentialStatus(broker, result.status || {});
  toast(`${BROKER_LABELS[broker]} API 凭证已清除`);
}

async function connectRobinhood() {
  const selected = document.querySelector('input[name="broker"]:checked')?.value;
  if (selected !== "robinhood") throw new Error("请先选择 Robinhood");
  await mutate("/api/robinhood/oauth/start");
  toast("已打开 Robinhood 官方授权页面；完成后返回 Moonvest 即可");
}

async function disconnectRobinhood() {
  if (!window.confirm("确定断开 Robinhood 官方 MCP 授权吗？")) return;
  await mutate("/api/robinhood/oauth/disconnect");
  toast("Robinhood 已断开");
  state.robinhoodConnected = false;
  await loadDashboard(true);
}

async function loadAccounts(quiet = false) {
  const broker = document.querySelector('input[name="broker"]:checked')?.value || state.dashboard?.settings.broker || "moomoo";
  const requestId = ++state.accountRequest;
  const select = $("#account-select");
  const note = $("#account-discovery-note");
  const previousSelection = select.value;
  const formWasDirty = $("#settings-form").dataset.dirty === "true";
  if (note) note.textContent = `正在从 ${BROKER_LABELS[broker] || broker} 自动发现账户…`;
  if (!quiet) toast(`正在从 ${BROKER_LABELS[broker] || broker} 发现账户…`);
  const data = await api(`/api/accounts?broker=${encodeURIComponent(broker)}`);
  const visibleBroker = document.querySelector('input[name="broker"]:checked')?.value || state.dashboard?.settings.broker || "moomoo";
  if (requestId !== state.accountRequest || visibleBroker !== broker) return;
  state.accounts = data.accounts || [];
  const selectable = state.accounts.filter((account) => account.selectable);
  select.innerHTML = [`<option value="">请选择账户</option>`, ...selectable.map((account) => `<option value="${esc(accountKey(account))}">${esc(account.display_name)}</option>`)].join("");
  const savedSettings = state.dashboard?.settings || {};
  const savedAccount = savedSettings.broker === broker ? selectedBrokerAccount(savedSettings) : "";
  if (previousSelection && [...select.options].some((option) => option.value === previousSelection)) {
    select.value = previousSelection;
  } else if (savedAccount) {
    setAccountSelection(select, broker, broker === "moomoo" ? savedSettings.security_firm : broker.toUpperCase(), savedAccount, savedSettings.trading_env);
  }
  if (!select.value && selectable.length === 1) {
    select.value = accountKey(selectable[0]);
    if (!formWasDirty && !savedAccount) {
      await persistDiscoveredAccount(selectable[0]);
      if (note) note.textContent = `已自动连接并选中 ${selectable[0].display_name}。`;
    } else {
      markSettingsDirty();
      if (note) note.textContent = `已自动选中 ${selectable[0].display_name}，保存设置即可使用。`;
    }
  } else if (selectable.length) {
    if (note) note.textContent = select.value ? "账户已自动载入。" : `发现 ${selectable.length} 个可用账户，请选择执行账户。`;
  } else if (note) {
    note.textContent = broker === "moomoo" ? "未发现可用账户，请确认 moomoo OpenD 已启动并已登录。" : broker === "robinhood" ? "未发现 Agentic 账户，请先完成 Robinhood 官方授权与开户。" : "未发现可用账户，请检查券商连接。";
  }
  if (!quiet) toast(selectable.length ? `已发现 ${selectable.length} 个可用账户` : "没有发现可用账户", !selectable.length);
}

async function persistDiscoveredAccount(account) {
  const current = { ...(state.dashboard?.settings || {}) };
  const parsed = parseAccountKey(accountKey(account));
  current.broker = parsed.broker;
  current.trading_env = parsed.env;
  if (parsed.broker === "moomoo") {
    current.account_id = Number(parsed.id);
    current.security_firm = parsed.firm || "FUTUJP";
  } else if (parsed.broker === "ibkr") current.ibkr_account_id = parsed.id;
  else if (parsed.broker === "webull") current.webull_account_id = parsed.id;
  else if (parsed.broker === "schwab") current.schwab_account_hash = parsed.id;
  else if (parsed.broker === "robinhood") current.robinhood_account_id = parsed.id;
  await mutate("/api/settings", current, "PUT");
  $("#settings-form").dataset.dirty = "false";
  await loadDashboard(true);
}
function accountKey(account) {
  const broker = account.broker || state.dashboard?.settings.broker || "moomoo";
  return `${broker}|${account.security_firm}|${encodeURIComponent(String(account.acc_id || ""))}|${account.trd_env}`;
}
function setAccountSelection(select, broker, firm, id, env) {
  if (!select || !id) return;
  const key = `${broker}|${firm}|${encodeURIComponent(String(id))}|${env}`;
  if ([...select.options].some((option) => option.value === key)) select.value = key;
}
function parseAccountKey(value) {
  const [broker, firm, encodedId, env] = String(value || "").split("|");
  return { broker: broker || "", firm: firm || "", id: decodeURIComponent(encodedId || ""), env: env || "SIMULATE" };
}

function settingsPayload(form) {
  const data = new FormData(form);
  const follower = parseAccountKey(data.get("account_key"));
  const current = state.dashboard?.settings || {};
  const broker = String(data.get("broker") || "moomoo");
  const payload = {
    broker,
    opend_host: data.get("opend_host"), opend_port: Number(data.get("opend_port")),
    ibkr_host: data.get("ibkr_host"), ibkr_port: Number(data.get("ibkr_port")),
    webull_environment: data.get("webull_environment"), webull_app_key: data.get("webull_app_key"),
    schwab_client_id: data.get("schwab_client_id"), schwab_callback_url: data.get("schwab_callback_url"),
    security_firm: current.security_firm || "FUTUJP", account_id: Number(current.account_id || 0),
    ibkr_account_id: current.ibkr_account_id || "", webull_account_id: current.webull_account_id || "", schwab_account_hash: current.schwab_account_hash || "", robinhood_account_id: current.robinhood_account_id || "",
    trading_env: follower.id ? follower.env : (current.trading_env || "SIMULATE"),
    base_currency: data.get("base_currency"), us_session: data.get("us_session"), mode: data.get("mode"),
    copy_ratio: Number(data.get("copy_ratio_pct")) / 100,
    max_slippage_pct: Number(data.get("max_slippage_pct")),
    max_order_notional: Number(data.get("max_order_notional")), max_daily_notional: Number(data.get("max_daily_notional")),
    allowed_markets: String(data.get("allowed_markets") || "").split(",").map((value) => value.trim()).filter(Boolean),
    allowed_symbols: String(data.get("allowed_symbols") || "").split(",").map((value) => value.trim()).filter(Boolean),
    allow_unmanaged_sells: data.has("allow_unmanaged_sells"),
    expiry_guard_enabled: data.has("expiry_guard_enabled"), expiry_open_cutoff_minutes: Number(data.get("expiry_open_cutoff_minutes")),
    reject_nonconforming_option_ticks: data.has("reject_nonconforming_option_ticks"),
    moonvest_follow: String(data.get("moonvest_follow") || "").split(/[\s,]+/).map((value) => value.trim()).filter(Boolean),
    moonvest_cursor_mode: data.get("moonvest_cursor_mode"),
  };
  if (follower.id) {
    if (broker === "moomoo") { payload.account_id = Number(follower.id); payload.security_firm = follower.firm || "FUTUJP"; }
    else if (broker === "ibkr") payload.ibkr_account_id = follower.id;
    else if (broker === "webull") payload.webull_account_id = follower.id;
    else if (broker === "schwab") payload.schwab_account_hash = follower.id;
    else if (broker === "robinhood") payload.robinhood_account_id = follower.id;
  }
  return payload;
}

async function persistFormSettings(discoverAccounts = true) {
  const form = $("#settings-form");
  const payload = settingsPayload(form);
  await mutate("/api/settings", payload, "PUT");
  form.dataset.dirty = "false";
  await loadDashboard(true);
  if (discoverAccounts) await loadAccounts(true);
}

async function saveSettings() {
  const button = $("#save-settings");
  button.disabled = true;
  button.textContent = "正在保存…";
  try {
    await persistFormSettings(true);
    toast("设置已保存，SSE 将按新配置重连，订单执行已关闭");
  } finally {
    button.disabled = false;
    button.textContent = "保存设置";
  }
}

async function armToggle() {
  const dashboard = state.dashboard;
  if (!dashboard) return;
  if (dashboard.engine.armed) {
    await mutate("/api/engine/disarm");
    toast("订单执行已关闭");
  } else {
    if (dashboard.settings.mode === "observe") return showView("settings", "请先切换到人工确认或自动跟单");
    if (dashboard.moonvest.resync_required && !window.confirm("最近发生过 resync，来源状态已标记待核对。确认已核对券商持仓并继续启用执行吗？")) return;
    let manual = false;
    if (dashboard.settings.trading_env === "REAL") {
      manual = window.confirm("这将允许应用提交实盘订单。请确认已在券商端完成授权/解锁，并已核对账户与限额。是否继续？");
      if (!manual) return;
    }
    await mutate("/api/engine/arm", { manual_unlock_confirmed: manual });
    toast("订单执行已启用，4 小时后自动关闭");
  }
  await loadDashboard(true);
}
async function pauseToggle() {
  if (!state.dashboard) return;
  await mutate("/api/engine/pause", { paused: !state.dashboard.engine.paused });
  await loadDashboard(true);
}
async function signalAction(event) {
  const approve = event.target.closest(".approve-signal");
  const reject = event.target.closest(".reject-signal");
  if (!approve && !reject) return;
  const button = approve || reject;
  button.disabled = true;
  try {
    if (approve) await mutate(`/api/signals/${button.dataset.id}/approve`);
    else await mutate(`/api/signals/${button.dataset.id}/reject`, { reason: "操作员拒绝" });
    toast(approve ? "订单已提交" : "事件已拒绝");
    await loadDashboard(true);
  } finally { button.disabled = false; }
}

async function loadPortfolio() {
  try {
    const [portfolio, orders] = await Promise.all([api("/api/portfolio"), api("/api/orders")]);
    renderPortfolio(portfolio, orders.orders || []);
  } catch (error) { toast(error.message, true); }
}
function renderPortfolio(portfolio, orders) {
  const funds = portfolio.funds?.[0] || {};
  const cards = [["总资产", funds.total_assets ?? funds.total_assets_val], ["现金", funds.cash], ["证券市值", funds.market_val], ["未实现盈亏", funds.unrealized_pl]];
  $("#fund-cards").innerHTML = cards.map(([label, value]) => `<article class="stat-card"><span>${label}</span><strong class="${Number(value) > 0 && label.includes("盈亏") ? "positive" : Number(value) < 0 ? "negative" : ""}">${num(value)}</strong><small>${esc(state.dashboard?.settings.base_currency || "")}</small></article>`).join("");
  const positions = portfolio.positions || [];
  $("#position-count").textContent = positions.length;
  $("#positions-body").innerHTML = positions.map((item) => `<tr><td><strong>${esc(item.code)}</strong></td><td>${esc(item.stock_name || item.name || "")}</td><td>${num(item.qty, 0)}</td><td>${num(item.can_sell_qty, 0)}</td><td>${num(item.average_cost ?? item.cost_price, 3)}</td><td>${num(item.nominal_price ?? item.price, 3)}</td><td>${num(item.market_val)}</td><td class="${Number(item.unrealized_pl) >= 0 ? "positive" : "negative"}">${num(item.unrealized_pl ?? item.pl_val)}</td></tr>`).join("") || `<tr><td colspan="8">当前无持仓</td></tr>`;
  $("#order-count").textContent = orders.length;
  $("#orders-body").innerHTML = orders.map((item) => `<tr><td>${esc(item.create_time || "—")}</td><td><strong>${esc(item.code)}</strong></td><td>${sideTag(String(item.trd_side || "").toUpperCase())}</td><td>${num(item.qty, 0)}</td><td>${num(item.price, 3)}</td><td>${num(item.dealt_qty, 0)} @ ${num(item.dealt_avg_price, 3)}</td><td>${esc(item.order_status)}</td><td>${esc(item.order_id)}</td></tr>`).join("") || `<tr><td colspan="8">今日无订单</td></tr>`;
}
async function loadSignals() { try { const data = await api("/api/signals?limit=300"); renderAllSignals(data.signals || []); } catch (error) { toast(error.message, true); } }
async function loadEvents() { try { const data = await api("/api/events?limit=300"); renderEvents(data.events || []); } catch (error) { toast(error.message, true); } }

function showView(name, message) {
  state.activeView = name;
  $$(".view").forEach((element) => element.classList.toggle("active", element.id === `view-${name}`));
  $$(".nav-item").forEach((element) => element.classList.toggle("active", element.dataset.view === name));
  const titles = { dashboard: "跟单总览", portfolio: "账户与持仓", signals: "事件流水", settings: "连接与风控", events: "审计日志" };
  const eyebrows = { dashboard: "MOONVEST ACTIVE TRADES", portfolio: "ACCOUNT TRUTH", signals: "EVENT LEDGER", settings: "CONNECTION & GUARDRAILS", events: "LOCAL AUDIT TRAIL" };
  $("#page-title").textContent = titles[name] || "Moonvest";
  $("#page-eyebrow").textContent = eyebrows[name] || "MOONVEST";
  if (name === "portfolio") loadPortfolio();
  if (name === "signals") loadSignals();
  if (name === "events") loadEvents();
  if (message) toast(message);
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function bind() {
  $$(".nav-item").forEach((button) => button.addEventListener("click", () => showView(button.dataset.view)));
  $$('[data-view-jump]').forEach((button) => button.addEventListener("click", () => showView(button.dataset.viewJump)));
  $("#settings-form").addEventListener("submit", (event) => event.preventDefault());
  $("#save-settings").addEventListener("click", () => saveSettings().catch((error) => toast(error.message, true)));
  $("#settings-form").addEventListener("change", (event) => { if (event.isTrusted && !event.target.matches("[data-runtime-field]")) markSettingsDirty(); });
  $$('input[name="broker"]').forEach((input) => input.addEventListener("change", () => {
    renderBrokerConfig(input.value, null);
    state.accounts = [];
    $("#account-select").innerHTML = `<option value="">正在从 ${esc(BROKER_LABELS[input.value] || input.value)} 发现账户…</option>`;
    $("#account-discovery-note").textContent = "无需先保存券商，正在读取可用账户。";
    loadAccounts(true).catch((error) => { $("#account-discovery-note").textContent = error.message; });
  }));
  $("#save-moonvest-key").addEventListener("click", () => saveMoonvestKey().catch((error) => toast(error.message, true)));
  $("#clear-moonvest-key").addEventListener("click", () => clearMoonvestKey().catch((error) => toast(error.message, true)));
  $("#moonvest-cursor-input").addEventListener("input", (event) => { if (event.isTrusted) event.target.dataset.edited = "true"; });
  $("#apply-moonvest-cursor").addEventListener("click", () => applyMoonvestCursor().catch((error) => toast(error.message, true)));
  $("#clear-moonvest-cursor").addEventListener("click", () => clearMoonvestCursor().catch((error) => toast(error.message, true)));
  $("#save-webull-credentials").addEventListener("click", () => saveBrokerCredentials("webull").catch((error) => toast(error.message, true)));
  $("#save-schwab-credentials").addEventListener("click", () => saveBrokerCredentials("schwab").catch((error) => toast(error.message, true)));
  $("#connect-robinhood").addEventListener("click", () => connectRobinhood().catch((error) => toast(error.message, true)));
  $("#disconnect-robinhood").addEventListener("click", () => disconnectRobinhood().catch((error) => toast(error.message, true)));
  $$(".clear-broker-credentials").forEach((button) => button.addEventListener("click", () => clearBrokerCredentials(button.dataset.broker).catch((error) => toast(error.message, true))));
  $("#arm-button").addEventListener("click", () => armToggle().catch((error) => toast(error.message, true)));
  $("#pause-button").addEventListener("click", () => pauseToggle().catch((error) => toast(error.message, true)));
  $("#pending-list").addEventListener("click", (event) => signalAction(event).catch((error) => toast(error.message, true)));
  $("#reload-accounts").addEventListener("click", () => loadAccounts().catch((error) => toast(error.message, true)));
  $("#refresh-portfolio").addEventListener("click", loadPortfolio);
  $("#refresh-signals").addEventListener("click", loadSignals);
  $("#refresh-events").addEventListener("click", loadEvents);
  window.addEventListener("focus", () => {
    const broker = document.querySelector('input[name="broker"]:checked')?.value;
    if (broker === "robinhood") loadAccounts(true).catch(() => {});
  });
}

bind();
loadDashboard().then(() => loadAccounts(true).catch((error) => {
  const note = $("#account-discovery-note");
  if (note) note.textContent = error.message;
}));
state.timer = setInterval(() => { if (document.visibilityState === "visible") loadDashboard(true); }, 5000);
