const PAGE_PATHS = {
  alerts: "/alerts",
  campaigns: "/campaigns/draft",
};

const state = {
  health: null,
  accounts: [],
  alerts: [],
  decisions: [],
  actions: [],
  weeklyReport: null,
  thresholds: [],
  notifications: [],
  selectedAccountId: null,
  selectedAlertId: null,
  selectedAccountDetail: null,
  selectedDraftId: null,
  draftList: [],
  selectedDraft: null,
  lastSyncAt: null,
  currentPage: "alerts",
  pendingDecisions: new Set(),
  notificationTrayOpen: false,
  explainDrawerOpen: false,
  notificationSeenAt: localStorage.getItem("adsGenie.notificationSeenAt") || "",
  draftForm: {
    prompt: "",
    campaignGoal: "Lead generation",
    targetGeography: "",
    monthlyBudget: "",
    files: [],
  },
  changeRequestOpen: false,
  changeRequestNote: "",
};

const el = {
  shell: document.querySelector(".shell"),
  navTabs: document.querySelectorAll(".nav-tab"),
  accountRail: document.getElementById("accountRail"),
  runMonitoringBtn: document.getElementById("runMonitoringBtn"),
  accountsMonitored: document.getElementById("accountsMonitored"),
  lastSyncLabel: document.getElementById("lastSyncLabel"),
  selectedAccountMeta: document.getElementById("selectedAccountMeta"),
  selectedAccountTitle: document.getElementById("selectedAccountTitle"),
  monitoringStatus: document.getElementById("monitoringStatus"),
  flashHost: document.getElementById("flashHost"),
  alertsPage: document.getElementById("alertsPage"),
  campaignPage: document.getElementById("campaignPage"),
  metricCards: document.getElementById("metricCards"),
  queueSummary: document.getElementById("queueSummary"),
  alertFeed: document.getElementById("alertFeed"),
  systemStatus: document.getElementById("systemStatus"),
  weeklyReport: document.getElementById("weeklyReport"),
  calibrateThresholdsBtn: document.getElementById("calibrateThresholdsBtn"),
  generateWeeklyBtn: document.getElementById("generateWeeklyBtn"),
  loadWeeklyBtn: document.getElementById("loadWeeklyBtn"),
  notificationToggle: document.getElementById("notificationToggle"),
  notificationCount: document.getElementById("notificationCount"),
  notificationTray: document.getElementById("notificationTray"),
  closeNotificationTray: document.getElementById("closeNotificationTray"),
  notificationList: document.getElementById("notificationList"),
  campaignPageTitle: document.getElementById("campaignPageTitle"),
  draftSelector: document.getElementById("draftSelector"),
  refreshDraftsBtn: document.getElementById("refreshDraftsBtn"),
  buildCampaignBtn: document.getElementById("buildCampaignBtn"),
  campaignPromptInput: document.getElementById("campaignPromptInput"),
  campaignGoalInput: document.getElementById("campaignGoalInput"),
  campaignGeoInput: document.getElementById("campaignGeoInput"),
  campaignBudgetInput: document.getElementById("campaignBudgetInput"),
  campaignFilesInput: document.getElementById("campaignFilesInput"),
  campaignFileList: document.getElementById("campaignFileList"),
  campaignContextHint: document.getElementById("campaignContextHint"),
  campaignEmptyState: document.getElementById("campaignEmptyState"),
  campaignWorkspace: document.getElementById("campaignWorkspace"),
  campaignExplainDrawer: document.getElementById("campaignExplainDrawer"),
  campaignExplainBody: document.getElementById("campaignExplainBody"),
  closeExplainDrawer: document.getElementById("closeExplainDrawer"),
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function sanitizePayload(value) {
  if (typeof value === "string") return escapeHtml(value);
  if (Array.isArray(value)) return value.map((item) => sanitizePayload(item));
  if (value && typeof value === "object") {
    const clean = {};
    Object.entries(value).forEach(([key, item]) => {
      clean[key] = sanitizePayload(item);
    });
    return clean;
  }
  return value;
}

function renderErrorCard(target, message) {
  if (!target) return;
  target.innerHTML = "";
  const node = document.createElement("div");
  node.className = "empty-card";
  node.textContent = String(message || "Unexpected error");
  target.appendChild(node);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const payload = sanitizePayload(await response.json());
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || `Request failed: ${response.status}`);
  }
  return payload;
}

function showFlash(message, tone = "info") {
  if (!el.flashHost) return;
  const node = document.createElement("div");
  node.className = `flash ${tone}`;
  node.textContent = String(message || "Update complete");
  el.flashHost.appendChild(node);
  setTimeout(() => node.remove(), 4200);
}

function formatCurrency(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: number >= 100 ? 0 : 2,
  }).format(number);
}

function formatNumber(value, decimals = 1) {
  return Number(value || 0).toFixed(decimals);
}

function formatPercent(value, decimals = 1) {
  return `${Number(value || 0).toFixed(decimals)}%`;
}

function humanFileSize(bytes) {
  const size = Number(bytes || 0);
  if (size < 1024) return `${size} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function timeAgo(value) {
  if (!value) return "just now";
  const diffMs = Date.now() - new Date(value).getTime();
  const mins = Math.max(0, Math.round(diffMs / 60000));
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.round(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function signDelta(current, previous, suffix = "%") {
  const cur = Number(current || 0);
  const prev = Number(previous || 0);
  if (!prev) return `new ${suffix === "x" ? "signal" : "baseline"}`;
  const delta = ((cur - prev) / Math.abs(prev)) * 100;
  return `${delta >= 0 ? "+" : ""}${delta.toFixed(1)}% WoW`;
}

function statusClass(value) {
  return String(value || "none").toLowerCase();
}

function selectedAccount() {
  return state.accounts.find((account) => Number(account.id) === Number(state.selectedAccountId)) || null;
}

function selectedAlert() {
  return state.alerts.find((alert) => Number(alert.id) === Number(state.selectedAlertId)) || null;
}

function accountAlerts(accountId) {
  return state.alerts
    .filter((alert) => Number(alert.account_id) === Number(accountId))
    .sort((left, right) => {
      const order = { open: 0, escalated: 1, executed: 2, dismissed: 3, rolled_back: 4 };
      return (order[left.status] ?? 9) - (order[right.status] ?? 9);
    });
}

function accountActions(accountId) {
  return state.actions.filter((action) => Number(action.account_id) === Number(accountId));
}

function alertTags(alert) {
  const tags = [];
  const actions = alert.recommendation?.actions || [];
  actions.forEach((action) => {
    if (action.action_type) tags.push(String(action.action_type).replaceAll("_", " "));
    if (Array.isArray(action.params?.keywords)) tags.push(...action.params.keywords.slice(0, 3));
    if (action.params?.campaign_name) tags.push(action.params.campaign_name);
  });
  (alert.context?.roas_drop?.root_causes || []).forEach((item) => {
    if (item.cause) tags.push(String(item.cause).replaceAll("_", " "));
  });
  return [...new Set(tags)].slice(0, 6);
}

function primaryAlertAction(alert) {
  return alert?.recommendation?.actions?.[0] || null;
}

function unreadNotificationCount() {
  return state.notifications.filter((item) => !state.notificationSeenAt || String(item.created_at) > state.notificationSeenAt).length;
}

function isNotificationTrayTarget(target) {
  if (!(target instanceof Element)) return false;
  return Boolean(target.closest("#notificationTray") || target.closest("#notificationToggle"));
}

function syncNotificationTray() {
  if (!el.notificationToggle || !el.notificationTray) return;
  el.notificationToggle.setAttribute("aria-expanded", String(state.notificationTrayOpen));
  el.notificationTray.hidden = !state.notificationTrayOpen;
  if (state.notificationTrayOpen) {
    renderNotificationTray();
  }
}

function setNotificationTrayOpen(nextOpen, { focusToggle = false } = {}) {
  const normalized = Boolean(nextOpen);
  if (state.notificationTrayOpen === normalized) {
    syncNotificationTray();
    return;
  }
  state.notificationTrayOpen = normalized;
  syncNotificationTray();
  if (!normalized && focusToggle) {
    el.notificationToggle?.focus();
  }
}

function closeNotificationTray(options = {}) {
  setNotificationTrayOpen(false, options);
}

function openNotificationTray() {
  markNotificationsSeen();
  setNotificationTrayOpen(true);
}

function toggleNotificationTray() {
  if (state.notificationTrayOpen) {
    closeNotificationTray();
    return;
  }
  openNotificationTray();
}

function currentRoute() {
  const { pathname, search } = window.location;
  const query = new URLSearchParams(search);
  const match = pathname.match(/^\/campaigns\/draft\/(\d+)$/);
  return {
    page: pathname.startsWith("/campaigns/draft") ? "campaigns" : "alerts",
    draftId: match ? Number(match[1]) : Number(query.get("draft") || 0) || null,
  };
}

function syncRoute(replace = false) {
  let path = PAGE_PATHS[state.currentPage] || PAGE_PATHS.alerts;
  if (state.currentPage === "campaigns" && state.selectedDraftId) {
    path = `/campaigns/draft/${state.selectedDraftId}`;
  }
  if (`${window.location.pathname}${window.location.search}` === path) return;
  window.history[replace ? "replaceState" : "pushState"]({}, "", path);
}

function switchPage(page, { replace = false } = {}) {
  state.currentPage = page === "campaigns" ? "campaigns" : "alerts";
  el.shell.setAttribute("data-page", state.currentPage);
  el.navTabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.page === state.currentPage);
  });
  syncRoute(replace);
  render();
}

function updateDraftFormFromInputs() {
  state.draftForm.prompt = el.campaignPromptInput.value;
  state.draftForm.campaignGoal = el.campaignGoalInput.value;
  state.draftForm.targetGeography = el.campaignGeoInput.value;
  state.draftForm.monthlyBudget = el.campaignBudgetInput.value;
}

function defaultBudgetForAccount(account) {
  return Math.max(2500, Math.round((account?.health?.metrics?.spend_7d || 0) * 4.2));
}

function resetDraftForm(account, { preservePrompt = false } = {}) {
  state.draftForm = {
    prompt: preservePrompt ? state.draftForm.prompt : "",
    campaignGoal: "Lead generation",
    targetGeography: account ? `${account.name} +25mi` : "",
    monthlyBudget: account ? String(defaultBudgetForAccount(account)) : "3000",
    files: [],
  };
}

function syncDraftFormFromDraft(draft) {
  const account = selectedAccount();
  state.draftForm = {
    prompt: draft?.prompt_text || "",
    campaignGoal: draft?.campaign_goal || "Lead generation",
    targetGeography: draft?.target_geography || (account ? `${account.name} +25mi` : ""),
    monthlyBudget: String(Math.round(Number(draft?.monthly_budget || defaultBudgetForAccount(account)))),
    files: [],
  };
}

async function loadHealth() {
  const payload = await api("/api/health");
  state.health = payload;
}

async function loadNotifications() {
  const payload = await api("/api/notifications?limit=20").catch(() => ({ notifications: [] }));
  state.notifications = payload.notifications || [];
}

async function loadSelectedAccountDetail() {
  const account = selectedAccount();
  if (!account) {
    state.selectedAccountDetail = null;
    return;
  }
  const payload = await api(`/api/accounts/${account.id}`);
  state.selectedAccountDetail = payload;
}

async function loadDraftsForSelectedAccount(preferredDraftId = null) {
  const account = selectedAccount();
  if (!account) {
    state.draftList = [];
    state.selectedDraftId = null;
    state.selectedDraft = null;
    return;
  }
  const payload = await api(`/api/accounts/${account.id}/campaign-drafts`).catch(() => ({ drafts: [] }));
  state.draftList = payload.drafts || [];
  let nextDraftId = preferredDraftId || state.selectedDraftId;
  if (!state.draftList.some((draft) => Number(draft.id) === Number(nextDraftId))) {
    nextDraftId = state.draftList[0] ? Number(state.draftList[0].id) : null;
  }
  state.selectedDraftId = nextDraftId;
  state.selectedDraft = state.draftList.find((draft) => Number(draft.id) === Number(nextDraftId)) || null;
  if (state.selectedDraft) {
    syncDraftFormFromDraft(state.selectedDraft);
  } else {
    resetDraftForm(account);
  }
}

async function loadDashboard({ preferredDraftId = null } = {}) {
  const [accountsPayload, alertsPayload, decisionsPayload, actionsPayload, weeklyPayload, thresholdsPayload] = await Promise.all([
    api("/api/accounts"),
    api("/api/alerts"),
    api("/api/decisions"),
    api("/api/actions"),
    api("/api/reports/weekly/latest").catch(() => ({ report: null })),
    api("/api/thresholds").catch(() => ({ thresholds: [] })),
  ]);

  state.accounts = accountsPayload.accounts || [];
  state.alerts = alertsPayload.alerts || [];
  state.decisions = decisionsPayload.decisions || [];
  state.actions = (actionsPayload.actions || []).map((action) => ({ ...action, params: action.params || {} }));
  state.weeklyReport = weeklyPayload.report || null;
  state.thresholds = thresholdsPayload.thresholds || [];
  state.lastSyncAt = new Date().toISOString();

  if (!state.selectedAccountId && state.accounts.length) {
    state.selectedAccountId = Number(state.accounts[0].id);
  }

  if (state.selectedAccountId && !state.accounts.some((account) => Number(account.id) === Number(state.selectedAccountId))) {
    state.selectedAccountId = state.accounts[0] ? Number(state.accounts[0].id) : null;
  }

  if (state.selectedAccountId) {
    await Promise.all([loadSelectedAccountDetail(), loadDraftsForSelectedAccount(preferredDraftId)]);
    const alertsForAccount = accountAlerts(state.selectedAccountId);
    if (!alertsForAccount.some((alert) => Number(alert.id) === Number(state.selectedAlertId))) {
      state.selectedAlertId = alertsForAccount[0] ? Number(alertsForAccount[0].id) : null;
    }
  }
}

function renderTopBar() {
  const isLive = Boolean(state.health?.scheduler_enabled);
  el.monitoringStatus.className = `status-pill ${isLive ? "live" : "warn"}`;
  el.monitoringStatus.innerHTML = `<span class="status-dot"></span>${isLive ? "Monitoring live" : "Manual mode"}`;
  const unread = unreadNotificationCount();
  el.notificationCount.textContent = String(unread);
  el.notificationCount.hidden = unread === 0;
  syncNotificationTray();
}

function renderAccountRail() {
  el.accountsMonitored.textContent = String(state.accounts.length);
  el.lastSyncLabel.textContent = timeAgo(state.lastSyncAt);

  if (!state.accounts.length) {
    el.accountRail.innerHTML = '<div class="empty-card">No accounts available yet.</div>';
    return;
  }

  el.accountRail.innerHTML = state.accounts
    .map((account) => {
      const alerts = accountAlerts(account.id);
      const pending = alerts.filter((alert) => ["open", "escalated"].includes(alert.status)).length;
      const active = Number(account.id) === Number(state.selectedAccountId);
      return `
        <button class="account-row ${active ? "active" : ""}" data-account-id="${account.id}">
          <div class="account-main">
            <span class="account-dot ${statusClass(account.health?.severity)}"></span>
            <div>
              <div class="account-name">${account.name}</div>
              <div class="account-sub">${String(account.vertical).replaceAll("_", " ")}</div>
            </div>
          </div>
          ${pending ? `<span class="account-count">${pending}</span>` : ""}
        </button>
      `;
    })
    .join("");
}

function renderAlertsHeader() {
  const account = selectedAccount();
  if (!account) {
    el.selectedAccountMeta.textContent = "No account selected";
    el.selectedAccountTitle.textContent = "Ads Genie";
    return;
  }
  const campaigns = state.selectedAccountDetail?.campaigns || [];
  const health = account.health?.metrics || {};
  el.selectedAccountMeta.textContent = `${String(account.vertical).replaceAll("_", " ")} · ${campaigns.length} campaigns · ${formatCurrency(health.cpa_7d)} CPA`;
  el.selectedAccountTitle.textContent = account.name;
}

function renderMetricCards() {
  const account = selectedAccount();
  if (!account) {
    el.metricCards.innerHTML = "";
    return;
  }

  const health = account.health?.metrics || {};
  const waste = account.waste?.components || {};
  const benchmark = account.health?.benchmark || {};
  const metrics = [
    {
      label: "Spend (7D)",
      value: formatCurrency(health.spend_7d),
      delta: signDelta(health.spend_7d, health.spend_prev_7d),
      tone: "neutral",
    },
    {
      label: "Avg. CPA",
      value: formatCurrency(health.cpa_7d),
      delta: `vs ${formatCurrency(benchmark.cpa_target)} target`,
      tone: Number(health.cpa_7d) > Number(benchmark.cpa_target) ? "warn" : "good",
    },
    {
      label: "ROAS",
      value: `${formatNumber(health.roas_7d, 1)}x`,
      delta: signDelta(health.roas_7d, health.roas_prev_7d, "x"),
      tone: Number(health.roas_7d) < Number(benchmark.roas_healthy) ? "bad" : "good",
    },
    {
      label: "Wasted Spend",
      value: formatCurrency(waste.estimated_total_waste),
      delta: `${formatNumber((waste.waste_ratio || 0) * 100, 1)}% of spend`,
      tone: Number(waste.waste_ratio) >= 0.25 ? "bad" : "warn",
    },
  ];

  el.metricCards.innerHTML = metrics
    .map(
      (metric) => `
        <article class="metric-card ${metric.tone}">
          <h4>${metric.label}</h4>
          <strong>${metric.value}</strong>
          <span class="metric-delta">${metric.delta}</span>
        </article>
      `
    )
    .join("");
}

function renderAlertFeed() {
  const account = selectedAccount();
  if (!account) {
    el.alertFeed.innerHTML = '<div class="empty-card">Select an account to review alerts.</div>';
    el.queueSummary.textContent = "0 pending · 0 approved";
    return;
  }

  const alerts = accountAlerts(account.id);
  const pendingCount = alerts.filter((alert) => ["open", "escalated"].includes(alert.status)).length;
  const approvedCount = accountActions(account.id).filter((action) => action.status === "executed").length;
  el.queueSummary.textContent = `${pendingCount} pending · ${approvedCount} approved`;

  if (!alerts.length) {
    el.alertFeed.innerHTML = '<div class="empty-card">No alerts right now. Monitoring is active.</div>';
    return;
  }

  el.alertFeed.innerHTML = alerts
    .map((alert) => {
      const isActive = Number(alert.id) === Number(state.selectedAlertId);
      const action = primaryAlertAction(alert);
      const tags = alertTags(alert).map((tag) => `<span class="alert-chip">${tag}</span>`).join("");
      const pending = state.pendingDecisions.has(Number(alert.id));
      const showBuilder = action?.action_type === "draft_campaign";
      const canAct = ["open", "escalated"].includes(alert.status) && !pending;
      return `
        <article class="alert-card severity-${statusClass(alert.severity)} status-${statusClass(alert.status)} ${isActive ? "active" : ""}" data-alert-id="${alert.id}">
          <div class="alert-top">
            <span class="section-kicker">${account.name.toUpperCase()} · ${String(alert.alert_type).replaceAll("_", " ")}</span>
            <span class="status-tag ${statusClass(alert.status)}">${alert.status}</span>
          </div>
          <h4 class="alert-title">${alert.title}</h4>
          <p class="alert-body">${action?.reason || alert.summary}</p>
          <div class="alert-chip-row">${tags}</div>
          <p class="alert-meta">Detected ${timeAgo(alert.created_at)} · ${alert.autonomy_level} · ${alert.recommendation?.actions?.map((item) => item.action_type).join(" · ") || "no action"}</p>
          <div class="alert-actions">
            ${showBuilder ? `<button class="action-btn ghost" data-open-builder="${alert.id}">Open builder</button>` : ""}
            ${canAct ? `<button class="action-btn primary" data-decision="approve" data-alert-id="${alert.id}">Approve</button><button class="action-btn ghost" data-decision="modify" data-alert-id="${alert.id}">Modify</button><button class="action-btn ghost" data-decision="dismiss" data-alert-id="${alert.id}">Dismiss</button>` : pending ? `<span class="inline-status">Processing...</span>` : ""}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderWeeklyReport() {
  const runtime = state.health?.runtime || {};
  const categories = (runtime.top_categories || []).slice(0, 3).map(([name, count]) => `${name} (${count})`).join(" · ");
  const authConfigured = Boolean(state.health?.auth_configured);
  const authRequired = Boolean(state.health?.auth_required);
  const authLabel = authConfigured ? "Enabled" : authRequired ? "Required" : "Optional (local)";
  const authHint = authConfigured
    ? "Dashboard/API auth is active."
    : authRequired
    ? "Set APP_AUTH_USERNAME and APP_AUTH_PASSWORD before using mutating endpoints."
    : "Enable APP_AUTH_ENABLED and set APP_AUTH_PASSWORD to lock local dashboard/API.";

  el.systemStatus.innerHTML = `
    <div class="system-card">
      <h4>Google Ads</h4>
      <strong>${state.health?.google_ads_configured ? "Configured" : "Demo mode"}</strong>
      <p>${state.health?.google_ads_configured ? "Read path is ready for live accounts." : "Still using demo fallback data."}</p>
    </div>
    <div class="system-card">
      <h4>Slack</h4>
      <strong>${state.health?.slack_configured ? "Configured" : "Not configured"}</strong>
      <p>${state.health?.slack_configured ? "Interactive approvals are wired." : "Interactive approvals are implemented but waiting on credentials."}</p>
    </div>
    <div class="system-card">
      <h4>Auth</h4>
      <strong>${authLabel}</strong>
      <p>${authHint}</p>
    </div>
    <div class="system-card">
      <h4>Runtime</h4>
      <strong>${runtime.event_count || 0} events</strong>
      <p>${categories || "No recent runtime events logged."}</p>
    </div>
  `;

  if (!state.weeklyReport) {
    el.weeklyReport.innerHTML = '<div class="empty-card">No weekly report yet. Click refresh to generate one.</div>';
    return;
  }

  const content = state.weeklyReport.content_markdown || "";
  const lines = content.split("\n").filter(Boolean);
  const accountsReviewed = (lines.find((line) => line.includes("Accounts reviewed")) || "").split(":").pop()?.trim() || "-";
  const highFlags = (lines.find((line) => line.includes("High/Critical health flags")) || "").split(":").pop()?.trim() || "-";
  const criticalFlags = (lines.find((line) => line.includes("Critical health flags")) || "").split(":").pop()?.trim() || "-";

  el.weeklyReport.innerHTML = `
    <div class="report-shell">
      <div class="report-card">
        <div class="report-summary"><span>Accounts reviewed</span><strong>${accountsReviewed}</strong></div>
        <div class="report-summary"><span>High / critical flags</span><strong>${highFlags}</strong></div>
        <div class="report-summary"><span>Critical flags</span><strong>${criticalFlags}</strong></div>
      </div>
      <div class="report-preview">
        <pre>${content}</pre>
      </div>
    </div>
  `;
}

function renderNotificationTray() {
  if (!state.notifications.length) {
    el.notificationList.innerHTML = '<div class="empty-card">No notifications yet.</div>';
    return;
  }

  el.notificationList.innerHTML = state.notifications
    .map(
      (item) => `
        <button class="notification-card ${statusClass(item.severity)}" data-notification-id="${item.id}" data-kind="${item.kind}" data-account-id="${item.account_id || ""}" data-draft-id="${item.draft_id || ""}">
          <div class="notification-card-top">
            <span class="notification-kind">${String(item.kind || "system").replaceAll("_", " ")}</span>
            <span class="notification-time">${timeAgo(item.created_at)}</span>
          </div>
          <strong>${item.title || "Update"}</strong>
          <p>${item.body || "No detail provided."}</p>
        </button>
      `
    )
    .join("");
}

function renderDraftSelector() {
  if (!state.draftList.length) {
    el.draftSelector.innerHTML = '<option value="">No drafts</option>';
    el.draftSelector.disabled = true;
    return;
  }
  el.draftSelector.disabled = false;
  el.draftSelector.innerHTML = state.draftList
    .map((draft) => {
      const selected = Number(draft.id) === Number(state.selectedDraftId) ? "selected" : "";
      return `<option value="${draft.id}" ${selected}>#${draft.id} · ${draft.draft?.campaign_name || draft.campaign_goal} · ${draft.status}</option>`;
    })
    .join("");
}

function renderDraftForm() {
  el.campaignPromptInput.value = state.draftForm.prompt || "";
  el.campaignGoalInput.value = state.draftForm.campaignGoal || "";
  el.campaignGeoInput.value = state.draftForm.targetGeography || "";
  el.campaignBudgetInput.value = state.draftForm.monthlyBudget || "";

  if (!state.draftForm.files.length) {
    el.campaignFileList.innerHTML = '<span class="file-chip muted">No files attached</span>';
    return;
  }
  el.campaignFileList.innerHTML = state.draftForm.files
    .map(
      (file, index) => `
        <span class="file-chip">
          <span>${file.name}</span>
          <small>${humanFileSize(file.size)}</small>
          <button class="file-remove" data-file-index="${index}" aria-label="Remove ${file.name}">×</button>
        </span>
      `
    )
    .join("");
}

function renderCampaignEmptyState() {
  const hasDraft = Boolean(state.selectedDraft);
  el.campaignEmptyState.hidden = hasDraft;
  if (hasDraft) {
    el.campaignEmptyState.innerHTML = "";
    return;
  }

  el.campaignEmptyState.innerHTML = `
    <div class="empty-state-block">
      <p class="section-kicker">No pending draft</p>
      <h3>No campaign drafts pending.</h3>
      <p>Drafts are generated automatically when the system recommends a new campaign. You can also build one manually using the intake block above.</p>
    </div>
  `;
}

function renderExplainDrawer() {
  const draft = state.selectedDraft;
  el.campaignExplainDrawer.hidden = !state.explainDrawerOpen;
  if (!state.explainDrawerOpen || !draft) {
    el.campaignExplainBody.innerHTML = "";
    return;
  }
  const explanation = draft.draft?.structure_explanation || {};
  const bullets = explanation.bullets || [];
  el.campaignExplainBody.innerHTML = `
    <div class="drawer-panel">
      <h4>${explanation.title || "Why this draft exists"}</h4>
      <ul class="drawer-list">
        ${bullets.map((item) => `<li>${item}</li>`).join("")}
      </ul>
      <div class="drawer-note">
        <strong>Context summary</strong>
        <p>${draft.context_summary || draft.draft?.kal_note || "No additional context was recorded."}</p>
      </div>
    </div>
  `;
}

function renderCampaignWorkspace() {
  const account = selectedAccount();
  const draft = state.selectedDraft;
  el.campaignPageTitle.textContent = account ? `${account.name} campaign drafts` : "Draft and launch flow";
  renderDraftSelector();
  renderDraftForm();
  renderCampaignEmptyState();
  renderExplainDrawer();

  if (!draft) {
    el.campaignWorkspace.innerHTML = "";
    return;
  }

  const details = draft.draft || {};
  const adGroups = details.ad_groups || [];
  const benchmark = details.benchmark_comparison || {};
  const fileTags = (draft.files || []).map((file) => `<span class="negative-item">${file.filename}</span>`).join("");
  const sharedNegatives = (details.shared_negatives || []).slice(0, 12).map((keyword) => `<span class="negative-item">${keyword}</span>`).join("");
  const sourceMemory = (details.source_context?.memory || []).map((item) => `<div class="context-pill">${item.memory_key}: ${item.memory_value}</div>`).join("");

  el.campaignWorkspace.innerHTML = `
    <div class="campaign-workspace-grid">
      <section class="panel campaign-left-rail">
        <div class="panel-head">
          <div>
            <p class="section-kicker">Draft Structure</p>
            <h3>${details.campaign_name || "Draft campaign"}</h3>
          </div>
          <span class="review-pill">${draft.status.toUpperCase()}</span>
        </div>
        <div class="draft-summary-row">
          <span>${details.methodology || "STAG"} methodology</span>
          <span>${details.vertical_defaults || details.vertical || "General"}</span>
          <span>${details.status_message || "Nothing executes until you approve"}</span>
        </div>
        <div class="ad-group-stack">
          ${adGroups
            .map(
              (group) => `
                <article class="ad-group-card">
                  <div class="alert-top">
                    <h4 class="ad-group-title">${group.ad_group}</h4>
                    <span class="ad-group-meta">${group.keywords.length} keywords · ${formatCurrency(group.monthly_budget)} / mo</span>
                  </div>
                  ${group.keywords
                    .map(
                      (keyword) => `
                        <div class="keyword-item">
                          <span class="mono">${keyword.text}</span>
                          <div class="keyword-row">
                            <span class="match-chip ${statusClass(keyword.match_type)}">${keyword.match_type}</span>
                            <span>${formatCurrency(keyword.max_cpc)}</span>
                          </div>
                        </div>
                      `
                    )
                    .join("")}
                </article>
              `
            )
            .join("")}
        </div>
        <div class="rsa-block">
          <p class="section-kicker">Responsive search ad</p>
          <div class="headline-list">${(details.responsive_search_ad?.headlines || []).map((headline) => `<span class="headline-item">${headline}</span>`).join("")}</div>
          <div class="description-list">${(details.responsive_search_ad?.descriptions || []).map((description) => `<span class="description-item">${description}</span>`).join("")}</div>
          <div class="progress-row">
            <div class="progress-bar"><span style="width:82%"></span></div>
            <strong>${details.responsive_search_ad?.predicted_ad_strength || "Good (82%)"}</strong>
          </div>
        </div>
      </section>

      <aside class="campaign-right-rail">
        <section class="panel settings-card">
          <p class="section-kicker">Campaign settings</p>
          <div class="settings-row"><span>Daily budget</span><strong>${formatCurrency(details.daily_budget)}</strong></div>
          <div class="settings-row"><span>Bidding</span><strong>${details.bid_strategy || "Max Conversions"}</strong></div>
          <div class="settings-row"><span>Target CPA</span><strong>${formatCurrency(details.target_cpa)}</strong></div>
          <div class="settings-row"><span>Geo target</span><strong>${details.target_geography || "Local radius"}</strong></div>
          <div class="settings-row"><span>Network</span><strong>${details.network || "Search only"}</strong></div>
          <div class="settings-row"><span>Ad schedule</span><strong>${details.ad_schedule || "All week"}</strong></div>
          <div class="settings-row"><span>Vertical defaults</span><strong>${details.vertical_defaults || details.vertical || "General"}</strong></div>
        </section>

        <section class="panel settings-card insight-card">
          <p class="section-kicker">Benchmark comparison</p>
          <div class="settings-row"><span>Portfolio avg. CPA</span><strong>${formatCurrency(benchmark.portfolio_avg_cpa)}</strong></div>
          <div class="settings-row"><span>Target CPA this draft</span><strong>${formatCurrency(details.target_cpa)}</strong></div>
          <div class="settings-row"><span>Vertical avg. ROAS</span><strong>${formatNumber(benchmark.vertical_avg_roas, 1)}x</strong></div>
          <div class="settings-row"><span>Predicted ROAS</span><strong>${formatNumber(benchmark.predicted_roas_min, 1)}-${formatNumber(benchmark.predicted_roas_max, 1)}x</strong></div>
          <div class="note-card compact">
            <p>${details.kal_note || details.context_summary || "Review this draft before any launch step."}</p>
          </div>
        </section>

        <section class="panel settings-card">
          <p class="section-kicker">Shared negatives applied</p>
          <div class="negatives-row">${sharedNegatives || '<span class="negative-item">None recorded yet</span>'}</div>
        </section>

        <section class="panel settings-card">
          <p class="section-kicker">Reference context</p>
          <div class="negatives-row">${fileTags || '<span class="negative-item">No files attached</span>'}</div>
          <div class="context-grid">${sourceMemory || '<div class="context-pill">No stored account context</div>'}</div>
        </section>
      </aside>
    </div>

    <section class="panel action-bar-panel">
      <div class="action-bar-main">
        <div>
          <p class="section-kicker">Review status</p>
          <strong>${draft.status === "approved" ? "Campaign queued. Awaiting production push." : "Nothing executes until you approve"}</strong>
        </div>
        <div class="alert-actions wide-actions">
          <button class="primary-btn" data-draft-approve="${draft.id}" ${draft.status === "approved" ? "disabled" : ""}>${draft.status === "approved" ? "Submitted for launch" : "Approve & launch"}</button>
          <button class="ghost-btn" data-draft-modify-toggle="${draft.id}">Request changes</button>
          <button class="ghost-btn" data-draft-explain="${draft.id}">Explain structure</button>
        </div>
      </div>
      ${state.changeRequestOpen ? `
        <div class="change-request-panel">
          <label class="field-group field-span-2">
            <span>Revision request</span>
            <textarea id="changeRequestNote" rows="3" placeholder="Example: Make ad groups tighter around emergency terms and lower budget until QS improves.">${state.changeRequestNote || ""}</textarea>
          </label>
          <div class="alert-actions">
            <button class="primary-btn" data-draft-submit-change="${draft.id}">Send revision</button>
            <button class="ghost-btn" data-draft-cancel-change="${draft.id}">Cancel</button>
          </div>
        </div>
      ` : ""}
    </section>
  `;
}

function render() {
  renderTopBar();
  renderAccountRail();
  renderAlertsHeader();
  renderMetricCards();
  renderAlertFeed();
  renderWeeklyReport();
  renderNotificationTray();
  renderCampaignWorkspace();
}

async function applyDecision(alertId, decision, modifications = {}) {
  const alert = state.alerts.find((item) => Number(item.id) === Number(alertId));
  if (!alert) return;
  const alertIndex = state.alerts.findIndex((item) => Number(item.id) === Number(alertId));
  const previousStatus = state.alerts[alertIndex]?.status;
  if (alertIndex !== -1) {
    state.alerts[alertIndex] = {
      ...state.alerts[alertIndex],
      status: decision === "approve" ? "executed" : decision === "dismiss" ? "dismissed" : state.alerts[alertIndex].status,
    };
  }
  state.pendingDecisions.add(alertId);
  render();
  try {
    await api(`/api/alerts/${alertId}/decision`, {
      method: "POST",
      body: JSON.stringify({ decision, actor: "dashboard_user", modifications }),
    });
    state.pendingDecisions.delete(alertId);
    await Promise.all([loadDashboard(), loadNotifications()]);
    render();
    showFlash(`Alert ${decision === "approve" ? "approved" : decision}.`, "success");
  } catch (error) {
    if (alertIndex !== -1 && previousStatus) {
      state.alerts[alertIndex] = { ...state.alerts[alertIndex], status: previousStatus };
    }
    state.pendingDecisions.delete(alertId);
    render();
    showFlash(error.message, "error");
  }
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  const chunkSize = 0x8000;
  let binary = "";
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return btoa(binary);
}

async function ingestFiles(fileList) {
  const files = Array.from(fileList || []);
  if (!files.length) return;
  const limit = 6 - state.draftForm.files.length;
  const nextFiles = files.slice(0, limit);
  if (!nextFiles.length) {
    showFlash("File limit reached.", "error");
    return;
  }
  const encoded = [];
  for (const file of nextFiles) {
    const buffer = await file.arrayBuffer();
    encoded.push({
      name: file.name,
      type: file.type,
      size: file.size,
      content_base64: arrayBufferToBase64(buffer),
    });
  }
  state.draftForm.files = [...state.draftForm.files, ...encoded];
  el.campaignFilesInput.value = "";
  renderDraftForm();
  showFlash(`${encoded.length} file${encoded.length === 1 ? "" : "s"} added to campaign context.`, "success");
}

async function buildCampaignDraft() {
  const account = selectedAccount();
  if (!account) {
    showFlash("Select an account first.", "error");
    return;
  }
  updateDraftFormFromInputs();
  el.buildCampaignBtn.disabled = true;
  el.buildCampaignBtn.textContent = "Building...";
  try {
    const payload = await api("/api/campaigns/draft", {
      method: "POST",
      body: JSON.stringify({
        account_id: Number(account.id),
        prompt: state.draftForm.prompt,
        campaign_goal: state.draftForm.campaignGoal,
        target_geography: state.draftForm.targetGeography,
        monthly_budget: Number(state.draftForm.monthlyBudget || defaultBudgetForAccount(account)),
        files: state.draftForm.files,
      }),
    });
    state.selectedDraft = payload.draft;
    state.selectedDraftId = Number(payload.draft.id);
    state.changeRequestOpen = false;
    state.changeRequestNote = "";
    await Promise.all([loadDraftsForSelectedAccount(state.selectedDraftId), loadNotifications()]);
    switchPage("campaigns");
    showFlash("Campaign draft built.", "success");
  } catch (error) {
    showFlash(error.message, "error");
  } finally {
    el.buildCampaignBtn.disabled = false;
    el.buildCampaignBtn.textContent = "Build campaign";
    render();
  }
}

async function approveDraft(draftId) {
  if (!window.confirm("This will submit the campaign to Google Ads. Confirm?")) {
    return;
  }
  try {
    const payload = await api(`/api/campaigns/draft/${draftId}/approve`, {
      method: "POST",
      body: JSON.stringify({ actor: "dashboard_user" }),
    });
    state.selectedDraft = payload.draft;
    state.selectedDraftId = Number(payload.draft.id);
    await Promise.all([loadDraftsForSelectedAccount(state.selectedDraftId), loadNotifications()]);
    render();
    showFlash("Campaign queued for launch.", "success");
  } catch (error) {
    showFlash(error.message, "error");
  }
}

async function submitDraftChange(draftId) {
  updateDraftFormFromInputs();
  const noteEl = document.getElementById("changeRequestNote");
  const note = noteEl ? noteEl.value.trim() : state.changeRequestNote.trim();
  if (!note) {
    showFlash("Add a revision note first.", "error");
    return;
  }
  state.changeRequestNote = note;
  try {
    const payload = await api(`/api/campaigns/draft/${draftId}/modify`, {
      method: "POST",
      body: JSON.stringify({
        actor: "dashboard_user",
        note,
        prompt: state.draftForm.prompt,
        campaign_goal: state.draftForm.campaignGoal,
        target_geography: state.draftForm.targetGeography,
        monthly_budget: Number(state.draftForm.monthlyBudget || 0) || undefined,
        files: state.draftForm.files.length ? state.draftForm.files : undefined,
      }),
    });
    state.selectedDraft = payload.draft;
    state.selectedDraftId = Number(payload.draft.id);
    state.changeRequestOpen = false;
    state.changeRequestNote = "";
    await Promise.all([loadDraftsForSelectedAccount(state.selectedDraftId), loadNotifications()]);
    render();
    showFlash("Draft updated with your revision request.", "success");
  } catch (error) {
    showFlash(error.message, "error");
  }
}

async function runMonitoring() {
  el.runMonitoringBtn.disabled = true;
  el.runMonitoringBtn.textContent = "Running...";
  try {
    const account = selectedAccount();
    await api("/api/run-monitoring", {
      method: "POST",
      body: JSON.stringify({ account_id: account ? Number(account.id) : null }),
    });
    await Promise.all([loadDashboard(), loadNotifications()]);
    render();
    showFlash("Monitoring cycle completed.", "success");
  } catch (error) {
    showFlash(error.message, "error");
  } finally {
    el.runMonitoringBtn.disabled = false;
    el.runMonitoringBtn.textContent = "Run now";
  }
}

async function refreshWeeklyReport(forceGenerate = false) {
  el.generateWeeklyBtn.disabled = true;
  try {
    if (forceGenerate || !state.weeklyReport) {
      const payload = await api("/api/reports/weekly/generate", { method: "POST", body: JSON.stringify({}) });
      state.weeklyReport = payload.report;
    } else {
      const payload = await api("/api/reports/weekly/latest");
      state.weeklyReport = payload.report;
    }
    renderWeeklyReport();
    showFlash("Weekly report refreshed.", "success");
  } catch (error) {
    showFlash(error.message, "error");
  } finally {
    el.generateWeeklyBtn.disabled = false;
  }
}

async function calibrateThresholds() {
  el.calibrateThresholdsBtn.disabled = true;
  el.calibrateThresholdsBtn.textContent = "Calibrating...";
  try {
    const payload = await api("/api/thresholds/calibrate", {
      method: "POST",
      body: JSON.stringify({ apply: true }),
    });
    state.thresholds = payload.thresholds || [];
    await loadDashboard();
    render();
    const count = payload.result?.suggestions?.length || 0;
    showFlash(`Calibrated ${count} vertical threshold set${count === 1 ? "" : "s"}.`, "success");
  } catch (error) {
    showFlash(error.message, "error");
  } finally {
    el.calibrateThresholdsBtn.disabled = false;
    el.calibrateThresholdsBtn.textContent = "Calibrate thresholds";
  }
}

async function selectAccount(accountId) {
  state.selectedAccountId = Number(accountId);
  state.selectedAlertId = null;
  state.selectedDraftId = null;
  state.selectedDraft = null;
  state.changeRequestOpen = false;
  state.changeRequestNote = "";
  await Promise.all([loadSelectedAccountDetail(), loadDraftsForSelectedAccount()]);
  const alertsForAccount = accountAlerts(state.selectedAccountId);
  state.selectedAlertId = alertsForAccount[0] ? Number(alertsForAccount[0].id) : null;
  render();
}

async function openNotification(item) {
  if (item.account_id) {
    await selectAccount(item.account_id);
  }
  if (item.kind === "draft" && item.draft_id) {
    state.selectedDraftId = Number(item.draft_id);
    state.selectedDraft = state.draftList.find((draft) => Number(draft.id) === Number(item.draft_id)) || state.selectedDraft;
    if (state.selectedDraft) syncDraftFormFromDraft(state.selectedDraft);
    switchPage("campaigns");
  } else {
    if (item.alert_id) {
      state.selectedAlertId = Number(item.alert_id);
    }
    switchPage("alerts");
  }
  markNotificationsSeen();
}

function markNotificationsSeen() {
  const newest = state.notifications[0]?.created_at || new Date().toISOString();
  state.notificationSeenAt = newest;
  localStorage.setItem("adsGenie.notificationSeenAt", newest);
}

function bindEvents() {
  el.navTabs.forEach((tab) => {
    tab.addEventListener("click", () => switchPage(tab.dataset.page));
  });

  el.accountRail.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-account-id]");
    if (!button) return;
    await selectAccount(button.dataset.accountId);
  });

  el.alertFeed.addEventListener("click", async (event) => {
    const decisionButton = event.target.closest("[data-decision]");
    if (decisionButton) {
      const alertId = Number(decisionButton.dataset.alertId);
      const decision = decisionButton.dataset.decision;
      if (decision === "modify") {
        const note = window.prompt("Describe the change you want applied to this alert action.");
        if (note) await applyDecision(alertId, "modify", { note });
      } else {
        await applyDecision(alertId, decision);
      }
      return;
    }

    const builderButton = event.target.closest("[data-open-builder]");
    if (builderButton) {
      const alertId = Number(builderButton.dataset.openBuilder);
      const alert = state.alerts.find((item) => Number(item.id) === alertId);
      const draftAction = alert?.recommendation?.actions?.find((item) => item.action_type === "draft_campaign");
      if (draftAction) {
        state.draftForm.prompt = alert.summary || draftAction.reason || "";
        state.draftForm.campaignGoal = draftAction.params?.campaign_goal || state.draftForm.campaignGoal;
        state.draftForm.targetGeography = draftAction.params?.target_geography || state.draftForm.targetGeography;
        state.draftForm.monthlyBudget = String(Math.round(Number(draftAction.params?.recommended_monthly_budget || state.draftForm.monthlyBudget || 3000)));
      }
      switchPage("campaigns");
      render();
      return;
    }

    const card = event.target.closest("[data-alert-id]");
    if (!card) return;
    state.selectedAlertId = Number(card.dataset.alertId);
    render();
  });

  el.notificationToggle.addEventListener("click", () => {
    toggleNotificationTray();
  });

  el.closeNotificationTray.addEventListener("click", () => {
    closeNotificationTray({ focusToggle: true });
  });

  el.notificationList.addEventListener("click", async (event) => {
    const card = event.target.closest("[data-notification-id]");
    if (!card) return;
    const item = state.notifications.find((notification) => notification.id === card.dataset.notificationId);
    if (item) await openNotification(item);
  });

  el.runMonitoringBtn.addEventListener("click", runMonitoring);
  el.generateWeeklyBtn.addEventListener("click", () => refreshWeeklyReport(true));
  el.loadWeeklyBtn.addEventListener("click", () => refreshWeeklyReport(false));
  el.calibrateThresholdsBtn.addEventListener("click", calibrateThresholds);
  el.refreshDraftsBtn.addEventListener("click", async () => {
    await Promise.all([loadDraftsForSelectedAccount(state.selectedDraftId), loadNotifications()]);
    render();
  });
  el.buildCampaignBtn.addEventListener("click", buildCampaignDraft);
  el.campaignFilesInput.addEventListener("change", async () => ingestFiles(el.campaignFilesInput.files));
  [el.campaignPromptInput, el.campaignGoalInput, el.campaignGeoInput, el.campaignBudgetInput].forEach((input) => {
    input.addEventListener("input", updateDraftFormFromInputs);
  });
  el.campaignFileList.addEventListener("click", (event) => {
    const removeButton = event.target.closest("[data-file-index]");
    if (!removeButton) return;
    state.draftForm.files.splice(Number(removeButton.dataset.fileIndex), 1);
    renderDraftForm();
  });
  el.draftSelector.addEventListener("change", () => {
    state.selectedDraftId = Number(el.draftSelector.value || 0) || null;
    state.selectedDraft = state.draftList.find((draft) => Number(draft.id) === Number(state.selectedDraftId)) || null;
    state.changeRequestOpen = false;
    state.changeRequestNote = "";
    if (state.selectedDraft) syncDraftFormFromDraft(state.selectedDraft);
    syncRoute();
    render();
  });
  el.campaignWorkspace.addEventListener("click", async (event) => {
    const approveButton = event.target.closest("[data-draft-approve]");
    if (approveButton) {
      await approveDraft(Number(approveButton.dataset.draftApprove));
      return;
    }
    const modifyButton = event.target.closest("[data-draft-modify-toggle]");
    if (modifyButton) {
      state.changeRequestOpen = true;
      render();
      return;
    }
    const explainButton = event.target.closest("[data-draft-explain]");
    if (explainButton) {
      state.explainDrawerOpen = true;
      renderExplainDrawer();
      return;
    }
    const cancelButton = event.target.closest("[data-draft-cancel-change]");
    if (cancelButton) {
      state.changeRequestOpen = false;
      state.changeRequestNote = "";
      render();
      return;
    }
    const submitButton = event.target.closest("[data-draft-submit-change]");
    if (submitButton) {
      await submitDraftChange(Number(submitButton.dataset.draftSubmitChange));
    }
  });
  el.closeExplainDrawer.addEventListener("click", () => {
    state.explainDrawerOpen = false;
    renderExplainDrawer();
  });

  document.addEventListener(
    "pointerdown",
    (event) => {
      if (!state.notificationTrayOpen || isNotificationTrayTarget(event.target)) return;
      closeNotificationTray();
    },
    true
  );

  document.addEventListener("keydown", (event) => {
    if (!state.notificationTrayOpen || event.key !== "Escape") return;
    closeNotificationTray({ focusToggle: true });
  });

  window.addEventListener("popstate", async () => {
    const route = currentRoute();
    state.currentPage = route.page;
    if (route.draftId) {
      try {
        const payload = await api(`/api/campaigns/draft/${route.draftId}`);
        state.selectedAccountId = Number(payload.draft.account_id);
        await Promise.all([loadSelectedAccountDetail(), loadDraftsForSelectedAccount(route.draftId)]);
      } catch (_error) {
        await loadDraftsForSelectedAccount();
      }
    }
    render();
  });
}

async function init() {
  const route = currentRoute();
  state.currentPage = route.page;
  bindEvents();
  await Promise.all([loadHealth(), loadDashboard({ preferredDraftId: route.draftId }), loadNotifications()]);

  if (route.draftId && (!state.selectedDraft || Number(state.selectedDraft.id) !== Number(route.draftId))) {
    const payload = await api(`/api/campaigns/draft/${route.draftId}`).catch(() => null);
    if (payload?.draft) {
      state.selectedAccountId = Number(payload.draft.account_id);
      await Promise.all([loadSelectedAccountDetail(), loadDraftsForSelectedAccount(route.draftId)]);
    }
  }

  if (!state.weeklyReport) await refreshWeeklyReport(true);
  el.shell.setAttribute("data-page", state.currentPage);
  render();
  syncRoute(true);
  syncNotificationTray();
  window.setInterval(async () => {
    await loadNotifications();
    renderTopBar();
    if (state.notificationTrayOpen) renderNotificationTray();
  }, 15000);
}

init().catch((error) => {
  renderErrorCard(el.alertFeed, error.message);
  renderErrorCard(el.campaignWorkspace, error.message);
  showFlash(error.message, "error");
});
