const state = {
  health: null,
  accounts: [],
  alerts: [],
  decisions: [],
  actions: [],
  weeklyReport: null,
  selectedAccountId: null,
  selectedAlertId: null,
  selectedAccountDetail: null,
  draftPreview: null,
  lastSyncAt: null,
};

const el = {
  accountRail: document.getElementById("accountRail"),
  runMonitoringBtn: document.getElementById("runMonitoringBtn"),
  generateDraftBtn: document.getElementById("generateDraftBtn"),
  generateWeeklyBtn: document.getElementById("generateWeeklyBtn"),
  loadWeeklyBtn: document.getElementById("loadWeeklyBtn"),
  refreshSelectedBtn: document.getElementById("refreshSelectedBtn"),
  accountsMonitored: document.getElementById("accountsMonitored"),
  lastSyncLabel: document.getElementById("lastSyncLabel"),
  selectedAccountMeta: document.getElementById("selectedAccountMeta"),
  selectedAccountTitle: document.getElementById("selectedAccountTitle"),
  monitoringStatus: document.getElementById("monitoringStatus"),
  metricCards: document.getElementById("metricCards"),
  queueSummary: document.getElementById("queueSummary"),
  alertFeed: document.getElementById("alertFeed"),
  weeklyReport: document.getElementById("weeklyReport"),
  inspectorTitle: document.getElementById("inspectorTitle"),
  inspectorBody: document.getElementById("inspectorBody"),
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || `Request failed: ${response.status}`);
  }
  return payload;
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
    .sort((a, b) => {
      const order = { open: 0, escalated: 1, executed: 2, dismissed: 3 };
      return (order[a.status] ?? 9) - (order[b.status] ?? 9);
    });
}

function accountActions(accountId) {
  return state.actions.filter((action) => Number(action.account_id) === Number(accountId));
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

function signDelta(current, previous, suffix = "%") {
  const currentValue = Number(current || 0);
  const previousValue = Number(previous || 0);
  if (!previousValue) {
    return `new ${suffix === "x" ? "signal" : "baseline"}`;
  }
  const delta = ((currentValue - previousValue) / Math.abs(previousValue)) * 100;
  const prefix = delta >= 0 ? "+" : "";
  return `${prefix}${delta.toFixed(1)}% WoW`;
}

function timeAgo(value) {
  if (!value) {
    return "just now";
  }
  const date = new Date(value);
  const diffMs = Date.now() - date.getTime();
  const diffMinutes = Math.max(0, Math.round(diffMs / 60000));
  if (diffMinutes < 1) {
    return "just now";
  }
  if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  }
  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  return `${Math.round(diffHours / 24)}d ago`;
}

function statusClass(value) {
  return String(value || "none").toLowerCase();
}

function alertTags(alert) {
  const tags = [];
  const actions = alert.recommendation?.actions || [];
  actions.forEach((action) => {
    if (action.action_type) {
      tags.push(String(action.action_type).replaceAll("_", " "));
    }
    if (Array.isArray(action.params?.keywords)) {
      tags.push(...action.params.keywords.slice(0, 3));
    }
    if (action.params?.campaign_name) {
      tags.push(action.params.campaign_name);
    }
  });

  const rootCauses = alert.context?.roas_drop?.root_causes || [];
  rootCauses.forEach((item) => {
    if (item.cause) {
      tags.push(String(item.cause).replaceAll("_", " "));
    }
  });
  return [...new Set(tags)].slice(0, 6);
}

function primaryAlertAction(alert) {
  return alert?.recommendation?.actions?.[0] || null;
}

function activeInspectorPayload() {
  const alert = selectedAlert();
  const draftFromAlert = alert?.recommendation?.actions?.find((action) => action.action_type === "draft_campaign")?.params;
  if (draftFromAlert) {
    return { mode: "draft", draft: draftFromAlert, alert };
  }
  if (state.draftPreview) {
    return { mode: "draft", draft: state.draftPreview, alert: null };
  }
  if (alert) {
    return { mode: "review", alert };
  }
  return null;
}

async function loadHealth() {
  const payload = await api("/api/health");
  state.health = payload;
}

async function loadDashboard() {
  const [accountsPayload, alertsPayload, decisionsPayload, actionsPayload, weeklyPayload] = await Promise.all([
    api("/api/accounts"),
    api("/api/alerts"),
    api("/api/decisions"),
    api("/api/actions"),
    api("/api/reports/weekly/latest").catch(() => ({ report: null })),
  ]);

  state.accounts = accountsPayload.accounts || [];
  state.alerts = alertsPayload.alerts || [];
  state.decisions = decisionsPayload.decisions || [];
  state.actions = (actionsPayload.actions || []).map((action) => ({
    ...action,
    params: action.params || {},
  }));
  state.weeklyReport = weeklyPayload.report || null;
  state.lastSyncAt = new Date().toISOString();

  if (!state.selectedAccountId && state.accounts.length) {
    state.selectedAccountId = Number(state.accounts[0].id);
  }
  if (state.selectedAccountId) {
    const alertsForAccount = accountAlerts(state.selectedAccountId);
    if (!alertsForAccount.some((alert) => Number(alert.id) === Number(state.selectedAlertId))) {
      state.selectedAlertId = alertsForAccount[0] ? Number(alertsForAccount[0].id) : null;
    }
    await loadSelectedAccountDetail();
  }
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

  el.accountRail.querySelectorAll("[data-account-id]").forEach((button) => {
    button.addEventListener("click", async () => {
      state.selectedAccountId = Number(button.dataset.accountId);
      state.draftPreview = null;
      await loadSelectedAccountDetail();
      const alertsForAccount = accountAlerts(state.selectedAccountId);
      state.selectedAlertId = alertsForAccount[0] ? Number(alertsForAccount[0].id) : null;
      render();
    });
  });
}

function renderHeader() {
  const account = selectedAccount();
  if (!account) {
    el.selectedAccountMeta.textContent = "No account selected";
    el.selectedAccountTitle.textContent = "Ads Genie";
    return;
  }

  const campaigns = state.selectedAccountDetail?.campaigns || [];
  const health = account.health?.metrics || {};
  el.selectedAccountMeta.textContent = `${String(account.vertical).replaceAll("_", " ")} • ${campaigns.length} campaigns • ${formatCurrency(health.cpa_7d)} CPA`;
  el.selectedAccountTitle.textContent = account.name;
  el.monitoringStatus.className = `status-pill ${state.health?.scheduler_enabled ? "live" : "warn"}`;
  el.monitoringStatus.textContent = state.health?.scheduler_enabled ? "Monitoring live" : "Manual mode";
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
          <div class="metric-head">
            <h4>${metric.label}</h4>
          </div>
          <strong>${metric.value}</strong>
          <span class="metric-delta">${metric.delta}</span>
        </article>
      `,
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
    el.alertFeed.innerHTML = '<div class="empty-card">No alerts for this account yet. Run monitoring to generate analysis.</div>';
    return;
  }

  el.alertFeed.innerHTML = alerts
    .map((alert) => {
      const isActive = Number(alert.id) === Number(state.selectedAlertId);
      const action = primaryAlertAction(alert);
      const tags = alertTags(alert)
        .map((tag) => `<span class="alert-chip">${tag}</span>`)
        .join("");
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
            <button class="action-btn primary" data-decision="approve" data-alert-id="${alert.id}">Approve</button>
            <button class="action-btn ghost" data-decision="modify" data-alert-id="${alert.id}">Modify</button>
            <button class="action-btn ghost" data-decision="dismiss" data-alert-id="${alert.id}">Dismiss</button>
          </div>
        </article>
      `;
    })
    .join("");

  el.alertFeed.querySelectorAll(".alert-card").forEach((card) => {
    card.addEventListener("click", () => {
      state.selectedAlertId = Number(card.dataset.alertId);
      render();
    });
  });

  el.alertFeed.querySelectorAll("[data-decision]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const alertId = Number(button.dataset.alertId);
      const decision = button.dataset.decision;
      await applyDecision(alertId, decision);
    });
  });
}

function renderWeeklyReport() {
  if (!state.weeklyReport) {
    el.weeklyReport.innerHTML = '<div class="empty-card">No weekly report generated yet. Click refresh report to create one.</div>';
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

function renderDraftInspector(draft, alert) {
  const canReview = Boolean(alert) && ["open", "escalated"].includes(alert.status);
  const adGroups = draft.ad_groups || [];
  const benchmark = draft.benchmark_comparison || {};
  el.inspectorTitle.textContent = alert ? "Campaign Draft / Action Review" : "Draft Preview";
  el.inspectorBody.innerHTML = `
    <div class="draft-shell">
      <div class="draft-card draft-top">
        <div class="inspector-top">
          <div>
            <h4>${draft.campaign_name || "Draft campaign"}</h4>
            <p>Generated by ${draft.tool || "draft_campaign"} · ${draft.methodology || "STAG"} methodology · ${draft.vertical_defaults || draft.vertical || "general"} defaults</p>
          </div>
          <span class="review-pill">DRAFT · REVIEW REQUIRED</span>
        </div>
      </div>

      <div class="draft-card">
        <p>Autonomy level: Draft & Review — nothing executes until you approve.</p>
      </div>

      <div class="inspector-grid">
        <div class="draft-card">
          <p class="section-kicker">Ad groups — STAG structure (${adGroups.length})</p>
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
                          <span>${keyword.text}</span>
                          <div class="keyword-row">
                            <span class="match-chip ${statusClass(keyword.match_type)}">${keyword.match_type}</span>
                            <span>${formatCurrency(keyword.max_cpc)}</span>
                          </div>
                        </div>
                      `,
                    )
                    .join("")}
                </article>
              `,
            )
            .join("")}

          <div class="rsa-block">
            <p class="section-kicker">Responsive search ad</p>
            <div class="headline-list">
              ${(draft.responsive_search_ad?.headlines || []).map((item) => `<span class="headline-item">${item}</span>`).join("")}
            </div>
            <div class="description-list">
              ${(draft.responsive_search_ad?.descriptions || []).map((item) => `<span class="description-item">${item}</span>`).join("")}
            </div>
            <div class="rsa-meta">
              <span class="account-health-meta">Predicted ad strength</span>
              <strong>${draft.responsive_search_ad?.predicted_ad_strength || "Good"}</strong>
            </div>
          </div>
        </div>

        <div class="draft-shell">
          <div class="settings-card">
            <p class="section-kicker">Campaign settings</p>
            <div class="settings-row"><span>Daily budget</span><strong>${formatCurrency(draft.daily_budget)}</strong></div>
            <div class="settings-row"><span>Bidding</span><strong>${draft.bid_strategy || "Max Conversions"}</strong></div>
            <div class="settings-row"><span>Target CPA</span><strong>${formatCurrency(draft.target_cpa)}</strong></div>
            <div class="settings-row"><span>Geo target</span><strong>${draft.target_geography || "Local radius"}</strong></div>
            <div class="settings-row"><span>Network</span><strong>${draft.network || "Search only"}</strong></div>
            <div class="settings-row"><span>Ad schedule</span><strong>${draft.ad_schedule || "All week"}</strong></div>
            <div class="settings-row"><span>Vertical defaults</span><strong>${draft.vertical_defaults || draft.vertical || "General"}</strong></div>
          </div>

          <div class="settings-card">
            <p class="section-kicker">Benchmark comparison</p>
            <div class="settings-row"><span>Portfolio avg. CPA</span><strong>${formatCurrency(benchmark.portfolio_avg_cpa)}</strong></div>
            <div class="settings-row"><span>Vertical avg. ROAS</span><strong>${formatNumber(benchmark.vertical_avg_roas, 1)}x</strong></div>
            <div class="settings-row"><span>Predicted ROAS</span><strong>${formatNumber(benchmark.predicted_roas_min, 1)}-${formatNumber(benchmark.predicted_roas_max, 1)}x</strong></div>
          </div>

          <div class="note-card">
            <p>${draft.kal_note || "Use the draft as a review artifact before any manual launch."}</p>
          </div>

          <div class="settings-card">
            <p class="section-kicker">Shared negatives applied</p>
            <div class="negatives-row">
              ${(draft.shared_negatives || []).slice(0, 16).map((keyword) => `<span class="negative-item">${keyword}</span>`).join("")}
            </div>
          </div>
        </div>
      </div>

      ${canReview ? `
        <div class="draft-card">
          <div class="alert-actions">
            <button class="action-btn primary" data-inspector-decision="approve" data-alert-id="${alert.id}">Approve</button>
            <button class="action-btn ghost" data-inspector-decision="modify" data-alert-id="${alert.id}">Modify</button>
            <button class="action-btn ghost" data-inspector-decision="dismiss" data-alert-id="${alert.id}">Dismiss</button>
          </div>
        </div>
      ` : ""}
    </div>
  `;

  el.inspectorBody.querySelectorAll("[data-inspector-decision]").forEach((button) => {
    button.addEventListener("click", async () => {
      await applyDecision(Number(button.dataset.alertId), button.dataset.inspectorDecision);
    });
  });
}

function renderReviewInspector(alert) {
  const actionItems = alert.recommendation?.actions || [];
  const context = alert.context || {};
  const health = context.health_check?.metrics || {};
  const waste = context.budget_waste?.components || {};
  const roas = context.roas_drop?.roas || {};
  const account = selectedAccount();
  const negatives = state.selectedAccountDetail?.negatives || [];

  el.inspectorTitle.textContent = "Action Review";
  el.inspectorBody.innerHTML = `
    <div class="review-shell">
      <div class="review-card">
        <div class="inspector-top">
          <div>
            <h4>${alert.title}</h4>
            <p>${alert.summary}</p>
          </div>
          <span class="status-tag ${statusClass(alert.status)}">${alert.status}</span>
        </div>
      </div>

      <div class="inspector-grid">
        <div class="review-card">
          <p class="section-kicker">Recommended actions</p>
          ${actionItems
            .map(
              (item) => `
                <article class="ad-group-card">
                  <div class="alert-top">
                    <h4>${String(item.action_type).replaceAll("_", " ")}</h4>
                    <span class="status-tag open">${item.risk || "review"}</span>
                  </div>
                  <p>${item.reason || "No reason supplied."}</p>
                  <pre>${JSON.stringify(item.params || {}, null, 2)}</pre>
                </article>
              `,
            )
            .join("")}
        </div>

        <div class="draft-shell">
          <div class="context-card">
            <p class="section-kicker">Performance snapshot</p>
            <div class="context-grid">
              <div class="context-pill">ROAS 7D: ${formatNumber(health.roas_7d, 1)}x</div>
              <div class="context-pill">CPA 7D: ${formatCurrency(health.cpa_7d)}</div>
              <div class="context-pill">Wasted spend: ${formatCurrency(waste.estimated_total_waste)}</div>
              <div class="context-pill">ROAS drop: ${formatNumber(roas.drop_pct, 1)}%</div>
            </div>
          </div>

          <div class="settings-card">
            <p class="section-kicker">Existing negatives</p>
            <div class="negatives-row">
              ${negatives.length ? negatives.slice(0, 14).map((item) => `<span class="negative-item">${item.keyword}</span>`).join("") : '<span class="negative-item">None recorded yet</span>'}
            </div>
          </div>

          <div class="draft-card">
            <div class="alert-actions">
              <button class="action-btn primary" data-inspector-decision="approve" data-alert-id="${alert.id}">Approve</button>
              <button class="action-btn ghost" data-inspector-decision="modify" data-alert-id="${alert.id}">Modify</button>
              <button class="action-btn ghost" data-inspector-decision="dismiss" data-alert-id="${alert.id}">Dismiss</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;

  el.inspectorBody.querySelectorAll("[data-inspector-decision]").forEach((button) => {
    button.addEventListener("click", async () => {
      await applyDecision(Number(button.dataset.alertId), button.dataset.inspectorDecision);
    });
  });
}

function renderInspector() {
  const payload = activeInspectorPayload();
  if (!payload) {
    el.inspectorTitle.textContent = "Select an alert or generate a draft";
    el.inspectorBody.innerHTML = `
      <div class="inspector-empty">
        <div>
          <p class="section-kicker">No review item selected</p>
          <p>Choose an account alert from the queue or click Generate draft for the selected account.</p>
        </div>
      </div>
    `;
    return;
  }

  if (payload.mode === "draft") {
    renderDraftInspector(payload.draft, payload.alert);
    return;
  }
  renderReviewInspector(payload.alert);
}

async function applyDecision(alertId, decision) {
  const alert = state.alerts.find((item) => Number(item.id) === Number(alertId));
  if (!alert) {
    return;
  }

  const modifications = {};
  const action = primaryAlertAction(alert);
  if (decision === "modify") {
    if (action?.action_type === "add_negative_keywords") {
      const raw = window.prompt("Override keywords (comma-separated)", (action.params?.keywords || []).join(", "));
      if (raw === null) {
        return;
      }
      modifications.keywords = raw.split(",").map((item) => item.trim()).filter(Boolean);
    } else if (action?.action_type === "adjust_bid") {
      const raw = window.prompt("Enter new bid delta %", String(action.params?.pct_delta ?? -8));
      if (raw === null) {
        return;
      }
      modifications.pct_delta = Number(raw);
    } else {
      const note = window.prompt("Modification note", "Tighten geo, bids, and ad copy before execution.");
      if (note === null) {
        return;
      }
      modifications.note = note;
    }
  }

  await api(`/api/alerts/${alertId}/decision`, {
    method: "POST",
    body: JSON.stringify({
      decision,
      actor: "dashboard_user",
      modifications,
    }),
  });

  state.draftPreview = null;
  await loadDashboard();
  render();
}

async function generateDraft() {
  const account = selectedAccount();
  if (!account) {
    return;
  }
  const defaultBudget = Math.max(2500, Math.round((account.health?.metrics?.spend_7d || 0) * 4.2));
  const payload = await api("/api/tools/run", {
    method: "POST",
    body: JSON.stringify({
      tool_name: "draft_campaign",
      account_id: Number(account.id),
      params: {
        monthly_budget: defaultBudget,
        campaign_goal: "Lead generation",
        target_geography: `${account.name} +25mi`,
      },
    }),
  });
  state.draftPreview = payload.result;
  renderInspector();
}

async function runMonitoring() {
  const account = selectedAccount();
  await api("/api/run-monitoring", {
    method: "POST",
    body: JSON.stringify({ account_id: account ? Number(account.id) : null }),
  });
  await loadDashboard();
  render();
}

async function refreshWeeklyReport(forceGenerate = false) {
  if (forceGenerate || !state.weeklyReport) {
    const payload = await api("/api/reports/weekly/generate", { method: "POST", body: JSON.stringify({}) });
    state.weeklyReport = payload.report;
  } else {
    const payload = await api("/api/reports/weekly/latest");
    state.weeklyReport = payload.report;
  }
  renderWeeklyReport();
}

function render() {
  renderAccountRail();
  renderHeader();
  renderMetricCards();
  renderAlertFeed();
  renderWeeklyReport();
  renderInspector();
}

function bindEvents() {
  el.runMonitoringBtn.addEventListener("click", runMonitoring);
  el.generateDraftBtn.addEventListener("click", generateDraft);
  el.generateWeeklyBtn.addEventListener("click", () => refreshWeeklyReport(true));
  el.loadWeeklyBtn.addEventListener("click", () => refreshWeeklyReport(false));
  el.refreshSelectedBtn.addEventListener("click", async () => {
    await loadDashboard();
    render();
  });
}

async function init() {
  bindEvents();
  await Promise.all([loadHealth(), loadDashboard()]);
  if (!state.weeklyReport) {
    await refreshWeeklyReport(true);
  }
  render();
}

init().catch((error) => {
  el.alertFeed.innerHTML = `<div class="empty-card">${error.message}</div>`;
  el.inspectorBody.innerHTML = `<div class="empty-card">${error.message}</div>`;
});
