/* eslint-disable max-lines */
function isoNow() {
  return new Date().toISOString();
}

function buildWeeklyReportContent(generatedAt) {
  const dateLabel = new Date(generatedAt).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  return [
    `# Weekly MCC Report (${dateLabel})`,
    "",
    "- Accounts reviewed: 3",
    "- High/Critical health flags: 2",
    "- Critical health flags: 1",
    "- Top issue: ROAS drop in one service account",
    "- Recommended next step: tighten negatives and lower CPA target by 8%",
  ].join("\n");
}

function createAdGroups(goal, budget) {
  const monthlyBudget = Number(budget || 3000);
  const perGroupBudget = Math.max(12, Math.round((monthlyBudget / 30) / 3));
  const cpc = Math.max(2.2, Math.round((monthlyBudget / 900) * 100) / 100);
  return [
    {
      ad_group: `${goal} Core`,
      monthly_budget: Math.round(monthlyBudget * 0.45),
      keywords: [
        { text: `[${goal.toLowerCase()} services]`, match_type: "exact", max_cpc: cpc },
        { text: `"${goal.toLowerCase()} near me"`, match_type: "phrase", max_cpc: Math.max(2.1, cpc - 0.2) },
        { text: `[best ${goal.toLowerCase()}]`, match_type: "exact", max_cpc: cpc + 0.25 },
      ],
    },
    {
      ad_group: `${goal} Urgent`,
      monthly_budget: Math.round(monthlyBudget * 0.35),
      keywords: [
        { text: `"urgent ${goal.toLowerCase()}"`, match_type: "phrase", max_cpc: cpc + 0.35 },
        { text: `"same day ${goal.toLowerCase()}"`, match_type: "phrase", max_cpc: cpc + 0.4 },
        { text: `[24 hour ${goal.toLowerCase()}]`, match_type: "exact", max_cpc: cpc + 0.5 },
      ],
    },
    {
      ad_group: `${goal} Qualified`,
      monthly_budget: Math.round(monthlyBudget * 0.2),
      keywords: [
        { text: `"licensed ${goal.toLowerCase()}"`, match_type: "phrase", max_cpc: Math.max(1.9, cpc - 0.35) },
        { text: `"trusted ${goal.toLowerCase()} provider"`, match_type: "phrase", max_cpc: Math.max(1.8, cpc - 0.45) },
        { text: `"top rated ${goal.toLowerCase()} company"`, match_type: "phrase", max_cpc: Math.max(1.75, cpc - 0.5) },
      ],
    },
  ].map((group) => ({
    ...group,
    daily_budget: perGroupBudget,
  }));
}

function buildDraftStructure(input) {
  const campaignGoal = String(input.campaign_goal || "Lead generation").trim() || "Lead generation";
  const monthlyBudget = Math.max(500, Number(input.monthly_budget || 3000) || 3000);
  const files = Array.isArray(input.files) ? input.files : [];
  const fileNames = files.map((file) => String(file.name || "context.txt")).slice(0, 6);
  const adGroups = createAdGroups(campaignGoal, monthlyBudget);
  const predictedMin = Number((2.4 + Math.min(1.1, monthlyBudget / 9000)).toFixed(1));
  const predictedMax = Number((predictedMin + 1.1).toFixed(1));
  return {
    campaign_name: `${campaignGoal} - ${String(input.target_geography || "Local radius +25mi").trim() || "Local radius +25mi"}`,
    methodology: "STAG",
    status_message: "Nothing executes until you approve",
    vertical_defaults: "service_local",
    daily_budget: Math.max(16, Math.round(monthlyBudget / 30)),
    bid_strategy: "Maximize Conversions",
    target_cpa: Number((monthlyBudget / 42).toFixed(2)),
    target_geography: String(input.target_geography || "Local radius +25mi"),
    network: "Search only",
    ad_schedule: "Mon-Sun 06:00-22:00",
    ad_groups: adGroups,
    responsive_search_ad: {
      headlines: [
        `${campaignGoal} Experts`,
        "Same-Day Availability",
        "Book in Minutes",
        "Trusted Local Team",
        "Transparent Pricing",
      ],
      descriptions: [
        "Get qualified leads with intent-focused keywords and location controls.",
        "Built from your brief with guardrails for budget and conversion quality.",
      ],
      predicted_ad_strength: "Good (82%)",
    },
    benchmark_comparison: {
      portfolio_avg_cpa: 92,
      vertical_avg_roas: 2.8,
      predicted_roas_min: predictedMin,
      predicted_roas_max: predictedMax,
    },
    shared_negatives: [
      "free",
      "jobs",
      "internship",
      "course",
      "template",
      "salary",
      "pdf",
      "youtube",
      "reddit",
      "cheap",
    ],
    source_context: {
      memory: [
        { memory_key: "lead_quality_priority", memory_value: "high" },
        { memory_key: "brand_tone", memory_value: "premium_clear" },
      ],
    },
    structure_explanation: {
      title: "Draft architecture rationale",
      bullets: [
        "Split intent by urgency and qualification to tighten query-theme matching.",
        "Bias budget to core and urgent groups for faster signal collection.",
        "Apply shared negatives to reduce low-intent and research traffic.",
      ],
    },
    kal_note: fileNames.length
      ? `Ingested ${fileNames.length} context file(s): ${fileNames.join(", ")}.`
      : "Built from operator prompt and account baseline.",
  };
}

function seedState() {
  const now = isoNow();
  const weeklyReport = {
    id: 1,
    report_type: "weekly_mcc",
    period_start: "2026-03-13T00:00:00Z",
    period_end: "2026-03-20T00:00:00Z",
    content_markdown: buildWeeklyReportContent(now),
    generated_at: now,
  };

  const accounts = [
    {
      id: 1,
      name: "North Star Dental",
      customer_id: "111-222-3333",
      vertical: "dental",
      timezone: "America/Toronto",
      status: "active",
      data_source: "demo",
      created_at: now,
      updated_at: now,
      health: {
        severity: "warning",
        benchmark: { roas_healthy: 3.2, cpa_target: 88, quality_score_min: 7.1 },
        metrics: {
          spend_7d: 1920,
          spend_prev_7d: 1710,
          conversions_7d: 24,
          conversions_prev_7d: 23,
          cpa_7d: 80,
          roas_7d: 3.1,
          roas_prev_7d: 3.4,
        },
      },
      waste: {
        components: {
          estimated_total_waste: 462,
          waste_ratio: 0.24,
        },
      },
    },
    {
      id: 2,
      name: "Aurora Legal Group",
      customer_id: "444-555-6666",
      vertical: "legal",
      timezone: "America/Toronto",
      status: "active",
      data_source: "demo",
      created_at: now,
      updated_at: now,
      health: {
        severity: "high",
        benchmark: { roas_healthy: 2.9, cpa_target: 120, quality_score_min: 6.8 },
        metrics: {
          spend_7d: 2870,
          spend_prev_7d: 2440,
          conversions_7d: 19,
          conversions_prev_7d: 22,
          cpa_7d: 151,
          roas_7d: 2.3,
          roas_prev_7d: 3.0,
        },
      },
      waste: {
        components: {
          estimated_total_waste: 812,
          waste_ratio: 0.28,
        },
      },
    },
    {
      id: 3,
      name: "Peak HVAC Services",
      customer_id: "777-888-9999",
      vertical: "home_services",
      timezone: "America/Toronto",
      status: "active",
      data_source: "demo",
      created_at: now,
      updated_at: now,
      health: {
        severity: "healthy",
        benchmark: { roas_healthy: 3.0, cpa_target: 96, quality_score_min: 7.0 },
        metrics: {
          spend_7d: 1630,
          spend_prev_7d: 1585,
          conversions_7d: 21,
          conversions_prev_7d: 20,
          cpa_7d: 77,
          roas_7d: 3.6,
          roas_prev_7d: 3.3,
        },
      },
      waste: {
        components: {
          estimated_total_waste: 318,
          waste_ratio: 0.19,
        },
      },
    },
  ];

  const accountDetails = {
    1: {
      campaigns: [{ id: 1, name: "Emergency Dentist Downtown" }, { id: 2, name: "Implants High Intent" }],
      context_memory: [
        { id: 1, account_id: 1, memory_key: "primary_offer", memory_value: "Emergency same-day slots", updated_at: now },
        { id: 2, account_id: 1, memory_key: "quiet_hours", memory_value: "22:00-06:00", updated_at: now },
      ],
      negatives: [{ id: 1, account_id: 1, keyword: "free", source: "shared", created_at: now }],
    },
    2: {
      campaigns: [{ id: 3, name: "Personal Injury Search" }, { id: 4, name: "Family Law Leads" }],
      context_memory: [{ id: 3, account_id: 2, memory_key: "intake_speed", memory_value: "under_5_min", updated_at: now }],
      negatives: [{ id: 2, account_id: 2, keyword: "template", source: "shared", created_at: now }],
    },
    3: {
      campaigns: [{ id: 5, name: "Furnace Repair Local" }, { id: 6, name: "AC Tune-Up Seasonal" }],
      context_memory: [{ id: 4, account_id: 3, memory_key: "service_radius", memory_value: "30mi", updated_at: now }],
      negatives: [{ id: 3, account_id: 3, keyword: "diy", source: "shared", created_at: now }],
    },
  };

  const alerts = [
    {
      id: 101,
      account_id: 1,
      alert_type: "search_terms_waste",
      severity: "high",
      status: "open",
      autonomy_level: "propose_and_wait",
      requires_approval: 1,
      title: "High waste on broad informational terms",
      summary: "Budget leakage detected on low-intent queries over the last 7 days.",
      recommendation: {
        actions: [
          {
            action_type: "add_negative_keywords",
            reason: "Block non-commercial search terms from consuming budget.",
            params: { keywords: ["what is", "wiki", "salary", "free"] },
          },
        ],
      },
      context: { waste_ratio: 0.31 },
      created_at: now,
      updated_at: now,
    },
    {
      id: 102,
      account_id: 2,
      alert_type: "roas_drop",
      severity: "critical",
      status: "escalated",
      autonomy_level: "escalate",
      requires_approval: 1,
      title: "ROAS dropped 22% week-over-week",
      summary: "One top ad group deteriorated while CPC rose on generic legal terms.",
      recommendation: {
        actions: [
          {
            action_type: "adjust_bid",
            reason: "Lower bids on unstable broad groups and protect best-converting ad groups.",
            params: { campaign_name: "Personal Injury Search", bid_modifier: 0.88 },
          },
        ],
      },
      context: { roas_drop: { pct: 22.1, root_causes: [{ cause: "cpc_inflation" }, { cause: "query_broadening" }] } },
      created_at: now,
      updated_at: now,
    },
    {
      id: 103,
      account_id: 3,
      alert_type: "expansion_opportunity",
      severity: "info",
      status: "open",
      autonomy_level: "draft_and_review",
      requires_approval: 1,
      title: "Seasonal opportunity for emergency AC demand",
      summary: "Demand indicators suggest launching a high-intent emergency AC draft campaign.",
      recommendation: {
        actions: [
          {
            action_type: "draft_campaign",
            reason: "Create a controlled campaign draft to capture seasonal high-intent traffic.",
            params: {
              campaign_goal: "Emergency AC leads",
              target_geography: "Toronto +30mi",
              recommended_monthly_budget: 3600,
            },
          },
        ],
      },
      context: { trend: "upward" },
      created_at: now,
      updated_at: now,
    },
  ];

  const actions = [
    {
      id: 201,
      alert_id: 99,
      account_id: 1,
      action_type: "add_negative_keywords",
      status: "executed",
      params: { keywords: ["jobs", "training"] },
      created_at: now,
      executed_at: now,
    },
  ];

  const decisions = [
    {
      id: 301,
      alert_id: 99,
      account_id: 1,
      actor: "dashboard_user",
      action: "approve",
      payload: { action_id: 201 },
      created_at: now,
    },
  ];

  const draftInput = {
    campaign_goal: "Emergency AC leads",
    target_geography: "Toronto +30mi",
    monthly_budget: 3600,
    files: [],
  };
  const firstDraft = {
    id: 401,
    account_id: 3,
    status: "draft",
    prompt_text: "Focus on urgent residential breakdown terms and preserve premium tone.",
    campaign_goal: draftInput.campaign_goal,
    target_geography: draftInput.target_geography,
    monthly_budget: draftInput.monthly_budget,
    context_summary: "Seasonal demand spike observed in account trendline.",
    review_note: null,
    draft: buildDraftStructure(draftInput),
    files: [],
    created_at: now,
    updated_at: now,
  };

  const notifications = [
    {
      id: "n-1",
      kind: "alert",
      severity: "critical",
      title: "ROAS drop escalation",
      body: "Aurora Legal Group needs manual review.",
      account_id: 2,
      alert_id: 102,
      created_at: now,
    },
    {
      id: "n-2",
      kind: "draft",
      severity: "info",
      title: "Campaign draft ready",
      body: "Peak HVAC Services has a new draft campaign ready for approval.",
      account_id: 3,
      draft_id: 401,
      created_at: now,
    },
  ];

  return {
    nextAlertId: 104,
    nextActionId: 202,
    nextDecisionId: 302,
    nextDraftId: 402,
    nextNotificationSeq: 3,
    accounts,
    accountDetails,
    alerts,
    actions,
    decisions,
    drafts: [firstDraft],
    thresholds: [
      { vertical: "dental", roas_healthy: 3.2, cpa_target: 88, quality_score_min: 7.1, source: "calibrated" },
      { vertical: "legal", roas_healthy: 2.9, cpa_target: 120, quality_score_min: 6.8, source: "calibrated" },
      { vertical: "home_services", roas_healthy: 3.0, cpa_target: 96, quality_score_min: 7.0, source: "calibrated" },
    ],
    weeklyReport,
    notifications,
    lastMonitoringRunAt: now,
  };
}

function getState() {
  if (!globalThis.__adsGenieDemoState) {
    globalThis.__adsGenieDemoState = seedState();
  }
  return globalThis.__adsGenieDemoState;
}

function requestId() {
  return Math.random().toString(16).slice(2, 14);
}

function sendJson(res, statusCode, payload) {
  res.status(statusCode).setHeader("Content-Type", "application/json; charset=utf-8");
  res.send(JSON.stringify(payload));
}

function ok(res, payload) {
  sendJson(res, 200, { ok: true, request_id: requestId(), ...payload });
}

function fail(res, statusCode, error) {
  sendJson(res, statusCode, { ok: false, request_id: requestId(), error: String(error || "Request failed") });
}

function parseJsonBody(req) {
  if (req.body && typeof req.body === "object") return Promise.resolve(req.body);
  if (typeof req.body === "string" && req.body.trim()) {
    try {
      return Promise.resolve(JSON.parse(req.body));
    } catch (_error) {
      return Promise.resolve({});
    }
  }
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      if (!chunks.length) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf-8") || "{}"));
      } catch (_error) {
        resolve({});
      }
    });
  });
}

function routeParts(req) {
  const raw = req.query && req.query.route;
  if (Array.isArray(raw)) return raw.filter(Boolean);
  if (typeof raw === "string" && raw) return raw.split("/").filter(Boolean);
  const fromUrl = String(req.url || "").split("?")[0].replace(/^\/api\/?/, "");
  return fromUrl.split("/").filter(Boolean);
}

function parseLimit(req) {
  const value = req.query && req.query.limit;
  const limit = Number(Array.isArray(value) ? value[0] : value || 20);
  if (Number.isNaN(limit)) return 20;
  return Math.max(1, Math.min(50, limit));
}

function addNotification(state, item) {
  const id = `n-${state.nextNotificationSeq++}`;
  const notification = { id, created_at: isoNow(), ...item };
  state.notifications.unshift(notification);
  state.notifications = state.notifications.slice(0, 60);
}

function formatSettingValue(key, value) {
  if (value === null || value === undefined || value === "") return "-";
  if (key === "daily_budget" || key === "target_cpa") {
    const num = Number(value);
    if (Number.isFinite(num)) return `$${num.toFixed(num >= 100 ? 0 : 2)}`;
  }
  if (key === "predicted_roas") {
    const min = Number(value && value.min);
    const max = Number(value && value.max);
    if (Number.isFinite(min) && Number.isFinite(max)) return `${min.toFixed(1)}-${max.toFixed(1)}x`;
  }
  return String(value);
}

function parseDraftChangeNote(note) {
  const text = String(note || "").toLowerCase();
  if (!text) return {};
  const overrides = {};
  const dailyMatch = text.match(/\$?\s*(\d+(?:\.\d+)?)\s*(?:\/\s*day|per\s*day|a\s*day|daily)\b/);
  if (dailyMatch) {
    const daily = Number(dailyMatch[1]);
    if (Number.isFinite(daily) && daily > 0) {
      overrides.monthly_budget = Math.round(daily * 30.4);
    }
  }
  const monthlyMatch = text.match(/\$?\s*(\d+(?:\.\d+)?)\s*(?:\/\s*month|per\s*month|a\s*month|monthly)\b/);
  if (!overrides.monthly_budget && monthlyMatch) {
    const monthly = Number(monthlyMatch[1]);
    if (Number.isFinite(monthly) && monthly > 0) {
      overrides.monthly_budget = Math.round(monthly);
    }
  }
  const cpaMatch = text.match(/(?:target\s*cpa|cpa)\s*(?:to|=|:)?\s*\$?\s*(\d+(?:\.\d+)?)/);
  if (cpaMatch) {
    const cpa = Number(cpaMatch[1]);
    if (Number.isFinite(cpa) && cpa > 0) {
      overrides.target_cpa = Number(cpa.toFixed(2));
    }
  }
  const geoMatch = text.match(/(?:geo(?:\s*target)?|target\s*geography|location)\s*(?:to|=|:)?\s*([a-z0-9+,\-\s]{3,60})/i);
  if (geoMatch) {
    const geography = String(geoMatch[1]).trim().replace(/\s{2,}/g, " ");
    if (geography.length >= 3) {
      overrides.target_geography = geography;
    }
  }
  return overrides;
}

function buildChangeSummary(previousDraft, nextDraft, note) {
  const before = previousDraft && previousDraft.draft ? previousDraft.draft : {};
  const after = nextDraft && nextDraft.draft ? nextDraft.draft : {};
  const fields = [
    { key: "daily_budget", label: "Daily budget" },
    { key: "target_cpa", label: "Target CPA" },
    { key: "target_geography", label: "Geo target" },
    { key: "bid_strategy", label: "Bidding" },
    { key: "network", label: "Network" },
    { key: "ad_schedule", label: "Ad schedule" },
    { key: "vertical_defaults", label: "Vertical defaults" },
  ];
  const items = [];
  fields.forEach((field) => {
    const beforeValue = before[field.key];
    const afterValue = after[field.key];
    if (String(beforeValue ?? "") === String(afterValue ?? "")) return;
    items.push({
      key: field.key,
      label: field.label,
      before: formatSettingValue(field.key, beforeValue),
      after: formatSettingValue(field.key, afterValue),
    });
  });
  const beforeMin = Number(before.benchmark_comparison && before.benchmark_comparison.predicted_roas_min);
  const beforeMax = Number(before.benchmark_comparison && before.benchmark_comparison.predicted_roas_max);
  const afterMin = Number(after.benchmark_comparison && after.benchmark_comparison.predicted_roas_min);
  const afterMax = Number(after.benchmark_comparison && after.benchmark_comparison.predicted_roas_max);
  if (
    Number.isFinite(beforeMin) && Number.isFinite(beforeMax) && Number.isFinite(afterMin) && Number.isFinite(afterMax)
    && (beforeMin !== afterMin || beforeMax !== afterMax)
  ) {
    items.push({
      key: "predicted_roas",
      label: "Predicted ROAS",
      before: formatSettingValue("predicted_roas", { min: beforeMin, max: beforeMax }),
      after: formatSettingValue("predicted_roas", { min: afterMin, max: afterMax }),
    });
  }
  return {
    generated_at: isoNow(),
    instruction_note: String(note || ""),
    item_count: items.length,
    items,
  };
}

function listDraftsForAccount(state, accountId) {
  return state.drafts
    .filter((draft) => Number(draft.account_id) === Number(accountId))
    .sort((left, right) => String(right.updated_at).localeCompare(String(left.updated_at)));
}

function getAccountById(state, accountId) {
  return state.accounts.find((account) => Number(account.id) === Number(accountId)) || null;
}

function createDraftFromInput(state, input, existingDraftId) {
  const account = getAccountById(state, input.account_id);
  if (!account) {
    throw new Error("Unknown account");
  }

  const now = isoNow();
  const files = Array.isArray(input.files) ? input.files.slice(0, 6) : [];
  const normalizedGoal = String(input.campaign_goal || "Lead generation").trim() || "Lead generation";
  const normalizedGeo = String(input.target_geography || `${account.name} +25mi`).trim() || `${account.name} +25mi`;
  const normalizedBudget = Math.max(500, Number(input.monthly_budget || 3000) || 3000);
  const normalizedPrompt = String(input.prompt || "").trim();
  const targetCpaOverride = Number(input.target_cpa_override || 0) || null;

  const base = {
    id: existingDraftId || state.nextDraftId++,
    account_id: Number(account.id),
    status: "draft",
    prompt_text: normalizedPrompt,
    campaign_goal: normalizedGoal,
    target_geography: normalizedGeo,
    monthly_budget: normalizedBudget,
    context_summary:
      normalizedPrompt ||
      (files.length
        ? `Added ${files.length} context file(s): ${files.map((file) => file.name).join(", ")}.`
        : "Generated from live account context and portfolio benchmarks."),
    review_note: input.review_note || null,
    draft: buildDraftStructure({
      campaign_goal: normalizedGoal,
      target_geography: normalizedGeo,
      monthly_budget: normalizedBudget,
      files,
    }),
    delivery: {
      pipeline_mode: "demo_simulated",
      destination: {
        type: "google_ads",
        account_name: account.name,
        customer_id: account.customer_id,
        live_enabled: false,
        label: `${account.name} (${account.customer_id})`,
      },
    },
    execution: null,
    change_summary: null,
    files: files.map((file) => ({
      filename: String(file.name || "context.txt"),
      content_type: String(file.type || "application/octet-stream"),
      size_bytes: Number(file.size || 0),
      excerpt: "",
    })),
    created_at: now,
    updated_at: now,
  };

  if (targetCpaOverride && Number.isFinite(targetCpaOverride)) {
    base.draft.target_cpa = Number(targetCpaOverride.toFixed(2));
    if (base.draft.benchmark_comparison) {
      base.draft.benchmark_comparison.predicted_roas_min = Number(Math.max(1.1, base.draft.target_cpa / 40).toFixed(1));
      base.draft.benchmark_comparison.predicted_roas_max = Number((base.draft.benchmark_comparison.predicted_roas_min + 1.1).toFixed(1));
    }
  }

  return base;
}

function handleGet(req, res, state, parts) {
  if (!parts.length || parts[0] === "health") {
    ok(res, {
      mode: "demo",
      environment: "vercel-demo",
      google_ads_configured: false,
      slack_configured: false,
      auth_configured: false,
      auth_required: false,
      scheduler_enabled: false,
      monitor_interval_seconds: 900,
      runtime: {
        event_count: 12,
        top_categories: [
          ["monitoring", 4],
          ["alerts", 5],
          ["campaign_drafts", 3],
        ],
      },
    });
    return;
  }

  if (parts[0] === "notifications") {
    ok(res, { notifications: state.notifications.slice(0, parseLimit(req)) });
    return;
  }

  if (parts[0] === "accounts") {
    if (parts.length === 1) {
      ok(res, { accounts: state.accounts });
      return;
    }
    if (parts.length === 2) {
      const accountId = Number(parts[1]);
      const account = getAccountById(state, accountId);
      if (!account) {
        fail(res, 404, "Not Found");
        return;
      }
      const detail = state.accountDetails[accountId] || { campaigns: [], context_memory: [], negatives: [] };
      ok(res, {
        account,
        campaigns: detail.campaigns,
        context_memory: detail.context_memory,
        negatives: detail.negatives,
      });
      return;
    }
    if (parts.length === 3 && parts[2] === "campaign-drafts") {
      const accountId = Number(parts[1]);
      ok(res, { drafts: listDraftsForAccount(state, accountId) });
      return;
    }
  }

  if (parts[0] === "alerts") {
    ok(res, { alerts: state.alerts });
    return;
  }

  if (parts[0] === "decisions") {
    ok(res, { decisions: state.decisions });
    return;
  }

  if (parts[0] === "actions") {
    ok(res, { actions: state.actions });
    return;
  }

  if (parts[0] === "reports" && parts[1] === "weekly" && parts[2] === "latest") {
    ok(res, { report: state.weeklyReport });
    return;
  }

  if (parts[0] === "thresholds") {
    ok(res, { thresholds: state.thresholds });
    return;
  }

  if (parts[0] === "campaigns" && parts[1] === "draft" && parts.length === 3) {
    const draftId = Number(parts[2]);
    const draft = state.drafts.find((item) => Number(item.id) === draftId);
    if (!draft) {
      fail(res, 404, "Not Found");
      return;
    }
    ok(res, { draft });
    return;
  }

  fail(res, 404, "Not Found");
}

async function handlePost(req, res, state, parts) {
  if (parts[0] === "campaigns" && parts[1] === "draft" && parts.length === 2) {
    try {
      const body = await parseJsonBody(req);
      const draft = createDraftFromInput(state, body);
      state.drafts.unshift(draft);
      addNotification(state, {
        kind: "draft",
        severity: "info",
        title: "Campaign draft built",
        body: `Draft #${draft.id} is ready for review.`,
        account_id: draft.account_id,
        draft_id: draft.id,
      });
      ok(res, { draft });
    } catch (error) {
      fail(res, 400, error.message || "Failed to build draft");
    }
    return;
  }

  if (parts[0] === "campaigns" && parts[1] === "draft" && parts[3] === "approve") {
    const draftId = Number(parts[2]);
    const draft = state.drafts.find((item) => Number(item.id) === draftId);
    if (!draft) {
      fail(res, 404, "Not Found");
      return;
    }
    draft.status = "approved";
    draft.updated_at = isoNow();
    const account = getAccountById(state, draft.account_id);
    const executionArn = `arn:aws:states:demo:000000000000:execution:ads-genie:draft-${draft.id}`;
    const timelineAt = draft.updated_at;
    draft.execution = {
      pipeline_mode: "demo_simulated",
      execution_arn: executionArn,
      current_state: "simulated_complete",
      destination: {
        type: "google_ads",
        account_name: account ? account.name : "Selected account",
        customer_id: account ? account.customer_id : "",
        live_enabled: false,
        label: account ? `${account.name} (${account.customer_id})` : "Selected account",
      },
      timeline: [
        { state: "queued", at: timelineAt, detail: "Approval captured in demo environment." },
        { state: "running_simulation", at: timelineAt, detail: "Simulating production workflow and Google Ads push." },
        { state: "simulated_complete", at: timelineAt, detail: "Simulation complete. No live account mutation performed." },
      ],
    };
    if (draft.draft) {
      draft.draft.status_message = "Simulation complete in demo mode. No live account mutation performed.";
    }
    const action = {
      id: state.nextActionId++,
      alert_id: null,
      account_id: draft.account_id,
      action_type: "launch_campaign_draft",
      status: "executed",
      params: { draft_id: draft.id, campaign_name: draft.draft.campaign_name },
      created_at: draft.updated_at,
      executed_at: draft.updated_at,
    };
    state.actions.unshift(action);
    state.decisions.unshift({
      id: state.nextDecisionId++,
      alert_id: null,
      account_id: draft.account_id,
      actor: "dashboard_user",
      action: "approve_campaign_draft",
      payload: { draft_id: draft.id },
      created_at: draft.updated_at,
    });
    addNotification(state, {
      kind: "system",
      severity: "info",
      title: "Draft simulation complete",
      body: `Draft #${draft.id} was approved and simulated against ${account ? account.customer_id : "selected account"}.`,
      account_id: draft.account_id,
      draft_id: draft.id,
    });
    ok(res, {
      execution_arn: executionArn,
      draft,
    });
    return;
  }

  if (parts[0] === "campaigns" && parts[1] === "draft" && parts[3] === "modify") {
    const draftId = Number(parts[2]);
    const draftIndex = state.drafts.findIndex((item) => Number(item.id) === draftId);
    if (draftIndex < 0) {
      fail(res, 404, "Not Found");
      return;
    }
    const body = await parseJsonBody(req);
    const note = String(body.note || "").trim();
    if (!note) {
      fail(res, 400, "A revision note is required");
      return;
    }
    const existing = state.drafts[draftIndex];
    const noteOverrides = parseDraftChangeNote(note);
    const explicitMonthlyBudget = Number(body.monthly_budget || 0) || null;
    const effectiveMonthlyBudget = explicitMonthlyBudget || noteOverrides.monthly_budget || existing.monthly_budget;
    const effectiveTargetGeography = body.target_geography || noteOverrides.target_geography || existing.target_geography;
    const effectiveTargetCpa = Number(body.target_cpa || 0) || noteOverrides.target_cpa || null;
    const updatedDraft = createDraftFromInput(
      state,
      {
        account_id: existing.account_id,
        prompt: body.prompt || existing.prompt_text,
        campaign_goal: body.campaign_goal || existing.campaign_goal,
        target_geography: effectiveTargetGeography,
        monthly_budget: effectiveMonthlyBudget,
        target_cpa_override: effectiveTargetCpa,
        files: Array.isArray(body.files) && body.files.length ? body.files : existing.files.map((file) => ({
          name: file.filename,
          type: file.content_type,
          size: file.size_bytes,
        })),
        review_note: note,
      },
      existing.id
    );
    updatedDraft.created_at = existing.created_at;
    updatedDraft.execution = null;
    updatedDraft.change_summary = buildChangeSummary(existing, updatedDraft, note);
    if (updatedDraft.draft) {
      updatedDraft.draft.status_message =
        updatedDraft.change_summary.item_count > 0
          ? `Simulated update applied (${updatedDraft.change_summary.item_count} setting ${updatedDraft.change_summary.item_count === 1 ? "change" : "changes"}).`
          : "Draft regenerated from your revision request.";
    }
    state.drafts[draftIndex] = updatedDraft;
    state.decisions.unshift({
      id: state.nextDecisionId++,
      alert_id: null,
      account_id: updatedDraft.account_id,
      actor: String(body.actor || "dashboard_user"),
      action: "modify_campaign_draft",
      payload: { draft_id: updatedDraft.id, note },
      created_at: updatedDraft.updated_at,
    });
    addNotification(state, {
      kind: "draft",
      severity: "warning",
      title: "Draft updated",
      body:
        updatedDraft.change_summary && updatedDraft.change_summary.item_count
          ? `Revision applied to draft #${updatedDraft.id}: ${updatedDraft.change_summary.item_count} setting ${updatedDraft.change_summary.item_count === 1 ? "change" : "changes"}.`
          : `Revision applied to draft #${updatedDraft.id}.`,
      account_id: updatedDraft.account_id,
      draft_id: updatedDraft.id,
    });
    ok(res, { draft: updatedDraft });
    return;
  }

  if (parts[0] === "alerts" && parts[2] === "decision") {
    const alertId = Number(parts[1]);
    const alert = state.alerts.find((item) => Number(item.id) === alertId);
    if (!alert) {
      fail(res, 404, "Not Found");
      return;
    }
    const body = await parseJsonBody(req);
    const decision = String(body.decision || "").toLowerCase();
    if (!["approve", "dismiss", "modify"].includes(decision)) {
      fail(res, 400, "Unsupported decision");
      return;
    }

    const now = isoNow();
    if (decision === "approve") alert.status = "executed";
    if (decision === "dismiss") alert.status = "dismissed";
    if (decision === "modify") alert.status = "executed";
    alert.updated_at = now;

    state.decisions.unshift({
      id: state.nextDecisionId++,
      alert_id: alert.id,
      account_id: alert.account_id,
      actor: String(body.actor || "dashboard_user"),
      action: decision,
      payload: { modifications: body.modifications || {} },
      created_at: now,
    });

    if (decision === "approve" || decision === "modify") {
      const primaryAction = (alert.recommendation && alert.recommendation.actions && alert.recommendation.actions[0]) || null;
      state.actions.unshift({
        id: state.nextActionId++,
        alert_id: alert.id,
        account_id: alert.account_id,
        action_type: primaryAction ? primaryAction.action_type : "manual_action",
        status: "executed",
        params: primaryAction ? primaryAction.params || {} : {},
        created_at: now,
        executed_at: now,
      });

      if (primaryAction && primaryAction.action_type === "draft_campaign") {
        const newDraft = createDraftFromInput(state, {
          account_id: alert.account_id,
          prompt: alert.summary,
          campaign_goal: primaryAction.params && primaryAction.params.campaign_goal,
          target_geography: primaryAction.params && primaryAction.params.target_geography,
          monthly_budget: primaryAction.params && primaryAction.params.recommended_monthly_budget,
          files: [],
        });
        state.drafts.unshift(newDraft);
        addNotification(state, {
          kind: "draft",
          severity: "info",
          title: "Draft created from alert",
          body: `Draft #${newDraft.id} is ready from alert #${alert.id}.`,
          account_id: alert.account_id,
          alert_id: alert.id,
          draft_id: newDraft.id,
        });
      }
    }

    addNotification(state, {
      kind: "alert",
      severity: decision === "dismiss" ? "info" : "warning",
      title: `Alert ${decision}`,
      body: `Alert #${alert.id} marked as ${alert.status}.`,
      account_id: alert.account_id,
      alert_id: alert.id,
    });

    ok(res, { result: { alert_id: alert.id, status: alert.status } });
    return;
  }

  if (parts[0] === "run-monitoring") {
    state.lastMonitoringRunAt = isoNow();
    const first = state.accounts[0];
    if (first && first.health && first.health.metrics) {
      first.health.metrics.spend_prev_7d = first.health.metrics.spend_7d;
      first.health.metrics.spend_7d = Number((first.health.metrics.spend_7d * 1.01).toFixed(2));
      first.health.metrics.roas_prev_7d = first.health.metrics.roas_7d;
      first.health.metrics.roas_7d = Number((first.health.metrics.roas_7d * 1.02).toFixed(2));
      first.updated_at = state.lastMonitoringRunAt;
    }
    addNotification(state, {
      kind: "system",
      severity: "info",
      title: "Monitoring cycle complete",
      body: "Fresh metrics were generated in demo mode.",
      account_id: first ? first.id : null,
    });
    ok(res, { result: { completed: true, ran_at: state.lastMonitoringRunAt } });
    return;
  }

  if (parts[0] === "reports" && parts[1] === "weekly" && parts[2] === "generate") {
    const now = isoNow();
    state.weeklyReport = {
      ...state.weeklyReport,
      content_markdown: buildWeeklyReportContent(now),
      generated_at: now,
    };
    addNotification(state, {
      kind: "system",
      severity: "info",
      title: "Weekly report refreshed",
      body: "Portfolio report regenerated from latest demo state.",
      account_id: null,
    });
    ok(res, { report: state.weeklyReport });
    return;
  }

  if (parts[0] === "thresholds" && parts[1] === "calibrate") {
    state.thresholds = state.thresholds.map((threshold) => ({
      ...threshold,
      cpa_target: Number((Number(threshold.cpa_target) * 0.99).toFixed(1)),
      source: "calibrated_demo",
    }));
    ok(res, {
      result: {
        suggestions: state.thresholds.map((threshold) => ({
          vertical: threshold.vertical,
          cpa_target: threshold.cpa_target,
        })),
      },
      thresholds: state.thresholds,
    });
    return;
  }

  fail(res, 404, "Not Found");
}

module.exports = async function handler(req, res) {
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }
  const state = getState();
  const parts = routeParts(req);

  if (req.method === "GET") {
    handleGet(req, res, state, parts);
    return;
  }

  if (req.method === "POST") {
    await handlePost(req, res, state, parts);
    return;
  }

  fail(res, 405, "Method Not Allowed");
};
