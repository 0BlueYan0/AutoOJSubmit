const path = require("path");
const express = require("express");
const session = require("express-session");

const app = express();

const PORT = process.env.PORT || 3000;
const SESSION_SECRET = process.env.SESSION_SECRET || "change-this-secret";

app.use(express.json({ limit: "10mb" }));
app.use(
  session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: "lax",
      secure: false,
      maxAge: 1000 * 60 * 60 * 24 * 30
    }
  })
);

function normalizeBaseUrl(url) {
  const trimmed = String(url || "").trim();
  return trimmed.replace(/\/+$/, "");
}

function isString(value) {
  return typeof value === "string";
}

function extractCookiesFromHeaders(headers) {
  const raw = headers.getSetCookie ? headers.getSetCookie() : headers.get("set-cookie");
  const list = Array.isArray(raw)
    ? raw
    : typeof raw === "string"
      ? raw.split(/,(?=[^;,\s]+=)/g)
      : [];
  const map = new Map();

  for (const item of list) {
    const first = item.split(";")[0];
    const eq = first.indexOf("=");
    if (eq > 0) {
      const name = first.slice(0, eq).trim();
      const value = first.slice(eq + 1).trim();
      map.set(name, value);
    }
  }

  return map;
}

function mergeCookieMaps(target, source) {
  for (const [name, value] of source.entries()) {
    target.set(name, value);
  }
}

function parseCookieInput(raw) {
  const map = new Map();
  if (!isString(raw) || !raw.trim()) {
    return map;
  }
  for (const seg of raw.split(";")) {
    const item = seg.trim();
    if (!item) {
      continue;
    }
    const eq = item.indexOf("=");
    if (eq <= 0) {
      continue;
    }
    const name = item.slice(0, eq).trim();
    const value = item.slice(eq + 1).trim();
    if (name) {
      map.set(name, value);
    }
  }
  return map;
}

function cookieHeaderFromMap(map) {
  return Array.from(map.entries())
    .map(([k, v]) => `${k}=${v}`)
    .join("; ");
}

function getState(req) {
  if (!req.session.ojState) {
    req.session.ojState = {
      baseUrl: "",
      cookies: {},
      username: "",
      loggedIn: false,
      hasContestAccess: {}
    };
  }
  return req.session.ojState;
}

function getCookieMapFromState(state) {
  return new Map(Object.entries(state.cookies || {}));
}

function setCookieMapToState(state, map) {
  state.cookies = Object.fromEntries(map.entries());
}

function isUnsafeMethod(method) {
  const m = String(method || "GET").toUpperCase();
  return m !== "GET" && m !== "HEAD" && m !== "OPTIONS";
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function parseWaitSecondsFromMessage(message) {
  const m = /please\s*wait\s*(\d+)\s*(?:seconds?|seaconds?|sec)/i.exec(String(message || ""));
  if (!m) {
    return null;
  }
  return Number(m[1]) || 0;
}

async function ojFetchWithState(state, path, init = {}) {
  if (!state || !state.baseUrl) {
    throw new Error("尚未設定 OJ 站點網址");
  }

  const cookieMap = new Map(Object.entries(state.cookies || {}));
  const headers = new Headers(init.headers || {});
  if (cookieMap.size > 0) {
    headers.set("cookie", cookieHeaderFromMap(cookieMap));
  }

  const method = init.method || "GET";

  if (!headers.has("content-type") && init.body && typeof init.body === "string") {
    headers.set("content-type", "application/json");
  }

  if (isUnsafeMethod(method)) {
    const csrfToken = cookieMap.get("csrftoken");
    if (csrfToken && !headers.has("x-csrftoken")) {
      headers.set("x-csrftoken", csrfToken);
    }
    if (!headers.has("origin")) {
      headers.set("origin", state.baseUrl);
    }
    if (!headers.has("referer")) {
      headers.set("referer", `${state.baseUrl}/`);
    }
  }

  const response = await fetch(`${state.baseUrl}${path}`, {
    method,
    headers,
    body: init.body
  });

  const newCookies = extractCookiesFromHeaders(response.headers);
  if (newCookies.size > 0) {
    mergeCookieMaps(cookieMap, newCookies);
    state.cookies = Object.fromEntries(cookieMap.entries());
  }

  const text = await response.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (err) {
    json = null;
  }

  return {
    status: response.status,
    ok: response.ok,
    json,
    text
  };
}

async function ojFetch(req, path, init = {}) {
  const state = getState(req);
  const result = await ojFetchWithState(state, path, init);
  return result;
}

function extractErrorMessage(apiResult, fallback) {
  if (!apiResult) {
    return fallback;
  }
  if (apiResult.json && apiResult.json.data) {
    if (typeof apiResult.json.data === "string") {
      return apiResult.json.data;
    }
    try {
      return JSON.stringify(apiResult.json.data);
    } catch (err) {
      return fallback;
    }
  }
  if (apiResult.text) {
    return apiResult.text.slice(0, 300);
  }
  return fallback;
}

function judgeResultLabel(code) {
  const mapping = {
    "-2": "Compile Error",
    "-1": "Wrong Answer",
    "0": "Accepted",
    "1": "CPU Time Limit Exceeded",
    "2": "Real Time Limit Exceeded",
    "3": "Memory Limit Exceeded",
    "4": "Runtime Error",
    "5": "System Error",
    "6": "Pending",
    "7": "Judging",
    "8": "Partially Accepted"
  };
  return mapping[String(code)] || String(code);
}

function judgeResultShort(code) {
  const mapping = {
    "-2": "CE",
    "-1": "WA",
    "0": "AC",
    "1": "TLE",
    "2": "TLE",
    "3": "MLE",
    "4": "RE",
    "5": "SE",
    "6": "PD",
    "7": "JG",
    "8": "PA"
  };
  return mapping[String(code)] || String(code);
}

function isPendingJudgeShort(shortCode) {
  const s = String(shortCode || "").toUpperCase();
  return s === "PD" || s === "JG" || s === "SUB" || s === "";
}

function applySubmissionDetailToRow(row, detail) {
  if (!row || !detail) {
    return;
  }
  row.result = detail.result;
  row.resultShort = judgeResultShort(detail.result);
  row.resultLabel = judgeResultLabel(detail.result);
  row.code = isString(detail.code) ? detail.code : row.code;
  row.language = detail.language || row.language;
  row.problem = detail.problem || row.problem;
}

async function runJudgeFollowupTick(job) {
  if (!job || job.cancelRequested || !job.state || !job.state.baseUrl) {
    return 0;
  }

  const pendingRows = job.rows.filter((row) => row && row.submissionId && isPendingJudgeShort(row.resultShort));
  if (pendingRows.length === 0) {
    return 0;
  }

  const resolved = job.rows.filter((row) => row && row.submissionId && !isPendingJudgeShort(row.resultShort)).length;
  const totalWithSubmissionId = job.rows.filter((row) => row && row.submissionId).length;
  job.lastMessage = `判題查詢中 ${resolved}/${totalWithSubmissionId}`;

  for (const row of pendingRows) {
    if (job.cancelRequested) {
      job.status = "cancelled";
      job.lastMessage = "已取消";
      return pendingRows.length;
    }

    row.progressState = "judging";
    try {
      const detailResult = await ojFetchWithState(job.state, `/api/submission?id=${encodeURIComponent(String(row.submissionId))}`);
      if (detailResult.json && !detailResult.json.error && detailResult.json.data) {
        const detail = detailResult.json.data;
        applySubmissionDetailToRow(row, detail);
        if (isPendingJudgeShort(row.resultShort)) {
          row.message = `判題中 ${row.resultShort}`;
        } else {
          row.message = row.resultLabel || row.resultShort || "判題完成";
          row.progressState = "done";
        }
      } else {
        row.message = "查詢判題狀態失敗，稍後重試";
      }
    } catch (err) {
      row.message = "查詢判題狀態失敗，稍後重試";
    }
  }

  return job.rows.filter((row) => row && row.submissionId && isPendingJudgeShort(row.resultShort)).length;
}

async function runJudgeFollowupQueue(job, options = {}) {
  const pollIntervalMs = Number.isFinite(Number(options.pollIntervalMs))
    ? Math.max(500, Math.floor(Number(options.pollIntervalMs)))
    : 2000;
  const maxPollRounds = Number.isFinite(Number(options.maxPollRounds))
    ? Math.max(1, Math.floor(Number(options.maxPollRounds)))
    : 600;
  const shouldStop = typeof options.shouldStop === "function"
    ? options.shouldStop
    : () => true;

  for (let round = 0; round < maxPollRounds; round += 1) {
    if (job.cancelRequested) {
      job.status = "cancelled";
      job.lastMessage = "已取消";
      return;
    }

    const pendingCount = await runJudgeFollowupTick(job);
    const stopRequested = shouldStop();

    if (pendingCount === 0 && stopRequested) {
      return;
    }

    await sleep(pollIntervalMs);
  }

  const pendingCount = job.rows.filter((row) => row && row.submissionId && isPendingJudgeShort(row.resultShort)).length;
  if (pendingCount > 0) {
    job.lastMessage = `提交完成，但仍有 ${pendingCount} 筆判題中`;
  }
}

const bulkJobs = new Map();

function uid() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function cloneStateForJob(state) {
  return {
    baseUrl: state.baseUrl,
    cookies: { ...(state.cookies || {}) },
    username: state.username,
    loggedIn: state.loggedIn,
    hasContestAccess: { ...(state.hasContestAccess || {}) }
  };
}

function publicJob(job) {
  return {
    jobId: job.jobId,
    status: job.status,
    contestId: job.contestId,
    total: job.total,
    currentIndex: job.currentIndex,
    successCount: job.successCount,
    failureCount: job.failureCount,
    intervalMs: job.intervalMs,
    createdAt: job.createdAt,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    rows: job.rows,
    pendingJudgeCount: (job.rows || []).filter((row) => row && row.submissionId && isPendingJudgeShort(row.resultShort)).length,
    lastMessage: job.lastMessage
  };
}

function logApiError(scope, err, extra = {}) {
  const msg = err && err.message ? err.message : String(err);
  const payload = {
    scope,
    message: msg,
    ...extra
  };
  console.error("[OJ-API-ERROR]", JSON.stringify(payload));
}

async function runBulkJob(job) {
  job.status = "running";
  job.startedAt = new Date().toISOString();

  try {
    const contestProblemList = await ojFetchWithState(job.state, `/api/contest/problem?contest_id=${encodeURIComponent(String(job.contestId))}`);
    if (!contestProblemList.json || contestProblemList.json.error) {
      throw new Error(extractErrorMessage(contestProblemList, "無法取得競賽題目列表"));
    }

    const problemMap = new Map();
    for (const p of contestProblemList.json.data || []) {
      problemMap.set(String(p._id).toUpperCase(), p);
    }

    for (let i = 0; i < job.items.length; i += 1) {
      if (job.cancelRequested) {
        job.status = "cancelled";
        job.lastMessage = "已取消";
        break;
      }

      job.currentIndex = i;
      const item = job.items[i] || {};
      const row = job.rows[i];

      if (i > 0 && job.intervalMs > 0) {
        row.progressState = "waiting";
        const waitEndAt = Date.now() + job.intervalMs;
        while (Date.now() < waitEndAt) {
          if (job.cancelRequested) {
            break;
          }
          await sleep(500);
        }
        if (job.cancelRequested) {
          job.status = "cancelled";
          job.lastMessage = "已取消";
          break;
        }
      }

      const problemDisplayId = String(item.problemDisplayId || "").trim();
      const language = String(item.language || "").trim();
      const code = isString(item.code) ? item.code : "";

      if (!problemDisplayId || !language || !code) {
        row.ok = false;
        row.message = "缺少 problemDisplayId/language/code";
        row.progressState = "done";
        job.failureCount += 1;
        continue;
      }

      const problem = problemMap.get(problemDisplayId.toUpperCase());
      if (!problem) {
        row.ok = false;
        row.message = "題號不存在於此競賽";
        row.progressState = "done";
        job.failureCount += 1;
        continue;
      }

      if (!Array.isArray(problem.languages) || !problem.languages.includes(language)) {
        row.ok = false;
        row.message = `語言不允許，可用: ${(problem.languages || []).join(", ")}`;
        row.progressState = "done";
        job.failureCount += 1;
        continue;
      }

      row.progressState = "submitting";
      row.message = "提交中...";
      row.problemId = problem.id;
      row.resultShort = "SUB";

      let submitResult = null;
      let retries = 0;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        submitResult = await ojFetchWithState(job.state, "/api/submission", {
          method: "POST",
          body: JSON.stringify({
            contest_id: Number(job.contestId),
            problem_id: problem.id,
            language,
            code
          })
        });

        if (!submitResult.json || submitResult.json.error) {
          const errMsg = extractErrorMessage(submitResult, "提交失敗");
          const waitSeconds = parseWaitSecondsFromMessage(errMsg);
          if (waitSeconds !== null && attempt < 2) {
            retries += 1;
            row.progressState = "waiting";
            const retryWaitSec = Math.max(1, waitSeconds);
            row.message = `等待重試 ${retryWaitSec} 秒`;
            await sleep((retryWaitSec + 1) * 1000);
            continue;
          }
        }
        break;
      }

      row.retries = retries;

      if (!submitResult.json || submitResult.json.error) {
        row.ok = false;
        row.resultShort = "ERR";
        row.message = extractErrorMessage(submitResult, "提交失敗");
        row.progressState = "done";
        job.failureCount += 1;
        continue;
      }

      const submissionId = submitResult.json.data && submitResult.json.data.submission_id ? submitResult.json.data.submission_id : null;
      row.ok = true;
      row.submissionId = submissionId;
      row.message = "提交成功";
      row.progressState = "done";
      row.resultShort = "PD";
      job.successCount += 1;

      if (submissionId) {
        try {
          const detailResult = await ojFetchWithState(job.state, `/api/submission?id=${encodeURIComponent(String(submissionId))}`);
          if (detailResult.json && !detailResult.json.error && detailResult.json.data) {
            const detail = detailResult.json.data;
            applySubmissionDetailToRow(row, detail);
            row.message = row.resultLabel || row.resultShort || "提交成功";
          }
        } catch (err) {
          // keep submission success state even if detail fetch fails
        }
      }

      // Keep all previous PD/JG submissions refreshed during long batch submit.
      await runJudgeFollowupTick(job);
    }

    await runJudgeFollowupQueue(job, {
      pollIntervalMs: 2000,
      maxPollRounds: 600,
      shouldStop: () => true
    });

    if (job.status !== "cancelled") {
      job.status = "finished";
      job.lastMessage = `完成 ${job.successCount}/${job.total}`;
    }
  } catch (err) {
    logApiError("runBulkJob", err, { jobId: job.jobId, contestId: job.contestId });
    job.status = "failed";
    job.lastMessage = err.message || "批次任務失敗";
  } finally {
    job.finishedAt = new Date().toISOString();
  }
}

app.get("/api/session", (req, res) => {
  const state = getState(req);
  res.json({
    ok: true,
    state: {
      baseUrl: state.baseUrl,
      username: state.username,
      loggedIn: state.loggedIn,
      hasContestAccess: state.hasContestAccess || {}
    }
  });
});

app.post("/api/config", async (req, res) => {
  const { baseUrl } = req.body || {};
  if (!isString(baseUrl) || !baseUrl.trim()) {
    return res.status(400).json({ ok: false, message: "baseUrl 必填" });
  }

  const normalized = normalizeBaseUrl(baseUrl);
  if (!/^https?:\/\//i.test(normalized)) {
    return res.status(400).json({ ok: false, message: "baseUrl 必須是 http/https 網址" });
  }

  const state = getState(req);
  state.baseUrl = normalized;
  state.cookies = {};
  state.loggedIn = false;
  state.username = "";
  state.hasContestAccess = {};

  try {
    await ojFetch(req, "/api/profile");
    return res.json({ ok: true, message: "已設定站點" });
  } catch (err) {
    logApiError("config", err, { baseUrl: normalized });
    return res.status(400).json({ ok: false, message: `站點連線失敗: ${err.message}` });
  }
});

app.post("/api/login", async (req, res) => {
  const { username, password, tfaCode } = req.body || {};
  if (!isString(username) || !username.trim() || !isString(password) || !password) {
    return res.status(400).json({ ok: false, message: "username/password 必填" });
  }

  try {
    const payload = {
      username: username.trim(),
      password
    };
    if (isString(tfaCode) && tfaCode.trim()) {
      payload.tfa_code = tfaCode.trim();
    }

    const result = await ojFetch(req, "/api/login", {
      method: "POST",
      body: JSON.stringify(payload)
    });

    if (!result.json || result.json.error) {
      const msg = extractErrorMessage(result, "登入失敗");
      return res.status(400).json({ ok: false, message: msg, raw: result.json || result.text });
    }

    const profile = await ojFetch(req, "/api/profile");
    const state = getState(req);
    state.loggedIn = true;
    state.username = profile.json && profile.json.data && profile.json.data.user ? profile.json.data.user.username : username.trim();

    return res.json({ ok: true, message: "登入成功", username: state.username });
  } catch (err) {
    logApiError("login", err, { username: username && String(username).trim() });
    return res.status(500).json({ ok: false, message: `登入錯誤: ${err.message}` });
  }
});

app.post("/api/cookies/import", async (req, res) => {
  const { cookieHeader, sessionid, csrftoken } = req.body || {};
  const state = getState(req);

  if (!state.baseUrl) {
    return res.status(400).json({ ok: false, message: "請先設定 OJ Base URL" });
  }

  const cookieMap = getCookieMapFromState(state);
  mergeCookieMaps(cookieMap, parseCookieInput(cookieHeader));

  if (isString(sessionid) && sessionid.trim()) {
    cookieMap.set("sessionid", sessionid.trim());
  }
  if (isString(csrftoken) && csrftoken.trim()) {
    cookieMap.set("csrftoken", csrftoken.trim());
  }

  if (!cookieMap.has("sessionid")) {
    return res.status(400).json({ ok: false, message: "至少需要 sessionid" });
  }

  setCookieMapToState(state, cookieMap);

  try {
    const profile = await ojFetch(req, "/api/profile");
    const user = profile.json && profile.json.data && profile.json.data.user ? profile.json.data.user : null;

    if (!user) {
      state.loggedIn = false;
      state.username = "";
      return res.status(400).json({
        ok: false,
        message: "Cookie 驗證失敗，請確認你已在目標 OJ 登入且 sessionid 正確",
        raw: profile.json || profile.text
      });
    }

    state.loggedIn = true;
    state.username = user.username || "";
    return res.json({ ok: true, message: "Cookie 匯入成功", username: state.username });
  } catch (err) {
    logApiError("cookies-import", err, { username: state.username || "" });
    return res.status(500).json({ ok: false, message: `Cookie 匯入錯誤: ${err.message}` });
  }
});

app.post("/api/contest/access", async (req, res) => {
  const { contestId, password } = req.body || {};
  if (!contestId) {
    return res.status(400).json({ ok: false, message: "contestId 必填" });
  }

  try {
    if (isString(password) && password.length > 0) {
      const verifyResult = await ojFetch(req, "/api/contest/password", {
        method: "POST",
        body: JSON.stringify({ contest_id: Number(contestId), password })
      });

      if (!verifyResult.json || verifyResult.json.error) {
        return res.status(400).json({
          ok: false,
          message: extractErrorMessage(verifyResult, "競賽密碼驗證失敗"),
          raw: verifyResult.json || verifyResult.text
        });
      }
    }

    const accessResult = await ojFetch(req, `/api/contest/access?contest_id=${encodeURIComponent(String(contestId))}`);
    if (!accessResult.json || accessResult.json.error) {
      return res.status(400).json({
        ok: false,
        message: extractErrorMessage(accessResult, "取得競賽存取狀態失敗"),
        raw: accessResult.json || accessResult.text
      });
    }

    const access = Boolean(accessResult.json.data && accessResult.json.data.access);
    const state = getState(req);
    state.hasContestAccess[String(contestId)] = access;

    return res.json({ ok: true, access });
  } catch (err) {
    logApiError("contest-access", err, { contestId });
    return res.status(500).json({ ok: false, message: `競賽驗證錯誤: ${err.message}` });
  }
});

app.post("/api/problems/resolve", async (req, res) => {
  const { contestId, problemDisplayId } = req.body || {};
  if (!contestId || !isString(problemDisplayId) || !problemDisplayId.trim()) {
    return res.status(400).json({ ok: false, message: "contestId/problemDisplayId 必填" });
  }

  try {
    const result = await ojFetch(req, `/api/contest/problem?contest_id=${encodeURIComponent(String(contestId))}`);
    if (!result.json || result.json.error) {
      return res.status(400).json({
        ok: false,
        message: extractErrorMessage(result, "取得競賽題目列表失敗"),
        raw: result.json || result.text
      });
    }

    const problems = Array.isArray(result.json.data) ? result.json.data : [];
    const target = problems.find((p) => String(p._id || "").toUpperCase() === String(problemDisplayId).trim().toUpperCase());
    if (!target) {
      return res.status(404).json({ ok: false, message: `找不到題號 ${problemDisplayId}` });
    }

    return res.json({
      ok: true,
      contestId: Number(contestId),
      problemDisplayId: target._id,
      problemId: target.id,
      title: target.title,
      languages: target.languages || []
    });
  } catch (err) {
    logApiError("problems-resolve", err, { contestId, problemDisplayId });
    return res.status(500).json({ ok: false, message: `題目解析錯誤: ${err.message}` });
  }
});

app.post("/api/submissions/bulk", async (req, res) => {
  const { contestId, items, intervalMs, asyncMode } = req.body || {};
  if (!contestId || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ ok: false, message: "contestId 與 items 必填" });
  }

  const requestedIntervalMs = Number(intervalMs);
  const submitIntervalMs = Number.isFinite(requestedIntervalMs)
    ? Math.max(0, Math.min(120000, Math.floor(requestedIntervalMs)))
    : 3000;

  const state = getState(req);
  if (!state.loggedIn) {
    return res.status(401).json({ ok: false, message: "尚未登入" });
  }

  if (asyncMode) {
    const jobId = uid();
    const rows = items.map((item, i) => ({
      index: i,
      tag: isString(item.tag) ? item.tag : `item-${i + 1}`,
      problemDisplayId: String(item.problemDisplayId || "").trim(),
      language: String(item.language || "").trim(),
      submissionId: null,
      ok: false,
      retries: 0,
      message: "等待中",
      progressState: "pending"
    }));

    const job = {
      jobId,
      user: state.username || "",
      state: cloneStateForJob(state),
      status: "queued",
      contestId: Number(contestId),
      intervalMs: submitIntervalMs,
      total: items.length,
      currentIndex: -1,
      successCount: 0,
      failureCount: 0,
      items,
      rows,
      createdAt: new Date().toISOString(),
      startedAt: null,
      finishedAt: null,
      cancelRequested: false,
      lastMessage: "任務已建立"
    };

    bulkJobs.set(jobId, job);
    runBulkJob(job).catch((err) => {
      logApiError("bulk-job-background", err, { jobId, contestId });
    });

    return res.json({ ok: true, async: true, job: publicJob(job) });
  }

  try {
    const contestProblemList = await ojFetch(req, `/api/contest/problem?contest_id=${encodeURIComponent(String(contestId))}`);
    if (!contestProblemList.json || contestProblemList.json.error) {
      return res.status(400).json({
        ok: false,
        message: extractErrorMessage(contestProblemList, "無法取得競賽題目列表"),
        raw: contestProblemList.json || contestProblemList.text
      });
    }
    const problemMap = new Map();
    for (const p of contestProblemList.json.data || []) {
      problemMap.set(String(p._id).toUpperCase(), p);
    }

    const results = [];

    for (let i = 0; i < items.length; i += 1) {
      if (i > 0 && submitIntervalMs > 0) {
        await sleep(submitIntervalMs);
      }

      const item = items[i] || {};
      const problemDisplayId = String(item.problemDisplayId || "").trim();
      const language = String(item.language || "").trim();
      const code = isString(item.code) ? item.code : "";
      const captcha = isString(item.captcha) && item.captcha.trim()
        ? item.captcha.trim()
        : isString(defaultCaptcha)
          ? defaultCaptcha.trim()
          : "";
      const tag = isString(item.tag) ? item.tag : `item-${i + 1}`;

      if (!problemDisplayId || !language || !code) {
        results.push({
          index: i,
          tag,
          problemDisplayId,
          ok: false,
          message: "缺少 problemDisplayId/language/code"
        });
        continue;
      }

      const problem = problemMap.get(problemDisplayId.toUpperCase());
      if (!problem) {
        results.push({
          index: i,
          tag,
          problemDisplayId,
          ok: false,
          message: "題號不存在於此競賽"
        });
        continue;
      }

      if (!Array.isArray(problem.languages) || !problem.languages.includes(language)) {
        results.push({
          index: i,
          tag,
          problemDisplayId,
          ok: false,
          message: `語言不允許，可用: ${(problem.languages || []).join(", ")}`
        });
        continue;
      }

      let submitResult = null;
      let retries = 0;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        submitResult = await ojFetch(req, "/api/submission", {
          method: "POST",
          body: JSON.stringify({
            contest_id: Number(contestId),
            problem_id: problem.id,
            language,
            code,
            ...(captcha ? { captcha } : {})
          })
        });

        if (!submitResult.json || submitResult.json.error) {
          const errMsg = extractErrorMessage(submitResult, "提交失敗");
          const waitSeconds = parseWaitSecondsFromMessage(errMsg);
          if (waitSeconds !== null && attempt < 2) {
            retries += 1;
            const retryWaitSec = Math.max(1, waitSeconds);
            await sleep((retryWaitSec + 1) * 1000);
            continue;
          }
        }
        break;
      }

      if (!submitResult.json || submitResult.json.error) {
        results.push({
          index: i,
          tag,
          problemDisplayId,
          problemId: problem.id,
          ok: false,
          message: extractErrorMessage(submitResult, "提交失敗"),
          retries,
          raw: submitResult.json || submitResult.text
        });
        continue;
      }

      const submissionId = submitResult.json.data && submitResult.json.data.submission_id ? submitResult.json.data.submission_id : null;
      results.push({
        index: i,
        tag,
        problemDisplayId,
        problemId: problem.id,
        ok: true,
        submissionId,
        retries,
        message: "提交成功"
      });
    }

    const successCount = results.filter((r) => r.ok).length;
    return res.json({
      ok: true,
      async: false,
      total: results.length,
      successCount,
      failureCount: results.length - successCount,
      intervalMs: submitIntervalMs,
      results
    });
  } catch (err) {
    logApiError("bulk-sync", err, { contestId, itemsCount: items.length });
    return res.status(500).json({ ok: false, message: `批次提交錯誤: ${err.message}` });
  }
});

app.get("/api/submissions/bulk/jobs", (req, res) => {
  const state = getState(req);
  const username = state.username || "";
  const list = Array.from(bulkJobs.values())
    .filter((job) => (job.user || "") === username)
    .sort((a, b) => String(b.createdAt).localeCompare(String(a.createdAt)))
    .slice(0, 20)
    .map((job) => publicJob(job));

  return res.json({ ok: true, jobs: list });
});

app.get("/api/submissions/bulk/jobs/:jobId", (req, res) => {
  const { jobId } = req.params;
  const state = getState(req);
  const username = state.username || "";
  const job = bulkJobs.get(String(jobId));
  if (!job) {
    return res.status(404).json({ ok: false, message: "找不到任務" });
  }
  if ((job.user || "") !== username) {
    return res.status(403).json({ ok: false, message: "無權限查看此任務" });
  }

  return res.json({ ok: true, job: publicJob(job) });
});

app.post("/api/submissions/bulk/jobs/:jobId/cancel", (req, res) => {
  const { jobId } = req.params;
  const state = getState(req);
  const username = state.username || "";
  const job = bulkJobs.get(String(jobId));
  if (!job) {
    return res.status(404).json({ ok: false, message: "找不到任務" });
  }
  if ((job.user || "") !== username) {
    return res.status(403).json({ ok: false, message: "無權限取消此任務" });
  }
  if (job.status === "finished" || job.status === "failed" || job.status === "cancelled") {
    return res.json({ ok: true, job: publicJob(job) });
  }

  job.cancelRequested = true;
  job.lastMessage = "取消中";
  return res.json({ ok: true, job: publicJob(job) });
});

app.get("/api/captcha", async (req, res) => {
  try {
    const result = await ojFetch(req, "/api/captcha");
    if (!result.json || result.json.error) {
      return res.status(400).json({
        ok: false,
        message: extractErrorMessage(result, "取得 captcha 失敗"),
        raw: result.json || result.text
      });
    }

    return res.json({ ok: true, imageBase64: result.json.data || "" });
  } catch (err) {
    logApiError("captcha", err);
    return res.status(500).json({ ok: false, message: `取得 captcha 錯誤: ${err.message}` });
  }
});

app.post("/api/submissions/status", async (req, res) => {
  const { ids } = req.body || {};
  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ ok: false, message: "ids 必填" });
  }

  const uniqueIds = Array.from(new Set(ids.map((x) => String(x).trim()).filter(Boolean)));
  const detail = [];
  for (const id of uniqueIds) {
    const result = await ojFetch(req, `/api/submission?id=${encodeURIComponent(id)}`);
    if (!result.json || result.json.error) {
      detail.push({
        id,
        ok: false,
        message: extractErrorMessage(result, "查詢失敗"),
        raw: result.json || result.text
      });
      continue;
    }

    const d = result.json.data || {};
    detail.push({
      id,
      ok: true,
      result: d.result,
      resultShort: judgeResultShort(d.result),
      resultLabel: judgeResultLabel(d.result),
      language: d.language,
      problem: d.problem,
      createTime: d.create_time,
      statisticInfo: d.statistic_info || {},
      shared: d.shared,
      code: isString(d.code) ? d.code : ""
    });
  }

  return res.json({ ok: true, total: detail.length, detail });
});

app.use((err, req, res, next) => {
  logApiError("unhandled", err, { path: req.path, method: req.method });
  if (res.headersSent) {
    return next(err);
  }
  return res.status(500).json({ ok: false, message: "伺服器內部錯誤" });
});

app.post("/api/logout", async (req, res) => {
  try {
    await ojFetch(req, "/api/logout");
  } catch (err) {
    // ignore upstream errors on logout
  }

  req.session.ojState = {
    baseUrl: "",
    cookies: {},
    username: "",
    loggedIn: false,
    hasContestAccess: {}
  };
  return res.json({ ok: true, message: "已登出" });
});

app.use(express.static(path.join(__dirname, "public")));

app.listen(PORT, () => {
  console.log(`OJ batch submit web is running at http://localhost:${PORT}`);
});
