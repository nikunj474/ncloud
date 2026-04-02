// =============================================================================
// fe_server.cc  --  Frontend server implementation
// =============================================================================

#include "fe_server.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <condition_variable>
#include <cstring>
#include <cerrno>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>
#include <chrono>
#include <random>
#include <iomanip>

// ---------------------------------------------------------------------------
// Embedded ThreadPool (same design as kvserver)
// ---------------------------------------------------------------------------
struct FEServer::ThreadPool {
    std::vector<std::thread>          workers;
    std::queue<std::function<void()>> tasks;
    std::mutex                        mu;
    std::condition_variable           cv;
    bool                              stop = false;

    explicit ThreadPool(int n) {
        for (int i = 0; i < n; ++i)
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    { std::unique_lock<std::mutex> lk(mu);
                      cv.wait(lk, [this]{ return stop || !tasks.empty(); });
                      if (stop && tasks.empty()) return;
                      task = std::move(tasks.front()); tasks.pop(); }
                    task();
                }
            });
    }
    ~ThreadPool() {
        { std::lock_guard<std::mutex> lk(mu); stop = true; }
        cv.notify_all();
        for (auto& w : workers) w.join();
    }
    void enqueue(std::function<void()> f) {
        { std::lock_guard<std::mutex> lk(mu); tasks.push(std::move(f)); }
        cv.notify_one();
    }
};

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------
FEServer::FEServer(const Config& cfg) : cfg_(cfg) {
    kv_ = std::make_unique<KVClient>(cfg_.kv_host, cfg_.kv_port);
    pool_ = std::make_unique<ThreadPool>(cfg_.threads);

    // Wire up session manager to KV client
    sessions_ = std::make_unique<SessionManager>(
        [this](const std::string& r, const std::string& c, std::string& v) {
            return kv_->get(r, c, v);
        },
        [this](const std::string& r, const std::string& c, const std::string& v) {
            return kv_->put(r, c, v);
        },
        [this](const std::string& r, const std::string& c) {
            return kv_->del(r, c);
        }
    );

    // Start inbound SMTP server
    smtp_ = std::make_unique<SMTPServer>(cfg_.smtp_port, kv_.get());
    smtp_->start();
}

FEServer::~FEServer() {
    if (smtp_) smtp_->stop();
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

// ---------------------------------------------------------------------------
// run()
// ---------------------------------------------------------------------------
void FEServer::run() {
    ::signal(SIGPIPE, SIG_IGN);
    listen_fd_ = create_listen_socket();
    running_ = true;

    std::cout << "[feserver:" << cfg_.server_id << "] listening on port "
              << cfg_.port << "\n";

    while (running_) {
        sockaddr_in addr{};
        socklen_t   len = sizeof(addr);
        int cfd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        if (cfd < 0) { if (running_) continue; break; }

        pool_->enqueue([this, cfd] { handle_connection(cfd); });
    }
}

void FEServer::stop() {
    running_ = false;
    if (listen_fd_ >= 0) { ::shutdown(listen_fd_, SHUT_RDWR); ::close(listen_fd_); listen_fd_ = -1; }
}

int FEServer::create_listen_socket() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket(): " + std::string(strerror(errno)));
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(cfg_.port));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("bind(): " + std::string(strerror(errno)));
    ::listen(fd, 256);
    return fd;
}

// ---------------------------------------------------------------------------
// Connection handling -- persistent HTTP/1.1
// ---------------------------------------------------------------------------
void FEServer::handle_connection(int fd) {
    struct timeval tv{.tv_sec = 60, .tv_usec = 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    while (handle_one_request(fd)) {}
    ::close(fd);
}

bool FEServer::handle_one_request(int fd) {
    HttpRequest req;
    if (!read_http_request(fd, req)) return false;

    // SSE -- does not return through normal response path
    if (req.path == "/events" && req.method == "GET") {
        std::string user = get_user(req);
        if (!user.empty()) handle_sse(fd, req, user);
        return false;  // SSE connection is long-lived, close after
    }

    HttpResponse resp = dispatch(req);
    bool is_head = (req.method == "HEAD");
    send_http_response(fd, resp, is_head);
    return req.keep_alive();
}

// ---------------------------------------------------------------------------
// Route dispatch
// ---------------------------------------------------------------------------
std::string FEServer::get_user(const HttpRequest& req) {
    return sessions_->authenticate(req);
}

HttpResponse FEServer::dispatch(const HttpRequest& req) {
    const std::string& path   = req.path;
    const std::string& method = req.method;

    // ---- Static / SPA shell -------------------------------------------------
    if (path == "/" || path == "/index.html")
        return handle_spa_shell(req);

    if (path.rfind("/static/", 0) == 0)
        return handle_static(req);

    // ---- Auth endpoints (no session required) --------------------------------
    if (path == "/api/login"  && method == "POST") return handle_login(req);
    if (path == "/api/signup" && method == "POST") return handle_signup(req);

    // ---- Protected endpoints (session required) ------------------------------
    std::string user = get_user(req);

    if (path == "/api/logout"          && method == "POST") return handle_logout(req);
    if (path == "/api/change-password" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_change_password(req);
    }

    // Mail
    if (path == "/api/inbox" && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_inbox(req, user);
    }
    if (path == "/api/send" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_send_email(req, user);
    }
    if (path == "/api/delete-email" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_delete_email(req, user);
    }

    // /api/email/:uid
    {
        auto m = match_route("/api/email/:uid", path);
        if (m.matched && method == "GET") {
            if (user.empty()) return HttpResponse::error(401, "Unauthorized");
            return handle_get_email(req, user, m.params.at("uid"));
        }
    }

    // Drive
    if (path == "/api/upload" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_upload(req, user);
    }
    if (path == "/api/rename" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_rename(req, user);
    }
    if (path == "/api/move" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_move(req, user);
    }
    if (path == "/api/mkdir" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_mkdir(req, user);
    }
    if (path == "/api/delete-path" && method == "POST") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_delete_path(req, user);
    }

    // /api/drive/* folder listing
    if (path.rfind("/api/drive", 0) == 0 && method == "GET") {
        if (user.empty()) return HttpResponse::error(401, "Unauthorized");
        return handle_drive_list(req, user);
    }

    // /api/download/:uid
    {
        auto m = match_route("/api/download/:uid", path);
        if (m.matched && method == "GET") {
            if (user.empty()) return HttpResponse::error(401, "Unauthorized");
            return handle_download(req, user, m.params.at("uid"));
        }
    }

    // Admin
    if (path == "/admin" && method == "GET")         return handle_admin_page(req);
    if (path == "/admin/metrics" && method == "GET") return handle_admin_metrics(req);

    return HttpResponse::not_found();
}

// ---------------------------------------------------------------------------
// SPA shell: the single HTML page served for every non-API route.
// The JS router takes over from here.
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_spa_shell(const HttpRequest&) {
    // The entire SPA shell is embedded here so the frontend has zero
    // external file dependencies during development.
    // In production you would serve this from cfg_.static_dir.
    static const std::string html = R"HTML(
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PennCloud</title>
  <style>
    /* ---- Reset + base ---- */
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --penn-blue: #011F5B;
      --accent:    #0066CC;
      --bg:        #F5F7FA;
      --surface:   #FFFFFF;
      --border:    #E2E8F0;
      --text:      #1A202C;
      --muted:     #718096;
      --success:   #1A6B3A;
      --danger:    #C53030;
      --sidebar-w: 220px;
    }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: var(--bg); color: var(--text); min-height: 100vh; }

    /* ---- Login page ---- */
    #login-page {
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; background: var(--penn-blue);
    }
    .login-card {
      background: var(--surface); border-radius: 12px;
      padding: 40px 36px; width: 360px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
    }
    .login-card h1 { color: var(--penn-blue); font-size: 26px; margin-bottom: 6px; }
    .login-card p  { color: var(--muted); font-size: 14px; margin-bottom: 24px; }
    .form-group { margin-bottom: 16px; }
    .form-group label { display: block; font-size: 13px; font-weight: 600;
                        color: var(--muted); margin-bottom: 6px; }
    .form-group input {
      width: 100%; padding: 10px 14px; border: 1px solid var(--border);
      border-radius: 8px; font-size: 14px; outline: none; transition: border .15s;
    }
    .form-group input:focus { border-color: var(--accent); }
    .btn {
      width: 100%; padding: 11px; border: none; border-radius: 8px;
      font-size: 14px; font-weight: 600; cursor: pointer; transition: opacity .15s;
    }
    .btn-primary { background: var(--penn-blue); color: white; }
    .btn-primary:hover { opacity: .88; }
    .btn-link { background: none; color: var(--accent); font-size: 13px;
                text-decoration: underline; cursor: pointer; border: none; }
    .error-msg { color: var(--danger); font-size: 13px; margin-top: 8px; display: none; }

    /* ---- App shell ---- */
    #app { display: none; min-height: 100vh; flex-direction: column; }
    .topbar {
      background: var(--penn-blue); color: white;
      padding: 0 24px; height: 52px;
      display: flex; align-items: center; justify-content: space-between;
    }
    .topbar .brand { font-size: 18px; font-weight: 700; letter-spacing: -0.3px; }
    .topbar .user-info { font-size: 13px; opacity: .8; display: flex;
                         align-items: center; gap: 16px; }
    .topbar .logout-btn { background: rgba(255,255,255,0.15); border: none;
                          color: white; padding: 5px 12px; border-radius: 6px;
                          cursor: pointer; font-size: 12px; }
    .main-layout { display: flex; flex: 1; }
    .sidebar {
      width: var(--sidebar-w); background: var(--surface);
      border-right: 1px solid var(--border); padding: 16px 0;
      display: flex; flex-direction: column; gap: 2px;
    }
    .nav-item {
      display: flex; align-items: center; gap: 10px;
      padding: 10px 20px; cursor: pointer; border-radius: 0;
      font-size: 14px; color: var(--muted); transition: all .1s;
      border: none; background: none; width: 100%; text-align: left;
    }
    .nav-item:hover { background: var(--bg); color: var(--text); }
    .nav-item.active { background: #EEF3FB; color: var(--penn-blue);
                       font-weight: 600; border-right: 3px solid var(--penn-blue); }
    .nav-icon { width: 18px; text-align: center; font-size: 16px; }
    .content { flex: 1; padding: 24px; overflow-y: auto; }

    /* ---- Inbox ---- */
    .page-title { font-size: 20px; font-weight: 700; color: var(--penn-blue);
                  margin-bottom: 16px; display: flex; align-items: center;
                  justify-content: space-between; }
    .compose-btn {
      background: var(--penn-blue); color: white; border: none;
      padding: 8px 16px; border-radius: 8px; cursor: pointer;
      font-size: 13px; font-weight: 600;
    }
    .email-list { background: var(--surface); border-radius: 10px;
                  border: 1px solid var(--border); overflow: hidden; }
    .email-row {
      display: flex; align-items: center; padding: 14px 20px;
      border-bottom: 1px solid var(--border); cursor: pointer;
      transition: background .1s; gap: 16px;
    }
    .email-row:hover { background: var(--bg); }
    .email-row:last-child { border-bottom: none; }
    .email-row.unread .email-from { font-weight: 700; }
    .email-from  { width: 160px; flex-shrink: 0; font-size: 14px;
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .email-subj  { flex: 1; font-size: 14px; color: var(--text);
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .email-time  { font-size: 12px; color: var(--muted); flex-shrink: 0; }
    .badge-new {
      font-size: 10px; background: var(--accent); color: white;
      padding: 1px 6px; border-radius: 10px; flex-shrink: 0;
    }

    /* ---- Email view ---- */
    .email-view { background: var(--surface); border-radius: 10px;
                  border: 1px solid var(--border); padding: 28px; }
    .email-view h2 { font-size: 18px; margin-bottom: 12px; }
    .email-meta { color: var(--muted); font-size: 13px; margin-bottom: 16px;
                  display: flex; flex-direction: column; gap: 4px; }
    .email-body { white-space: pre-wrap; font-size: 14px; line-height: 1.6;
                  border-top: 1px solid var(--border); padding-top: 16px; }
    .email-actions { display: flex; gap: 8px; margin-bottom: 16px; }
    .action-btn {
      padding: 7px 14px; border-radius: 7px; font-size: 13px; cursor: pointer;
      border: 1px solid var(--border); background: var(--surface);
    }
    .action-btn.danger { color: var(--danger); border-color: var(--danger); }

    /* ---- Compose ---- */
    .compose-form { background: var(--surface); border-radius: 10px;
                    border: 1px solid var(--border); padding: 28px; max-width: 700px; }
    .compose-form input, .compose-form textarea {
      width: 100%; padding: 10px 14px; border: 1px solid var(--border);
      border-radius: 8px; font-size: 14px; margin-bottom: 12px;
      font-family: inherit; outline: none;
    }
    .compose-form textarea { height: 200px; resize: vertical; }
    .compose-form input:focus, .compose-form textarea:focus { border-color: var(--accent); }

    /* ---- Drive ---- */
    .drive-toolbar { display: flex; gap: 8px; margin-bottom: 16px; }
    .drive-toolbar button {
      padding: 7px 14px; border-radius: 7px; font-size: 13px; cursor: pointer;
      border: 1px solid var(--border); background: var(--surface);
    }
    .breadcrumb { font-size: 13px; color: var(--muted); margin-bottom: 12px; }
    .breadcrumb span { cursor: pointer; color: var(--accent); }
    .breadcrumb span:hover { text-decoration: underline; }
    .file-grid { background: var(--surface); border-radius: 10px;
                 border: 1px solid var(--border); overflow: hidden; }
    .file-row {
      display: flex; align-items: center; padding: 12px 20px;
      border-bottom: 1px solid var(--border); gap: 12px;
    }
    .file-row:last-child { border-bottom: none; }
    .file-icon { font-size: 18px; width: 24px; text-align: center; }
    .file-name { flex: 1; font-size: 14px; cursor: pointer; }
    .file-name:hover { color: var(--accent); text-decoration: underline; }
    .file-size { font-size: 12px; color: var(--muted); min-width: 80px; }
    .file-actions { display: flex; gap: 6px; }
    .file-actions button {
      padding: 3px 8px; font-size: 11px; border-radius: 5px;
      border: 1px solid var(--border); cursor: pointer; background: var(--surface);
    }

    /* ---- Notifications ---- */
    .toast {
      position: fixed; bottom: 24px; right: 24px;
      background: var(--text); color: white;
      padding: 12px 20px; border-radius: 8px; font-size: 13px;
      opacity: 0; transition: opacity .3s; pointer-events: none; z-index: 999;
    }
    .toast.show { opacity: 1; }
    .spinner { text-align: center; padding: 40px; color: var(--muted); }
  </style>
</head>
<body>

<!-- Login Page -->
<div id="login-page">
  <div class="login-card">
    <h1>PennCloud</h1>
    <p id="login-subtitle">Sign in to your account</p>
    <div class="form-group">
      <label>Username</label>
      <input id="username-in" type="text" placeholder="e.g. nikunj" autocomplete="username">
    </div>
    <div class="form-group">
      <label>Password</label>
      <input id="password-in" type="password" placeholder="Your password" autocomplete="current-password">
    </div>
    <button class="btn btn-primary" id="login-btn" onclick="doLogin()">Sign in</button>
    <div id="login-error" class="error-msg"></div>
    <br>
    <button class="btn-link" onclick="showSignup()">Create an account</button>
  </div>
</div>

<!-- App Shell -->
<div id="app">
  <div class="topbar">
    <div class="brand">PennCloud</div>
    <div class="user-info">
      <span id="topbar-user"></span>
      <button class="logout-btn" onclick="doLogout()">Sign out</button>
    </div>
  </div>
  <div class="main-layout">
    <div class="sidebar">
      <button class="nav-item" id="nav-inbox" onclick="navigate('inbox')">
        <span class="nav-icon">&#9993;</span> Inbox
      </button>
      <button class="nav-item" id="nav-compose" onclick="navigate('compose')">
        <span class="nav-icon">&#9998;</span> Compose
      </button>
      <button class="nav-item" id="nav-drive" onclick="navigate('drive')">
        <span class="nav-icon">&#128193;</span> Drive
      </button>
      <button class="nav-item" id="nav-settings" onclick="navigate('settings')">
        <span class="nav-icon">&#9881;</span> Settings
      </button>
      <button class="nav-item" id="nav-admin" onclick="window.open('/admin','_blank')">
        <span class="nav-icon">&#9879;</span> Admin
      </button>
    </div>
    <div class="content" id="content">
      <div class="spinner">Loading...</div>
    </div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
// =============================================================================
// PennCloud SPA Router (F4 innovation)
// Zero full-page reloads. All navigation is fetch() + DOM swap.
// =============================================================================

let currentUser = '';
let currentPath = '/';    // drive current folder
let eventSource = null;   // SSE connection (F1)

// ---- Auth ------------------------------------------------------------------
async function doLogin() {
  const u = document.getElementById('username-in').value.trim();
  const p = document.getElementById('password-in').value;
  if (!u || !p) { showError('Please enter username and password.'); return; }

  const r = await fetch('/api/login', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `username=${encodeURIComponent(u)}&password=${encodeURIComponent(p)}`
  });
  const data = await r.json();
  if (data.ok) {
    currentUser = u;
    showApp();
  } else {
    showError(data.error || 'Invalid username or password.');
  }
}

async function doLogout() {
  await fetch('/api/logout', { method: 'POST' });
  if (eventSource) { eventSource.close(); eventSource = null; }
  showLogin();
}

function showSignup() {
  document.getElementById('login-subtitle').textContent = 'Create a new account';
  document.getElementById('login-btn').textContent = 'Create account';
  document.getElementById('login-btn').onclick = doSignup;
}

async function doSignup() {
  const u = document.getElementById('username-in').value.trim();
  const p = document.getElementById('password-in').value;
  const r = await fetch('/api/signup', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `username=${encodeURIComponent(u)}&password=${encodeURIComponent(p)}`
  });
  const data = await r.json();
  if (data.ok) { currentUser = u; showApp(); }
  else showError(data.error || 'Signup failed.');
}

function showLogin() {
  document.getElementById('login-page').style.display = 'flex';
  document.getElementById('app').style.display = 'none';
}

function showApp() {
  document.getElementById('login-page').style.display = 'none';
  document.getElementById('app').style.display = 'flex';
  document.getElementById('topbar-user').textContent = currentUser;
  startSSE();
  navigate('inbox');
}

function showError(msg) {
  const el = document.getElementById('login-error');
  el.textContent = msg;
  el.style.display = 'block';
}

// ---- Navigation (F4: SPA router) -------------------------------------------
function navigate(view, params = {}) {
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  const navEl = document.getElementById('nav-' + view);
  if (navEl) navEl.classList.add('active');
  history.pushState({view, params}, '', '/' + view);

  const content = document.getElementById('content');
  content.innerHTML = '<div class="spinner">Loading...</div>';

  switch (view) {
    case 'inbox':   renderInbox();  break;
    case 'compose': renderCompose(params); break;
    case 'drive':   renderDrive(params.path || '/'); break;
    case 'email':   renderEmail(params.uid); break;
    case 'settings':renderSettings(); break;
  }
}

window.addEventListener('popstate', e => {
  if (e.state) navigate(e.state.view, e.state.params || {});
});

// ---- Inbox -----------------------------------------------------------------
async function renderInbox() {
  const r = await fetch('/api/inbox');
  const data = await r.json();
  const content = document.getElementById('content');

  if (!data.ok) { content.innerHTML = '<p>Error loading inbox.</p>'; return; }

  const emails = data.emails || [];
  content.innerHTML = `
    <div class="page-title">
      Inbox <span style="font-size:14px;color:var(--muted);font-weight:400">${emails.length} messages</span>
      <button class="compose-btn" onclick="navigate('compose')">+ Compose</button>
    </div>
    <div class="email-list" id="email-list">
      ${emails.length === 0
        ? '<div style="padding:40px;text-align:center;color:var(--muted)">No messages yet</div>'
        : emails.map(e => `
          <div class="email-row" onclick="navigate('email', {uid:'${e.uid}'})">
            <div class="email-from">${escHtml(e.from)}</div>
            <div class="email-subj">${escHtml(e.subject)}</div>
            <div class="email-time">${escHtml(e.time)}</div>
          </div>`).join('')}
    </div>`;
}

// Prepend a new email row (called by SSE handler -- F1)
function prependEmailRow(email) {
  const list = document.getElementById('email-list');
  if (!list) return;
  const row = document.createElement('div');
  row.className = 'email-row unread';
  row.innerHTML = `
    <div class="email-from">${escHtml(email.from)}</div>
    <div class="email-subj">${escHtml(email.subject)}</div>
    <div class="email-time">just now</div>
    <span class="badge-new">New</span>`;
  row.onclick = () => navigate('email', {uid: email.uid});
  list.prepend(row);
  showToast('New email from ' + email.from);
}

// ---- Email view ------------------------------------------------------------
async function renderEmail(uid) {
  const r = await fetch(`/api/email/${uid}`);
  const data = await r.json();
  const content = document.getElementById('content');
  if (!data.ok) { content.innerHTML = '<p>Email not found.</p>'; return; }
  const e = data.email;
  content.innerHTML = `
    <button onclick="navigate('inbox')" style="margin-bottom:16px;background:none;border:none;cursor:pointer;color:var(--accent);font-size:13px;">
      &larr; Back to inbox
    </button>
    <div class="email-view">
      <h2>${escHtml(e.subject)}</h2>
      <div class="email-meta">
        <span><b>From:</b> ${escHtml(e.from)}</span>
        <span><b>To:</b> ${escHtml(e.to || currentUser)}</span>
        <span><b>Date:</b> ${escHtml(e.time)}</span>
      </div>
      <div class="email-actions">
        <button class="action-btn" onclick="navigate('compose',{reply_to:'${escHtml(e.from)}',subject:'Re: ${escHtml(e.subject)}'})">Reply</button>
        <button class="action-btn" onclick="navigate('compose',{subject:'Fwd: ${escHtml(e.subject)}',body:'\\n\\n--- Forwarded ---\\n${escHtml(e.body)}'})">Forward</button>
        <button class="action-btn danger" onclick="deleteEmail('${uid}')">Delete</button>
      </div>
      <div class="email-body">${escHtml(e.body)}</div>
    </div>`;
}

async function deleteEmail(uid) {
  await fetch('/api/delete-email', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `uid=${uid}`
  });
  showToast('Email deleted');
  navigate('inbox');
}

// ---- Compose ---------------------------------------------------------------
function renderCompose(params = {}) {
  document.getElementById('content').innerHTML = `
    <div class="page-title">Compose</div>
    <div class="compose-form">
      <input id="to-in"      type="text" placeholder="To (e.g. rohit or rohit@gmail.com)" value="${escHtml(params.reply_to || '')}">
      <input id="subject-in" type="text" placeholder="Subject" value="${escHtml(params.subject || '')}">
      <textarea id="body-in" placeholder="Write your message...">${escHtml(params.body || '')}</textarea>
      <div style="display:flex;gap:8px">
        <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="sendEmail()">Send</button>
        <button class="action-btn" onclick="navigate('inbox')">Cancel</button>
      </div>
      <div id="send-error" class="error-msg"></div>
    </div>`;
}

async function sendEmail() {
  const to      = document.getElementById('to-in').value.trim();
  const subject = document.getElementById('subject-in').value.trim();
  const body    = document.getElementById('body-in').value;
  if (!to || !subject) { document.getElementById('send-error').textContent = 'To and Subject are required.'; document.getElementById('send-error').style.display='block'; return; }

  const r = await fetch('/api/send', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `to=${encodeURIComponent(to)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`
  });
  const data = await r.json();
  if (data.ok) { showToast('Email sent!'); navigate('inbox'); }
  else { document.getElementById('send-error').textContent = data.error || 'Send failed.'; document.getElementById('send-error').style.display='block'; }
}

// ---- Drive -----------------------------------------------------------------
async function renderDrive(folderPath = '/') {
  currentPath = folderPath;
  const r = await fetch('/api/drive?path=' + encodeURIComponent(folderPath));
  const data = await r.json();
  const content = document.getElementById('content');
  if (!data.ok) { content.innerHTML = '<p>Error loading drive.</p>'; return; }

  const crumbs = folderPath === '/' ? ['/'] : ['/', ...folderPath.slice(1).split('/')];
  const breadcrumbHtml = crumbs.map((c, i) => {
    const p = '/' + crumbs.slice(1, i + 1).join('/');
    return `<span onclick="renderDrive('${escHtml(p)}')">${escHtml(c)}</span>`;
  }).join(' / ');

  const items = data.items || [];
  content.innerHTML = `
    <div class="page-title">Drive</div>
    <div class="breadcrumb">${breadcrumbHtml}</div>
    <div class="drive-toolbar">
      <button onclick="showUpload()">Upload file</button>
      <button onclick="makeFolder()">New folder</button>
    </div>
    <input type="file" id="file-input" style="display:none" onchange="uploadFile(this)">
    <div class="file-grid">
      ${items.length === 0
        ? '<div style="padding:40px;text-align:center;color:var(--muted)">This folder is empty</div>'
        : items.map(it => `
          <div class="file-row">
            <div class="file-icon">${it.type === 'folder' ? '&#128193;' : '&#128196;'}</div>
            <div class="file-name" onclick="${it.type === 'folder' ? `renderDrive('${escHtml(it.path)}')` : `downloadFile('${it.uid}','${escHtml(it.name)}')`}">${escHtml(it.name)}</div>
            <div class="file-size">${it.size || ''}</div>
            <div class="file-actions">
              <button onclick="renameItem('${escHtml(it.path)}')">Rename</button>
              <button onclick="deleteItem('${escHtml(it.path)}')">Delete</button>
            </div>
          </div>`).join('')}
    </div>`;
}

function showUpload() { document.getElementById('file-input').click(); }

async function uploadFile(input) {
  const file = input.files[0];
  if (!file) return;
  const fd = new FormData();
  fd.append('file', file);
  fd.append('path', currentPath);
  showToast('Uploading...');
  const r = await fetch('/api/upload', { method: 'POST', body: fd });
  const data = await r.json();
  showToast(data.ok ? 'Uploaded!' : 'Upload failed: ' + (data.error || ''));
  if (data.ok) renderDrive(currentPath);
}

async function downloadFile(uid, name) {
  const a = document.createElement('a');
  a.href = `/api/download/${uid}`;
  a.download = name;
  a.click();
}

async function renameItem(path) {
  const newName = prompt('New name:');
  if (!newName) return;
  const r = await fetch('/api/rename', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `path=${encodeURIComponent(path)}&name=${encodeURIComponent(newName)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Renamed!' : 'Rename failed.');
  if (data.ok) renderDrive(currentPath);
}

async function deleteItem(path) {
  if (!confirm('Delete this item?')) return;
  const r = await fetch('/api/delete-path', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `path=${encodeURIComponent(path)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Deleted.' : 'Delete failed.');
  if (data.ok) renderDrive(currentPath);
}

async function makeFolder() {
  const name = prompt('Folder name:');
  if (!name) return;
  const r = await fetch('/api/mkdir', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `path=${encodeURIComponent(currentPath)}&name=${encodeURIComponent(name)}`
  });
  const data = await r.json();
  showToast(data.ok ? 'Folder created.' : 'Failed.');
  if (data.ok) renderDrive(currentPath);
}

// ---- Settings --------------------------------------------------------------
function renderSettings() {
  document.getElementById('content').innerHTML = `
    <div class="page-title">Settings</div>
    <div class="compose-form" style="max-width:400px">
      <h3 style="margin-bottom:16px;font-size:15px">Change password</h3>
      <input id="old-pw"  type="password" placeholder="Current password">
      <input id="new-pw"  type="password" placeholder="New password">
      <input id="new-pw2" type="password" placeholder="Confirm new password">
      <button class="btn btn-primary" style="width:auto;padding:9px 24px" onclick="changePassword()">Update</button>
      <div id="pw-msg" class="error-msg"></div>
    </div>`;
}

async function changePassword() {
  const old = document.getElementById('old-pw').value;
  const n1  = document.getElementById('new-pw').value;
  const n2  = document.getElementById('new-pw2').value;
  if (n1 !== n2) { showPwMsg('Passwords do not match.'); return; }
  const r = await fetch('/api/change-password', {
    method: 'POST',
    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
    body: `old_password=${encodeURIComponent(old)}&new_password=${encodeURIComponent(n1)}`
  });
  const data = await r.json();
  showPwMsg(data.ok ? 'Password updated!' : (data.error || 'Failed.'));
}
function showPwMsg(m) {
  const el = document.getElementById('pw-msg');
  el.textContent = m; el.style.display = 'block';
}

// ---- SSE live inbox (F1 innovation) ----------------------------------------
function startSSE() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource('/events');
  eventSource.addEventListener('new_email', e => {
    try {
      const email = JSON.parse(e.data);
      // If user is on inbox view, prepend the row without full reload
      if (window.location.pathname === '/inbox') {
        prependEmailRow(email);
      } else {
        showToast('New email from ' + email.from);
      }
    } catch(err) {}
  });
  eventSource.onerror = () => {
    // Reconnect in 3 seconds
    setTimeout(startSSE, 3000);
  };
}

// ---- Utils -----------------------------------------------------------------
function escHtml(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                  .replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3000);
}

// Keyboard shortcut: Enter to login
document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.getElementById('login-page').style.display !== 'none') {
    doLogin();
  }
});
</script>
</body>
</html>
)HTML";
    return HttpResponse::ok(html, "text/html; charset=utf-8");
}

// ---------------------------------------------------------------------------
// Auth handlers
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_login(const HttpRequest& req) {
    auto params = parse_urlencoded(req.body);
    std::string username = params["username"];
    std::string password = params["password"];

    if (username.empty() || password.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Missing credentials"})");

    // Fetch stored password from KV
    std::string stored_pwd = kv_->get_str(username, "pwd");
    if (stored_pwd.empty() || stored_pwd != password)
        return HttpResponse::json(R"({"ok":false,"error":"Invalid username or password"})");

    // Create session
    std::string sid = sessions_->create(username);
    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    resp.set_cookie("sid", sid);
    return resp;
}

HttpResponse FEServer::handle_logout(const HttpRequest& req) {
    std::string sid = req.cookie("sid");
    if (!sid.empty()) sessions_->destroy(sid);
    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    // Clear cookie
    resp.headers["Set-Cookie"] = "sid=; Path=/; Max-Age=0; HttpOnly";
    return resp;
}

HttpResponse FEServer::handle_signup(const HttpRequest& req) {
    auto params = parse_urlencoded(req.body);
    std::string username = params["username"];
    std::string password = params["password"];

    if (username.empty() || password.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Missing fields"})");

    // Check if user already exists
    std::string existing = kv_->get_str(username, "pwd");
    if (!existing.empty())
        return HttpResponse::json(R"({"ok":false,"error":"Username already taken"})");

    kv_->put(username, "pwd", password);
    std::string sid = sessions_->create(username);
    HttpResponse resp = HttpResponse::json(R"({"ok":true})");
    resp.set_cookie("sid", sid);
    return resp;
}

HttpResponse FEServer::handle_change_password(const HttpRequest& req) {
    std::string user = sessions_->authenticate(req);
    if (user.empty()) return HttpResponse::error(401, "Unauthorized");

    auto params = parse_urlencoded(req.body);
    std::string old_pw = params["old_password"];
    std::string new_pw = params["new_password"];

    std::string stored = kv_->get_str(user, "pwd");
    if (stored != old_pw)
        return HttpResponse::json(R"({"ok":false,"error":"Wrong current password"})");

    kv_->put(user, "pwd", new_pw);
    return HttpResponse::json(R"({"ok":true})");
}

// ---------------------------------------------------------------------------
// Admin console (Yke — F5 admin metrics)
// ---------------------------------------------------------------------------

// Helper: open a short-lived TCP connection, send a command, read one response line + optional body
static std::string admin_query_node(const std::string& host, int port,
                                     const std::string& cmd, int timeout_ms = 500) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return "";

    struct timeval tv{};
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return "";
    }

    std::string full_cmd = cmd + "\r\n";
    ::send(fd, full_cmd.data(), full_cmd.size(), MSG_NOSIGNAL);

    // Read response line
    std::string line;
    char c;
    while (true) {
        ssize_t r = ::recv(fd, &c, 1, 0);
        if (r <= 0) break;
        if (c == '\n') {
            if (!line.empty() && line.back() == '\r') line.pop_back();
            break;
        }
        line += c;
        if (line.size() > 4096) break;
    }

    // If response is "+OK <len>", read the body
    std::string result = line;
    if (line.rfind("+OK ", 0) == 0 && line.size() > 4) {
        size_t body_len = 0;
        try { body_len = std::stoul(line.substr(4)); } catch (...) {}
        if (body_len > 0 && body_len < 65536) {
            std::string body(body_len, '\0');
            size_t got = 0;
            while (got < body_len) {
                ssize_t r = ::recv(fd, &body[got], body_len - got, 0);
                if (r <= 0) break;
                got += static_cast<size_t>(r);
            }
            result = body;
        }
    }

    ::close(fd);
    return result;
}

HttpResponse FEServer::handle_admin_metrics(const HttpRequest&) {
    // 1. Query coordinator for node list + alive/dead status
    std::string coord_resp = admin_query_node(cfg_.coord_host, cfg_.coord_port, "STATUS");

    // 2. Query the KV node(s) directly for STATS
    std::string kv_stats = admin_query_node(cfg_.kv_host, cfg_.kv_port, "STATS");

    // 3. Check KV node liveness via PING
    std::string kv_ping = admin_query_node(cfg_.kv_host, cfg_.kv_port, "PING");
    bool kv_alive = (kv_ping.rfind("+OK", 0) == 0);

    // Build JSON response
    std::string body = "{";
    body += "\"server_id\":" + json_str(cfg_.server_id) + ",";
    body += "\"kv_host\":" + json_str(cfg_.kv_host) + ",";
    body += "\"kv_port\":" + std::to_string(cfg_.kv_port) + ",";
    body += "\"kv_alive\":" + std::string(kv_alive ? "true" : "false") + ",";
    body += "\"kv_stats\":" + json_str(kv_stats) + ",";
    body += "\"coord_host\":" + json_str(cfg_.coord_host) + ",";
    body += "\"coord_port\":" + std::to_string(cfg_.coord_port) + ",";

    // coord_resp is the JSON array from coordinator's STATUS handler
    if (!coord_resp.empty() && coord_resp[0] == '[') {
        body += "\"nodes\":" + coord_resp;
    } else {
        body += "\"nodes\":[]";
    }
    body += "}";

    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_admin_page(const HttpRequest&) {
    static const std::string html = R"HTML(
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>PennCloud Admin Console</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --penn-blue: #011F5B;
      --accent:    #0066CC;
      --bg:        #F5F7FA;
      --surface:   #FFFFFF;
      --border:    #E2E8F0;
      --text:      #1A202C;
      --muted:     #718096;
      --success:   #38A169;
      --danger:    #E53E3E;
      --warning:   #D69E2E;
    }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
           background: var(--bg); color: var(--text); }

    .topbar {
      background: var(--penn-blue); color: white;
      padding: 0 24px; height: 52px;
      display: flex; align-items: center; justify-content: space-between;
    }
    .topbar .brand { font-size: 18px; font-weight: 700; }
    .topbar .brand a { color: white; text-decoration: none; }
    .topbar .badge { font-size: 12px; background: rgba(255,255,255,0.2);
                     padding: 2px 8px; border-radius: 4px; margin-left: 10px; }
    .topbar .refresh-info { font-size: 12px; opacity: 0.7; }

    .container { max-width: 1000px; margin: 24px auto; padding: 0 24px; }

    .section-title { font-size: 16px; font-weight: 700; color: var(--penn-blue);
                     margin: 24px 0 12px; display: flex; align-items: center; gap: 8px; }

    .card { background: var(--surface); border: 1px solid var(--border);
            border-radius: 10px; overflow: hidden; margin-bottom: 20px; }

    .summary-row { display: flex; gap: 16px; margin-bottom: 20px; }
    .summary-card {
      flex: 1; background: var(--surface); border: 1px solid var(--border);
      border-radius: 10px; padding: 20px;
    }
    .summary-card .label { font-size: 12px; color: var(--muted); text-transform: uppercase;
                           letter-spacing: 0.5px; margin-bottom: 4px; }
    .summary-card .value { font-size: 24px; font-weight: 700; }
    .summary-card .value.up { color: var(--success); }
    .summary-card .value.down { color: var(--danger); }

    table { width: 100%; border-collapse: collapse; }
    th { background: var(--bg); font-size: 12px; text-transform: uppercase;
         letter-spacing: 0.5px; color: var(--muted); padding: 10px 16px;
         text-align: left; font-weight: 600; }
    td { padding: 12px 16px; border-top: 1px solid var(--border); font-size: 14px; }

    .status-dot { display: inline-block; width: 8px; height: 8px;
                  border-radius: 50%; margin-right: 6px; }
    .status-dot.alive { background: var(--success); }
    .status-dot.dead  { background: var(--danger); }

    .tag { display: inline-block; padding: 2px 8px; border-radius: 4px;
           font-size: 11px; font-weight: 600; }
    .tag-primary { background: #EBF4FF; color: var(--accent); }
    .tag-secondary { background: #F0FFF4; color: var(--success); }
    .tag-unknown { background: #FFFBEB; color: var(--warning); }

    .kv-stat { display: inline-block; margin-right: 16px; }
    .kv-stat .stat-label { font-size: 11px; color: var(--muted); }
    .kv-stat .stat-value { font-size: 14px; font-weight: 600; }

    #error-banner { display: none; background: #FED7D7; color: #9B2C2C;
                    padding: 10px 16px; border-radius: 8px; margin-bottom: 16px;
                    font-size: 13px; }

    .last-update { font-size: 11px; color: var(--muted); text-align: right;
                   margin-top: 8px; }
  </style>
</head>
<body>

<div class="topbar">
  <div>
    <span class="brand"><a href="/">PennCloud</a></span>
    <span class="badge">Admin Console</span>
  </div>
  <div class="refresh-info">Auto-refresh: 5s</div>
</div>

<div class="container">
  <div id="error-banner"></div>

  <div class="summary-row" id="summary-row">
    <div class="summary-card">
      <div class="label">Frontend Server</div>
      <div class="value" id="fe-id">--</div>
    </div>
    <div class="summary-card">
      <div class="label">Nodes Online</div>
      <div class="value" id="nodes-up">--</div>
    </div>
    <div class="summary-card">
      <div class="label">Nodes Down</div>
      <div class="value" id="nodes-down">--</div>
    </div>
    <div class="summary-card">
      <div class="label">KV Direct</div>
      <div class="value" id="kv-direct">--</div>
    </div>
  </div>

  <div class="section-title">Backend Storage Nodes</div>
  <div class="card">
    <table>
      <thead>
        <tr>
          <th>Node ID</th>
          <th>Address</th>
          <th>Status</th>
          <th>LSN</th>
          <th>Missed Heartbeats</th>
        </tr>
      </thead>
      <tbody id="nodes-tbody">
        <tr><td colspan="5" style="text-align:center;color:var(--muted)">Loading...</td></tr>
      </tbody>
    </table>
  </div>

  <div class="section-title">KV Store Stats (Direct Connection)</div>
  <div class="card" style="padding: 16px 20px;">
    <div id="kv-stats-detail">Loading...</div>
  </div>

  <div class="last-update">Last updated: <span id="last-update">--</span></div>
</div>

<script>
async function refresh() {
  try {
    const r = await fetch('/admin/metrics');
    const data = await r.json();

    document.getElementById('error-banner').style.display = 'none';

    document.getElementById('fe-id').textContent = data.server_id || '--';

    const nodes = data.nodes || [];
    const up = nodes.filter(n => n.alive).length;
    const down = nodes.length - up;

    const upEl = document.getElementById('nodes-up');
    upEl.textContent = up + ' / ' + nodes.length;
    upEl.className = 'value' + (up === nodes.length ? ' up' : '');

    const downEl = document.getElementById('nodes-down');
    downEl.textContent = down;
    downEl.className = 'value' + (down > 0 ? ' down' : ' up');

    const kvEl = document.getElementById('kv-direct');
    kvEl.textContent = data.kv_alive ? 'Online' : 'Offline';
    kvEl.className = 'value' + (data.kv_alive ? ' up' : ' down');

    const tbody = document.getElementById('nodes-tbody');
    if (nodes.length === 0) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted)">No nodes reported by coordinator (coordinator may be offline)</td></tr>';
    } else {
      tbody.innerHTML = nodes.map(n => `
        <tr>
          <td><strong>${esc(n.id)}</strong></td>
          <td>${esc(n.host)}:${n.port}</td>
          <td>
            <span class="status-dot ${n.alive ? 'alive' : 'dead'}"></span>
            ${n.alive ? 'Online' : 'Down'}
          </td>
          <td>${n.lsn}</td>
          <td>${n.missed}</td>
        </tr>`).join('');
    }

    const statsEl = document.getElementById('kv-stats-detail');
    if (data.kv_stats && data.kv_stats.length > 0) {
      const parts = data.kv_stats.split(/\s+/);
      let html = '';
      for (const p of parts) {
        const kv = p.split('=');
        if (kv.length === 2) {
          const labels = {rows: 'Total Rows', ops: 'Total Ops', lsn: 'LSN'};
          html += '<div class="kv-stat"><div class="stat-label">' +
                  (labels[kv[0]] || kv[0]) +
                  '</div><div class="stat-value">' + esc(kv[1]) + '</div></div>';
        }
      }
      if (html) {
        statsEl.innerHTML = html;
      } else {
        statsEl.innerHTML = '<span style="color:var(--muted)">Raw: ' + esc(data.kv_stats) + '</span>';
      }
    } else {
      statsEl.innerHTML = '<span style="color:var(--muted)">KV node not reachable</span>';
    }

    document.getElementById('last-update').textContent = new Date().toLocaleTimeString();

  } catch (err) {
    document.getElementById('error-banner').textContent = 'Failed to fetch metrics: ' + err.message;
    document.getElementById('error-banner').style.display = 'block';
  }
}

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                  .replace(/"/g,'&quot;');
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
)HTML";
    return HttpResponse::ok(html, "text/html; charset=utf-8");
}

// ---------------------------------------------------------------------------
// Static file server (for CSS/JS if moved out of inline HTML)
// ---------------------------------------------------------------------------
HttpResponse FEServer::handle_static(const HttpRequest& req) {
    // Security: prevent directory traversal
    std::string path = req.path.substr(8);  // strip "/static/"
    if (path.find("..") != std::string::npos) return HttpResponse::forbidden();

    std::string full = cfg_.static_dir + "/" + path;
    std::ifstream f(full, std::ios::binary);
    if (!f) return HttpResponse::not_found();

    std::string content((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());

    // Simple MIME detection
    std::string mime = "application/octet-stream";
    if (path.size()>=4 && path.substr(path.size()-4)==".css")  mime = "text/css";
    else if (path.size()>=3 && path.substr(path.size()-3)==".js")   mime = "application/javascript";
    else if (path.size()>=4 && path.substr(path.size()-4)==".png")  mime = "image/png";
    else if (path.size()>=4 && path.substr(path.size()-4)==".svg")  mime = "image/svg+xml";

    return HttpResponse::ok(content, mime);
}

// ---------------------------------------------------------------------------
// SSE handler -- holds connection open and streams new email events (F1)
// This runs in its own thread (from thread pool).
// The SMTP server writes to "notify:{user}" col "latest" when new mail arrives.
// We poll that key every 500ms and push SSE events on change.
// ---------------------------------------------------------------------------
void FEServer::handle_sse(int fd, const HttpRequest&, const std::string& user) {
    // Send SSE response headers
    std::string headers =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n"
        "Transfer-Encoding: chunked\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n";
    ::send(fd, headers.data(), headers.size(), MSG_NOSIGNAL);

    std::string last_notify;
    int keepalive_ticks = 0;

    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        // Check for new email notification
        std::string notify = kv_->get_str("notify:" + user, "latest");
        if (!notify.empty() && notify != last_notify) {
            last_notify = notify;

            // Fetch the most recent email metadata from the notification value
            // Format: "uid:<uid>" stored by SMTP server
            std::string uid;
            if (notify.rfind("uid:", 0) == 0) uid = notify.substr(4);

            if (!uid.empty()) {
                // Build minimal JSON for the browser to display the new row
                std::string from    = kv_->get_str(user + ":mail", "msg:" + uid);
                // from is JSON {from,subject,time} -- send as-is
                std::string payload;
                if (from.empty()) {
                    payload = std::string("{\"uid\":\"") + uid
                            + "\",\"from\":\"(new email)\",\"subject\":\"\"}";
                } else {
                    payload = from;
                    // Inject uid field if not already present
                    if (payload.find("\"uid\"") == std::string::npos && !payload.empty()) {
                        payload = std::string("{\"uid\":\"") + uid + "\"," + payload.substr(1);
                    }
                }
                send_sse_event(fd, "new_email", payload);
            }
        }

        // Keep-alive ping every 15 seconds (prevents proxy timeouts)
        ++keepalive_ticks;
        if (keepalive_ticks >= 30) {
            keepalive_ticks = 0;
            std::string ping = ": keep-alive\n\n";
            std::string chunk = std::to_string(ping.size()) + "\r\n" + ping + "\r\n";
            if (::send(fd, chunk.data(), chunk.size(), MSG_NOSIGNAL) <= 0) break;
        }
    }
}

// ---------------------------------------------------------------------------
// Stub handlers -- filled in by each team member
// Liudawei: handle_inbox, handle_get_email, handle_send_email, handle_delete_email
// Yke:      handle_drive_list, handle_upload, handle_download, handle_rename,
//           handle_move, handle_mkdir, handle_delete_path
// ---------------------------------------------------------------------------










namespace {
std::string drive_json_escape(const std::string& s) {
    std::string out;
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"': out += "\\\""; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out += c; break;
        }
    }
    return out;
}

std::string drive_new_uid() {
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::mt19937 rng(static_cast<unsigned>(ns));
    std::uniform_int_distribution<int> dist(0, 0xFFFF);
    std::ostringstream oss;
    oss << ns << "_" << std::hex << std::setw(4) << std::setfill('0') << dist(rng);
    return oss.str();
}

std::vector<std::string> split_nl(const std::string& s) {
    std::vector<std::string> out;
    std::istringstream ss(s);
    std::string line;
    while (std::getline(ss, line)) if (!line.empty()) out.push_back(line);
    return out;
}

std::string join_nl(const std::vector<std::string>& v) {
    std::string out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) out += "\n";
        out += v[i];
    }
    return out;
}

std::string meta_json(const std::string& uid, const std::string& name,
                      size_t bytes, const std::string& mime,
                      const std::string& path) {
    return std::string("{")
         + "\"uid\":\"" + drive_json_escape(uid) + "\"," 
         + "\"name\":\"" + drive_json_escape(name) + "\"," 
         + "\"size\":\"" + std::to_string(bytes) + " B\"," 
         + "\"type\":\"" + drive_json_escape(mime) + "\"," 
         + "\"path\":\"" + drive_json_escape(path) + "\"}";
}

std::string meta_uid(const std::string& meta) {
    const std::string key = "\"uid\":\"";
    auto pos = meta.find(key);
    if (pos == std::string::npos) return "";
    pos += key.size();
    auto end = meta.find('"', pos);
    if (end == std::string::npos) return "";
    return meta.substr(pos, end - pos);
}
}

HttpResponse FEServer::handle_drive_list(const HttpRequest& req, const std::string& user) {
    std::string path = req.param("path");
    if (path.empty()) path = "/";
    std::string row = user + ":drive";
    auto items = split_nl(kv_->get_str(row, "index"));
    std::string body = "{\"ok\":true,\"items\":[";
    bool first = true;
    for (const auto& item_path : items) {
        if (path != "/" && item_path.rfind(path, 0) != 0) continue;
        std::string meta = kv_->get_str(row, "meta:" + item_path);
        if (meta.empty()) continue;
        if (!first) body += ",";
        first = false;
        body += meta;
    }
    body += "]}";
    return HttpResponse::json(body);
}

HttpResponse FEServer::handle_upload(const HttpRequest& req, const std::string& user) {
    if (!req.is_multipart())
        return HttpResponse::json(R"({"ok":false,"error":"expected multipart upload"})");
    auto parts = parse_multipart(req.body, req.header("content-type"));
    std::string folder = "/";
    std::string filename;
    std::string mime = "application/octet-stream";
    std::string bytes;
    for (const auto& p : parts) {
        if (p.name == "path") folder = p.data;
        if (p.name == "file") {
            filename = p.filename.empty() ? "upload.bin" : p.filename;
            bytes = p.data;
            if (!p.content_type.empty()) mime = p.content_type;
        }
    }
    if (filename.empty())
        return HttpResponse::json(R"({"ok":false,"error":"missing file"})");
    if (folder.empty()) folder = "/";
    if (folder.back() != '/') folder += '/';
    std::string path = folder + filename;
    if (path.rfind("//", 0) == 0) path.erase(0,1);
    std::string uid = drive_new_uid();
    std::string drive_row = user + ":drive";
    std::string file_row = user + ":file:" + uid;
    kv_->put(file_row, "data", bytes);
    kv_->put(file_row, "name", filename);
    kv_->put(file_row, "mime", mime);
    kv_->put(drive_row, "meta:" + path, meta_json(uid, filename, bytes.size(), mime, path));
    for (int i = 0; i < 5; ++i) {
        std::string old_index = kv_->get_str(drive_row, "index");
        auto items = split_nl(old_index);
        bool exists = false;
        for (const auto& x : items) if (x == path) exists = true;
        if (!exists) items.push_back(path);
        std::string new_index = join_nl(items);
        if (old_index.empty()) {
            if (kv_->put(drive_row, "index", new_index)) break;
        } else if (kv_->cput(drive_row, "index", old_index, new_index)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return HttpResponse::json(std::string("{\"ok\":true,\"uid\":\"") + uid + "\",\"path\":\"" + drive_json_escape(path) + "\"}");
}

HttpResponse FEServer::handle_download(const HttpRequest&, const std::string& user,
                                        const std::string& uid) {
    std::string row = user + ":file:" + uid;
    std::string data = kv_->get_str(row, "data");
    if (data.empty()) return HttpResponse::not_found();
    std::string name = kv_->get_str(row, "name");
    if (name.empty()) name = uid;
    std::string mime = kv_->get_str(row, "mime");
    if (mime.empty()) mime = "application/octet-stream";
    HttpResponse resp = HttpResponse::ok(data, mime);
    resp.headers["Content-Disposition"] = "attachment; filename=\"" + name + "\"";
    return resp;
}

HttpResponse FEServer::handle_rename(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string old_path = params["path"];
    std::string new_name = params["name"];
    if (old_path.empty() || new_name.empty())
        return HttpResponse::json(R"({"ok":false,"error":"missing path or name"})");
    std::string row = user + ":drive";
    std::string meta = kv_->get_str(row, "meta:" + old_path);
    if (meta.empty()) return HttpResponse::json(R"({"ok":false,"error":"not found"})");
    std::string uid = meta_uid(meta);
    if (uid.empty()) return HttpResponse::json(R"({"ok":false,"error":"corrupt metadata"})");
    auto slash = old_path.find_last_of('/');
    std::string parent = (slash == std::string::npos || slash == 0) ? "/" : old_path.substr(0, slash);
    std::string new_path = (parent == "/") ? "/" + new_name : parent + "/" + new_name;
    std::string file_row = user + ":file:" + uid;
    std::string mime = kv_->get_str(file_row, "mime");
    if (mime.empty()) mime = "application/octet-stream";
    std::string data = kv_->get_str(file_row, "data");
    kv_->put(file_row, "name", new_name);
    kv_->put(row, "meta:" + new_path, meta_json(uid, new_name, data.size(), mime, new_path));
    kv_->del(row, "meta:" + old_path);
    for (int i = 0; i < 5; ++i) {
        std::string old_index = kv_->get_str(row, "index");
        auto items = split_nl(old_index);
        for (auto& x : items) if (x == old_path) x = new_path;
        std::string new_index = join_nl(items);
        if (!old_index.empty() && kv_->cput(row, "index", old_index, new_index)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return HttpResponse::json(std::string("{\"ok\":true,\"path\":\"") + drive_json_escape(new_path) + "\"}");
}

HttpResponse FEServer::handle_move(const HttpRequest&, const std::string&) {
    return HttpResponse::json(R"({"ok":false,"error":"move deferred until Demo II"})");
}

HttpResponse FEServer::handle_mkdir(const HttpRequest&, const std::string&) {
    return HttpResponse::json(R"({"ok":false,"error":"folders deferred until Demo II"})");
}

HttpResponse FEServer::handle_delete_path(const HttpRequest& req, const std::string& user) {
    auto params = parse_urlencoded(req.body);
    std::string path = params["path"];
    if (path.empty()) return HttpResponse::json(R"({"ok":false,"error":"missing path"})");
    std::string row = user + ":drive";
    std::string meta = kv_->get_str(row, "meta:" + path);
    if (meta.empty()) return HttpResponse::json(R"({"ok":false,"error":"not found"})");
    std::string uid = meta_uid(meta);
    kv_->del(row, "meta:" + path);
    if (!uid.empty()) {
        std::string file_row = user + ":file:" + uid;
        kv_->del(file_row, "data");
        kv_->del(file_row, "name");
        kv_->del(file_row, "mime");
    }
    for (int i = 0; i < 5; ++i) {
        std::string old_index = kv_->get_str(row, "index");
        auto items = split_nl(old_index);
        std::vector<std::string> kept;
        for (const auto& x : items) if (x != path) kept.push_back(x);
        std::string new_index = join_nl(kept);
        if (!old_index.empty() && kv_->cput(row, "index", old_index, new_index)) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return HttpResponse::json(R"({"ok":true})");
}
