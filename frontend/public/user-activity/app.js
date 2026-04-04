const STAGES = ['homepage','category','product','cart','checkout','done'];
const STAGE_LABELS = {
  homepage: ['home',         'Home'],
  category: ['layout-grid', 'Category'],
  product:  ['package',     'Product'],
  cart:     ['shopping-cart','Cart'],
  checkout: ['credit-card', 'Checkout'],
  done:     ['check-circle','Ordered'],
};
const COLORS  = ['c0','c1','c2','c3','c4','c5','c6','c7'];
const DEV_ICO = { mobile:'smartphone', desktop:'monitor', tablet:'tablet-smartphone' };

const ALL_PRODUCTS = [
    { name: 'iPhone 15 Pro', price: 134900 },
    { name: 'Sony Alpha A7 IV', price: 214990 },
    { name: 'Samsung Galaxy S24 Ultra', price: 129999 },
    { name: 'OnePlus 12', price: 64999 },
    { name: 'Nothing Phone (2)', price: 39999 },
    { name: 'iPad Air M2', price: 59900 },
    { name: 'Samsung Tab S9 FE', price: 34999 },
    { name: 'Bose QuietComfort', price: 32900 },
    { name: 'Sony WH-1000XM5', price: 29990 },
    { name: 'AirPods Pro (2nd Gen)', price: 24900 },
    { name: 'boAt Airdopes 141', price: 1199 },
    { name: 'Noise Buds VS102', price: 1499 },
    { name: 'Kindle Paperwhite', price: 13999 },
    { name: 'Mechanical Keyboard (K2)', price: 12499 },
    { name: 'Gaming Mouse G502', price: 7495 },
    { name: 'Powerbank 20000mAh', price: 1999 },
    { name: 'Fast Charger 65W', price: 2499 },
    { name: 'Leather Messenger Bag', price: 18500 }
];

let USERS = [];

async function loadUsers() {
  try {
    const resp = await fetch('./users.json?t=' + Date.now());
    if (resp.ok) { 
        const data = await resp.json();
        USERS = buildUsers(data).slice(0, 20); 
        return; 
    }
  } catch (_) {}
  try {
    const xhr = new XMLHttpRequest();
    xhr.open('GET', './users.json', false);
    xhr.send(null);
    if (xhr.status === 0 || xhr.status === 200) {
      const data = JSON.parse(xhr.responseText);
      USERS = buildUsers(data).slice(0, 20);
    }
  } catch (e) { console.error(e); }
}

function buildUsers(raw) {
  return raw.map((u, i) => {
    const usr = {
        ...u,
        color:      COLORS[i % 8],
        _elapsed:   Math.floor(Math.random() * 8),
        _status:    'active',
        _nextPulse: 0,
        _watching:  false,
        _doneTime:  0,
        _intent:    ['high', 'med', 'low', 'express'][Math.floor(Math.random() * 4)],
    };
    if (STAGE_IDX[usr.stage] >= 2) {
        const itm = ALL_PRODUCTS[Math.floor(Math.random() * ALL_PRODUCTS.length)];
        usr.cart = [itm.name];
        usr.cartValue = itm.price;
    } else {
        usr.cart = [];
        usr.cartValue = 0;
    }
    return usr;
  });
}

let running = false, tick = 0, feedEvents = 0;
let filterMode = 'all', panelUser = null, panelAuto = true, panelThinking = false;
let stageFilter = null;
let feedTab     = 'activity';
let simInterval = null;

let dispatchHistory = [];
let dispatchedUsers = new Set();
let recoveredCount  = 0;

const ROTATE_EVERY  = 12;
let   autoRotateTick = 0;
let   autoRotateIdx  = 0;

const STAGE_ICONS = {
  homepage: 'home', category: 'layout-grid', product: 'package',
  cart: 'shopping-cart', checkout: 'credit-card', done: 'check-circle',
};

const STAGE_IDX = {};
STAGES.forEach((s, i) => STAGE_IDX[s] = i);

window.addEventListener('DOMContentLoaded', async () => {
  updateClock();
  setInterval(updateClock, 1000);
  await loadUsers();
  startSim();

  document.getElementById('filters').addEventListener('click', e => {
    const b = e.target.closest('.fb');
    if (!b) return;
    document.querySelectorAll('.fb').forEach(x => x.classList.remove('on'));
    b.classList.add('on');
    filterMode = b.dataset.f;
    renderTable();
  });

  document.getElementById('feedTabs').addEventListener('click', e => {
    const b = e.target.closest('.ftab');
    if (!b) return;
    document.querySelectorAll('.ftab').forEach(x => x.classList.remove('on'));
    b.classList.add('on');
    feedTab = b.dataset.tab;
    document.getElementById('feedBody').style.display     = feedTab === 'activity' ? '' : 'none';
    document.getElementById('dispatchBody').style.display = feedTab === 'dispatch'  ? '' : 'none';
    if (feedTab === 'dispatch') renderDispatchLog();
  });
});

function updateClock() {
  document.getElementById('clock').textContent =
    new Date().toLocaleTimeString('en-IN', { hour:'2-digit', minute:'2-digit', second:'2-digit' });
}

function startSim() {
  running = true;
  document.getElementById('liveDot').classList.add('on');
  document.getElementById('liveLabel').textContent = 'Live';
  USERS.forEach(u => {
    u._elapsed   = u.sticky ? Math.floor(Math.random() * 4) : Math.floor(Math.random() * 8);
    u._nextPulse = u.sticky ? Infinity : tick + u.pulse + Math.floor(Math.random() * u.pulse);
    u._status    = 'active';
    u._watching  = false;
  });
  simInterval = setInterval(simTick, 1000);
}

function simTick() {
  tick++;
  if (!panelThinking) autoRotateTick++;
  USERS.forEach(u => {
    if (u.stage === 'done') {
        u._status = 'done';
        u._doneTime++;
        if (u._doneTime > 45) recycleUser(u);
        return;
    }
    u._elapsed++;
    let moveProb = 0.12;
    if (u._intent === 'express') moveProb = 0.38;
    if (u._intent === 'low')     moveProb = 0.04;

    if (!u.sticky && tick >= u._nextPulse) {
      const was = u._status;
      u._elapsed = 0;
      u._nextPulse = tick + u.pulse + Math.floor(Math.random() * Math.max(1, u.pulse * 0.4));
      
      if (Math.random() < moveProb) {
        const si = STAGE_IDX[u.stage];
        if (si >= 0 && si < STAGES.length - 1) {
          u.stage = STAGES[si + 1];
          if (u.stage === 'product' && u.cart.length === 0) {
              const itm = ALL_PRODUCTS[Math.floor(Math.random()*ALL_PRODUCTS.length)];
              u.cart = [itm.name];
              u.cartValue = itm.price;
          }
          addFeed(u, 'move', 'advanced to ' + STAGE_LABELS[u.stage][1]);
        }
      }
      if (was !== 'active') addFeed(u, 'active', 'became active again');
    }
    const prev = u._status;
    if      (u._elapsed < 15) u._status = 'active';
    else if (u._elapsed < 30) u._status = 'idle';
    else                      u._status = 'inactive';
    
    if ((prev === 'inactive' || prev === 'idle') && u._status === 'active') {
      if (u._watching) {
        recoveredCount++;
        u._watching = false;
        const entry = [...dispatchHistory].reverse().find(d => d.userId === u.id && !d.recovered);
        if (entry) {
          entry.recovered   = true;
          entry.recoveredAt = nowStr();
        }
        addFeed(u, 'recover', 're-engaged session after intervention');
        if (feedTab === 'dispatch') renderDispatchLog();
        showToast('↩', u.name + ' re-engaged!', 'Agent recovery successful');
      }
    }
  });
  if (panelAuto && !panelThinking) {
    const targets = USERS.filter(u => u._status === 'inactive' && u._intent !== 'express' && !dispatchedUsers.has(u.id));
    if (targets.length > 0 && autoRotateTick >= ROTATE_EVERY) {
        autoRotateTick = 0;
        autoRotateIdx = (autoRotateIdx + 1) % targets.length;
        openPanel(targets[autoRotateIdx], false);
    } else if (targets.length > 0 && !panelUser) {
        openPanel(targets[0], false);
    }
  }
  updateStats(); renderFlow(); renderTable(); renderFunnel();
}

function recycleUser(u) {
    const first_names = ['Aryan', 'Sanya', 'Ishaan', 'Kiara', 'Vivaan', 'Zoya', 'Advait', 'Myra', 'Kabir', 'Shanaya'];
    const last_names = ['Mehra', 'Goel', 'Kapoor', 'Khanna', 'Malhotra', 'Suri', 'Oberoi', 'Bakshi', 'Bajaj', 'Thakur'];
    const cities = ['mumbai', 'delhi', 'bengaluru', 'hyderabad', 'pune', 'chennai', 'kolkata'];
    u.name = first_names[Math.floor(Math.random()*first_names.length)] + ' ' + last_names[Math.floor(Math.random()*last_names.length)];
    u.initials = u.name.split(' ').map(n=>n[0]).join('');
    u.id = 'USR-' + Math.floor(Math.random()*9000 + 1000);
    u.stage = 'homepage';
    u.pageLabel = 'Homepage';
    u.city = cities[Math.floor(Math.random()*cities.length)];
    u.cartValue = 0;
    u.cart = [];
    u._elapsed = 0;
    u._status = 'active';
    u._doneTime = 0;
    u._watching = false;
    u._intent = ['high', 'med', 'low', 'express'][Math.floor(Math.random() * 4)];
    u._nextPulse = tick + 10 + Math.floor(Math.random()*10);
    addFeed(u, 'active', 'new session identity (Infinite Engine)');
}

function updateStats() {
  const active   = USERS.filter(u => u._status === 'active' || u._status === 'done').length;
  const idle     = USERS.filter(u => u._status === 'idle').length;
  const inactive = USERS.filter(u => u._status === 'inactive').length;
  const cartRisk = USERS.filter(u => u._status === 'inactive').reduce((s, u) => s + u.cartValue, 0);
  const dispTotal = dispatchHistory.length;
  const uniqueTargets = dispatchedUsers.size;
  const recovRate = uniqueTargets > 0 ? Math.round((recoveredCount / uniqueTargets) * 100) : 0;
  document.getElementById('sActive').textContent    = active;
  document.getElementById('sIdle').textContent      = idle;
  document.getElementById('sInactive').textContent  = inactive;
  document.getElementById('sCart').textContent      = '₹' + cartRisk.toLocaleString('en-IN');
  document.getElementById('sNotifs').textContent    = dispTotal;
  const cappedRate = Math.min(recovRate, 100);
  document.getElementById('sRecovered').textContent = `${recoveredCount} (${cappedRate}%)`;
}

function renderFunnel() {
  const body = document.getElementById('funnelBody');
  if (!body || !running) return;
  const total = USERS.length;
  const reached = STAGES.map((s, i) => USERS.filter(u => STAGE_IDX[u.stage] >= i).length);
  body.innerHTML = STAGES.map((stage, i) => {
    const count = reached[i];
    const prevCount = i > 0 ? reached[i-1] : total;
    const conv = total > 0 ? Math.round((count / total) * 100) : 0;
    const drop = (i > 0 && prevCount > 0) ? Math.round(((prevCount - count) / prevCount) * 100) : 0;
    const label    = STAGE_LABELS[stage][1];
    const icon     = STAGE_LABELS[stage][0];
    const isActive = stageFilter === stage;
    const barColor = (drop > 50) ? 'var(--rd)' : (drop > 25) ? 'var(--am)' : 'var(--vi)';
    const atRisk   = USERS.filter(u => u.stage === stage && (u._status === 'inactive' || u._status === 'idle')).length;
    return `
      <div class="fn-step${isActive ? ' fn-active' : ''}" onclick="toggleStageFilter('${stage}')">
        <div class="fn-header">
          <div class="fn-icon"><i data-lucide="${icon}" style="width:14px;height:14px"></i></div>
          <div class="fn-info">
            <div class="fn-label">${label}</div>
            <div class="fn-nums">
              <span class="fn-count">${count} users</span>
              ${i > 0 ? `<span class="fn-drop" style="color:${drop > 30 ? 'var(--rd)' : 'var(--t3)'}">↓ ${drop}% drop-off</span>` : '<span class="fn-drop" style="color:var(--t3)">Awareness</span>'}
              ${atRisk > 0 ? `<span class="fn-risk">${atRisk} at risk</span>` : ''}
            </div>
          </div>
          <div class="fn-pct">${conv}%</div>
        </div>
        <div class="fn-bar-wrap"><div class="fn-bar" style="width:${conv}%;background:${barColor}"></div></div>
      </div>`;
  }).join('');
  if (typeof lucide !== 'undefined') lucide.createIcons();
}

function toggleStageFilter(stage) {
  stageFilter = (stageFilter === stage) ? null : stage;
  updateStageBadge();
  renderTable(); renderFlow(); renderFunnel();
}

function updateStageBadge() {
  const badge = document.getElementById('stageBadge');
  if (badge) badge.style.display = stageFilter ? 'flex' : 'none';
  if (stageFilter) document.getElementById('stageBadgeLabel').textContent = STAGE_LABELS[stageFilter][1] + ' stage only';
}

function renderFlow() {
  const wrap = document.getElementById('flowWrap');
  if (!running) return;
  wrap.innerHTML = STAGES.map(stage => {
    const usrs = USERS.filter(u => u.stage === stage);
    const inactiveCnt = usrs.filter(u => u._status === 'inactive').length;
    const idleCnt     = usrs.filter(u => u._status === 'idle').length;
    const riskPct     = usrs.length > 0 ? Math.round((inactiveCnt + (idleCnt*0.5)) / usrs.length * 100) : 0;
    const heatColor   = riskPct > 60 ? 'var(--rd)' : riskPct > 30 ? 'var(--am)' : 'var(--gn)';
    const isSelected  = stageFilter === stage;
    const avatars = usrs.slice(0, 15).map(u => {
      const intentClass = u._intent === 'express' ? 'fast' : '';
      return `<div class="fa ${u.color} s-${u._status} ${intentClass}" onclick="event.stopPropagation();openPanel(USERS.find(x=>x.id==='${u.id}'),true)">
        ${u.initials}
        <div class="tip"><strong>${u.name}</strong><br>${u.city}<br>Intent: ${u._intent.toUpperCase()}<br>Cart: ₹${u.cartValue.toLocaleString('en-IN')}</div>
      </div>`;
    }).join('');
    return `
      <div class="flow-stage ${isSelected ? 's-selected' : ''}" onclick="toggleStageFilter('${stage}')">
        <div class="fs-node"><i data-lucide="${STAGE_ICONS[stage]}" style="width:22px;height:22px"></i></div>
        <div class="fs-lbl">${STAGE_LABELS[stage][1]}</div>
        <div class="fs-cnt">${usrs.length}</div>
        <div class="fs-heat-wrap"><div class="fs-heat-bar" style="width:${riskPct}%;background:${heatColor}"></div></div>
        <div class="fs-avs">${avatars}${usrs.length > 15 ? `<div class="fa c-more">+${usrs.length-15}</div>` : ''}</div>
      </div>`;
  }).join('');
  if (typeof lucide !== 'undefined') lucide.createIcons();
}

function renderTable() {
  const tbody = document.getElementById('tbody');
  let list = USERS;
  if (stageFilter)     list = list.filter(u => u.stage === stageFilter);
  if (filterMode !== 'all')
    list = list.filter(u => u._status === filterMode || (filterMode === 'active' && u._status === 'done'));
  tbody.innerHTML = list.slice(0, 200).map(u => {
    const timerTxt = u.stage === 'done' ? '—' : u._elapsed + 's';
    const devIcon  = `<i data-lucide="${u.device === 'mobile'?'smartphone':u.device==='tablet'?'tablet-smartphone':'monitor'}" style="width:16px;height:16px;color:var(--t2)"></i>`;
    const isDispatched = dispatchedUsers.has(u.id);
    const recoveredEntry = dispatchHistory.find(d => d.userId === u.id && d.recovered);
    return `<tr onclick="openPanel(USERS.find(x=>x.id==='${u.id}'),true)">
      <td><div class="u-cell">
        <div class="u-av ${u.color}">${u.initials}</div>
        <div><div class="u-name">${u.name}</div><div class="u-id">${u.id} ${recoveredEntry ? '<span class="row-recovered">↩ Recovered</span>' : isDispatched ? '<span class="row-dispatched">→ Monitoring</span>' : ''}</div></div>
      </div></td>
      <td class="pg"><div class="pg-name">${u.pageLabel}</div><div class="pg-url">${u.url}</div></td>
      <td>${devIcon}</td>
      <td><span class="tc ${u._status==='inactive'?'dnr':u._status==='idle'?'warn':'ok'}">${timerTxt}</span></td>
      <td><span class="cv">${u.cartValue > 0 ? '₹'+u.cartValue.toLocaleString('en-IN') : '—'}</span></td>
      <td><span class="sb ${u._status}"><span class="sb-dot"></span>${u._status}</span></td>
      <td><button class="btn-n ${u._status==='inactive'?'urgent':''}" onclick="event.stopPropagation();openPanel(USERS.find(x=>x.id==='${u.id}'),true)">Agent Intervene</button></td>
    </tr>`;
  }).join('');
  if (typeof lucide !== 'undefined') lucide.createIcons();
}

function openPanel(user, manual) {
  if (!user) return;
  if (manual) { panelAuto = false; document.getElementById('pPinBar').style.display = 'flex'; }
  panelUser = user;
  document.getElementById('pEmpty').style.display   = 'none';
  document.getElementById('pContent').style.display = 'block';
  const av = document.getElementById('pAv');
  av.className   = 'p-av ' + user.color;
  av.textContent = user.initials;
  document.getElementById('pName').textContent = user.name;
  document.getElementById('pMeta').textContent = `${user.city} · ${user.device} · ${user.id}`;
  
  const ts = nowStr();
  document.getElementById('agentTime').textContent = ts;
  
  const log = document.getElementById('dispatchLog');
  log.innerHTML = `
    <div style="padding:24px 20px;text-align:center;color:var(--t3);font-size:11px;font-style:italic">
      <div class="loading-spinner" style="margin:0 auto 12px;width:18px;height:18px"></div>
      Analyzing behavior and preparing intervention channels...
    </div>`;
  panelThinking = true;
  
  buildUserDetails(user);
  
  setTimeout(() => {
    if (panelUser && panelUser.id === user.id) {
        buildDispatchLog(user, nowStr());
        panelThinking = false;
        if (!dispatchedUsers.has(user.id) || manual) {
            dispatchedUsers.add(user.id);
            user._watching = true;
            dispatchHistory.push({
                id: Date.now(), userId: user.id, userName: user.name, userColor: user.color, 
                userInitials: user.initials, channels: ['WhatsApp','Email','SMS','In-App'],
                timestamp: nowStr(), page: user.pageLabel, cartValue: user.cartValue, 
                recovered: false, recoveredAt: null,
            });
            updateStats();
            addFeed(user, 'notif', 'intervention channels active');
            if (feedTab === 'dispatch') renderDispatchLog();
        }
    }
  }, 3500);
}

function buildUserDetails(user) {
    const list = document.getElementById('pDetList');
    const items = [
        { lbl: 'Full Name', val: user.name },
        { lbl: 'User ID', val: user.id },
        { lbl: 'Current Location', val: user.city + ', India' },
        { lbl: 'Device Identity', val: user.device[0].toUpperCase() + user.device.slice(1) + ' Session' },
        { lbl: 'Buying Personality', val: user._intent.toUpperCase() === 'EXPRESS' ? '⚡ Express Shopper' : user._intent.toUpperCase() === 'HIGH' ? '🔥 High Intent' : '🛋️ Window Shopper' },
        { lbl: 'Active Page', val: user.pageLabel },
        { lbl: 'Session Duration', val: user._elapsed + ' seconds active' },
        { lbl: 'Cart Value', val: '₹' + user.cartValue.toLocaleString('en-IN') },
        { lbl: 'Items in Cart', val: user.cart.length > 0 ? user.cart.join(', ') : 'Browsing...' }
    ];
    list.innerHTML = items.map(i => `
        <div class="p-det-item">
            <div class="p-det-lbl">${i.lbl}</div>
            <div class="p-det-val">${i.val}</div>
        </div>
    `).join('');
}

function setPanelTab(tab) {
    document.querySelectorAll('.ptab-btn').forEach(b => b.classList.toggle('on', b.dataset.ptab === tab));
    document.getElementById('pTabIntervention').style.display = tab === 'intervention' ? 'block' : 'none';
    document.getElementById('pTabDetails').style.display = tab === 'details' ? 'block' : 'none';
}

function unpinPanel() {
  panelAuto = true;
  document.getElementById('pPinBar').style.display = 'none';
}

function buildDispatchLog(user, ts) {
  const fn  = user.name.split(' ')[0];
  const itm = user.cart.length > 0 ? user.cart[0] : 'items';
  const channels = [
    { icon:'message-circle', name:'WhatsApp', target:user.phone, msg:`Hey ${fn}! Your ${itm} is still waiting.` },
    { icon:'mail', name:'Email', target:user.email, msg:`Subject: ${fn}, complete your order! - We noticed you left ${itm}.` },
    { icon:'smartphone', name:'SMS', target:user.phone, msg:`Hi ${fn}, ${itm} is in your cart! Finish now: shop.adk.com` },
    { icon:'bell', name:'Push', target:'App', msg:`Special offer for ${fn}! Complete your purchase for ${itm} now.` },
  ];
  document.getElementById('dispatchLog').innerHTML = channels.map((ch, i) => `
    <div class="dispatch-row" style="animation-delay:${i*80}ms">
      <div class="dr-icon"><i data-lucide="${ch.icon}" style="width:18px;height:18px"></i></div>
      <div class="dr-content"><div class="dr-channel">${ch.name}</div><div class="dr-msg">${ch.msg}</div></div>
      <div class="dr-right"><div class="dr-time">${ts}</div><div class="dr-badge">Sent</div></div>
    </div>`).join('');
  if (typeof lucide !== 'undefined') lucide.createIcons();
}

function redispatch() {
  if (!panelUser) return;
  const btn = document.getElementById('btnRedispatch');
  btn.disabled = true;
  btn.innerHTML = '<i data-lucide="loader-2" class="spin" style="width:13px;height:13px"></i> Sending...';
  if (typeof lucide !== 'undefined') lucide.createIcons();
  setTimeout(() => {
    openPanel(panelUser, true);
    btn.disabled = false;
    btn.innerHTML = '<i data-lucide="refresh-cw" style="width:13px;height:13px"></i> Repeat Interventions';
    if (typeof lucide !== 'undefined') lucide.createIcons();
    showToast('✓', 'Re-sent to ' + panelUser.name, 'Multi-channel update successful');
  }, 1200);
}

function renderDispatchLog() {
  const body = document.getElementById('dispatchBody');
  if (!body) return;
  if (dispatchHistory.length === 0) {
    body.innerHTML = '<div class="dl-empty">No active agent interventions</div>';
    return;
  }
  body.innerHTML = [...dispatchHistory].reverse().map(d => `
    <div class="dl-entry ${d.recovered ? 'dl-rec' : ''}">
      <div class="dl-av ${d.userColor}">${d.userInitials}</div>
      <div class="dl-content">
        <div class="dl-top"><span class="dl-name">${d.userName}</span><span class="dl-time">${d.timestamp}</span></div>
        <div class="dl-meta">${d.page} · ₹${d.cartValue.toLocaleString('en-IN')}</div>
      </div>
      ${d.recovered ? `<div class="dl-badge dl-recovered">Recovered ${d.recoveredAt}</div>` : '<div class="dl-badge dl-watching">Monitoring</div>'}
    </div>`).join('');
  if (typeof lucide !== 'undefined') lucide.createIcons();
}

function addFeed(user, type, msg) {
  feedEvents++;
  const colors = { inactive:'var(--rd)', idle:'var(--am)', active:'var(--gn)', move:'var(--vi)', notif:'var(--cy)', recover:'var(--gn)' };
  const ts = nowStr();
  const el = document.createElement('div');
  el.className = 'fi';
  el.innerHTML = `<span class="fi-time">${ts}</span><span class="fi-dot" style="background:${colors[type]||'var(--t3)'}"></span><span class="fi-txt"><strong>${user.name}</strong> ${msg}</span>`;
  const feed = document.getElementById('feedBody');
  feed.prepend(el);
  if (feed.children.length > 50) feed.removeChild(feed.lastChild);
}

function showToast(icon, title, sub) {
  const el = document.createElement('div');
  el.className = 'toast';
  el.innerHTML = `<div class="toast-icon">${icon}</div><div><div class="toast-title">${title}</div><div class="toast-sub">${sub}</div></div>`;
  document.getElementById('toasts').prepend(el);
  setTimeout(() => el.remove(), 4000);
}

function nowStr() {
  return new Date().toLocaleTimeString('en-IN', { hour:'2-digit', minute:'2-digit', second:'2-digit' });
}
