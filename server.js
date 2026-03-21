'use strict';
const express = require('express');
const http = require('http');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const { Server } = require('socket.io');

// ── DB ────────────────────────────────────────────────────────────────────────
const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  family: 4
});

// Convert ? placeholders to $1,$2,... for PostgreSQL
function ph(sql) { let i=0; return sql.replace(/\?/g, ()=>`$${++i}`); }

async function all(sql, args = []) {
  const r = await db.query(ph(sql), args);
  return r.rows;
}
async function get(sql, args = []) { return (await all(sql, args))[0]; }
async function run(sql, args = []) {
  const isInsert = sql.trim().toUpperCase().startsWith('INSERT');
  const finalSql = isInsert && !sql.toUpperCase().includes('RETURNING') ? sql + ' RETURNING id' : sql;
  const r = await db.query(ph(finalSql), args);
  return { lastInsertRowid: r.rows[0]?.id, rowsAffected: r.rowCount };
}

async function initDb() {
  const stmts = [
    `CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL,
      email TEXT UNIQUE, password_hash TEXT NOT NULL,
      role TEXT DEFAULT 'user', is_banned INTEGER DEFAULT 0,
      xp INTEGER DEFAULT 0, coins INTEGER DEFAULT 500,
      levels_completed INTEGER DEFAULT 0, rank_points INTEGER DEFAULT 1000,
      wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
      active_title TEXT DEFAULT NULL, created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS level_completions (
      id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id),
      level_number INTEGER, score INTEGER DEFAULT 0, time_taken INTEGER DEFAULT 0,
      words_found INTEGER DEFAULT 0, completed_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS friendships (
      id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id),
      friend_id INTEGER REFERENCES users(id), status TEXT DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY, sender_id INTEGER REFERENCES users(id),
      receiver_id INTEGER REFERENCES users(id), content TEXT NOT NULL,
      read INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS ranked_matches (
      id SERIAL PRIMARY KEY, player1_id INTEGER, player2_id INTEGER,
      winner_id INTEGER, level_number INTEGER DEFAULT 1,
      player1_score INTEGER DEFAULT 0, player2_score INTEGER DEFAULT 0,
      status TEXT DEFAULT 'waiting', created_at TIMESTAMP DEFAULT NOW(), ended_at TIMESTAMP
    )`,
    `CREATE TABLE IF NOT EXISTS user_titles (
      id SERIAL PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      title_id TEXT NOT NULL, unlocked_at TIMESTAMP DEFAULT NOW(), UNIQUE(user_id, title_id)
    )`,
    `CREATE TABLE IF NOT EXISTS reports (
      id SERIAL PRIMARY KEY, reporter_id INTEGER REFERENCES users(id),
      reported_id INTEGER REFERENCES users(id), reason TEXT NOT NULL,
      status TEXT DEFAULT 'open', handled_by INTEGER REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS warnings (
      id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id),
      issued_by INTEGER REFERENCES users(id), reason TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE INDEX IF NOT EXISTS idx_ut ON user_titles(user_id)`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS total_online_seconds INTEGER DEFAULT 0`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS timeout_until TIMESTAMP DEFAULT NULL`,
    `CREATE TABLE IF NOT EXISTS staff_action_requests (
      id SERIAL PRIMARY KEY, requester_id INTEGER REFERENCES users(id),
      target_id INTEGER REFERENCES users(id), action TEXT NOT NULL,
      reason TEXT, timeout_hours INTEGER DEFAULT NULL,
      status TEXT DEFAULT 'pending', created_at TIMESTAMP DEFAULT NOW(),
      resolved_at TIMESTAMP, resolved_by INTEGER REFERENCES users(id)
    )`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_ip TEXT`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_fingerprint TEXT`,
    `CREATE TABLE IF NOT EXISTS banned_ips (
      id SERIAL PRIMARY KEY, ip TEXT UNIQUE NOT NULL,
      reason TEXT DEFAULT '', banned_by INTEGER REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS banned_devices (
      id SERIAL PRIMARY KEY, fingerprint TEXT UNIQUE NOT NULL,
      reason TEXT DEFAULT '', banned_by INTEGER REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS user_icons (
      id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
      icon_id TEXT NOT NULL, purchased_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(user_id, icon_id)
    )`,
    `ALTER TABLE users ADD COLUMN IF NOT EXISTS active_icon TEXT DEFAULT NULL`,
    `ALTER TABLE forum_posts ADD COLUMN IF NOT EXISTS media TEXT DEFAULT NULL`,
    `ALTER TABLE forum_replies ADD COLUMN IF NOT EXISTS media TEXT DEFAULT NULL`,
    `CREATE TABLE IF NOT EXISTS staff_messages (
      id SERIAL PRIMARY KEY, author_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      content TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS forum_posts (
      id SERIAL PRIMARY KEY, author_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      section TEXT NOT NULL DEFAULT 'general', title TEXT NOT NULL,
      content TEXT NOT NULL, pinned INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS forum_replies (
      id SERIAL PRIMARY KEY, post_id INTEGER REFERENCES forum_posts(id) ON DELETE CASCADE,
      author_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      content TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW()
    )`,
  ];
  for (const s of stmts) await db.query(s);
  // Migrate any legacy co-owner roles to staff-manager
  await run("UPDATE users SET role='staff-manager' WHERE role='co-owner'");
  const owner = await get("SELECT id FROM users WHERE username='AMGProdZ27'");
  if (!owner) {
    const hash = await bcrypt.hash('20261248', 10);
    await run("INSERT INTO users (username,password_hash,role) VALUES (?,?,?)", ['AMGProdZ27',hash,'owner']);
  } else { await run("UPDATE users SET role='owner' WHERE username='AMGProdZ27'"); }
  await run(`DELETE FROM user_titles WHERE title_id IN ('ranked_1','ranked_top3','ranked_top10','ranked_top25','ranked_top50') AND user_id NOT IN (SELECT id FROM users WHERE (wins+losses)>0)`);
  await run(`DELETE FROM user_titles WHERE title_id IN ('xp_1','xp_top3','xp_top10','xp_top25') AND user_id NOT IN (SELECT id FROM users WHERE xp>0)`);
  await run(`DELETE FROM user_titles WHERE title_id IN ('coins_1','coins_top3','coins_top10','coins_top25','levels_1','levels_top3','levels_top10','levels_top25') AND user_id NOT IN (SELECT id FROM users WHERE levels_completed>0)`);
  console.log('DB ready');
}

// ── BAN CACHE ─────────────────────────────────────────────────────────────────
let bannedIpSet = new Set();
let bannedFpSet = new Set();
async function loadBanCaches() {
  const ips = await all('SELECT ip FROM banned_ips').catch(() => []);
  const fps = await all('SELECT fingerprint FROM banned_devices').catch(() => []);
  bannedIpSet = new Set(ips.map(r => r.ip));
  bannedFpSet = new Set(fps.map(r => r.fingerprint));
}
function getIp(req) {
  return (req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || '').replace('::ffff:', '');
}

// ── TITLES ────────────────────────────────────────────────────────────────────
const TITLES = {
  ranked_1:{id:'ranked_1',name:'The Champion',icon:'🏆',rarity:'legendary',color:'#e74c3c',desc:'Claim #1 on Ranked leaderboard',category:'Ranked Leaderboard'},
  ranked_top3:{id:'ranked_top3',name:'Elite Duelist',icon:'⚔️',rarity:'epic',color:'#ff6b35',desc:'Reach top 3 on Ranked leaderboard',category:'Ranked Leaderboard'},
  ranked_top10:{id:'ranked_top10',name:'Ranked Contender',icon:'🛡️',rarity:'rare',color:'#9b59b6',desc:'Reach top 10 on Ranked leaderboard',category:'Ranked Leaderboard'},
  ranked_top25:{id:'ranked_top25',name:'Challenger',icon:'⚡',rarity:'uncommon',color:'#2980b9',desc:'Reach top 25 on Ranked leaderboard',category:'Ranked Leaderboard'},
  ranked_top50:{id:'ranked_top50',name:'Competitor',icon:'🎯',rarity:'common',color:'#7f8c8d',desc:'Appear on Ranked leaderboard',category:'Ranked Leaderboard'},
  xp_1:{id:'xp_1',name:'Scholar Supreme',icon:'📜',rarity:'legendary',color:'#f39c12',desc:'Hold the #1 XP record',category:'XP Leaderboard'},
  xp_top3:{id:'xp_top3',name:'Grand Scholar',icon:'📚',rarity:'epic',color:'#e67e22',desc:'Top 3 in XP',category:'XP Leaderboard'},
  xp_top10:{id:'xp_top10',name:'Wordsmith Elite',icon:'✍️',rarity:'rare',color:'#8e44ad',desc:'Top 10 in XP',category:'XP Leaderboard'},
  xp_top25:{id:'xp_top25',name:'Experienced',icon:'⭐',rarity:'uncommon',color:'#f1c40f',desc:'Top 25 in XP',category:'XP Leaderboard'},
  coins_1:{id:'coins_1',name:'The Tycoon',icon:'💰',rarity:'legendary',color:'#f1c40f',desc:'Hold the #1 coin fortune',category:'Coins Leaderboard'},
  coins_top3:{id:'coins_top3',name:'Gold Baron',icon:'🪙',rarity:'epic',color:'#d4a017',desc:'Top 3 in coins',category:'Coins Leaderboard'},
  coins_top10:{id:'coins_top10',name:'Wealthy Wordsmith',icon:'💎',rarity:'rare',color:'#b8860b',desc:'Top 10 in coins',category:'Coins Leaderboard'},
  coins_top25:{id:'coins_top25',name:'Coin Collector',icon:'🏦',rarity:'uncommon',color:'#c9a227',desc:'Top 25 in coins',category:'Coins Leaderboard'},
  levels_1:{id:'levels_1',name:'Infinite Mind',icon:'∞',rarity:'legendary',color:'#1abc9c',desc:'Most levels completed',category:'Levels Leaderboard'},
  levels_top3:{id:'levels_top3',name:'Level Master',icon:'📖',rarity:'epic',color:'#27ae60',desc:'Top 3 in levels',category:'Levels Leaderboard'},
  levels_top10:{id:'levels_top10',name:'Dedicated Player',icon:'🎖️',rarity:'rare',color:'#2ecc71',desc:'Top 10 in levels',category:'Levels Leaderboard'},
  levels_top25:{id:'levels_top25',name:'Persistent',icon:'🔁',rarity:'uncommon',color:'#16a085',desc:'Top 25 in levels',category:'Levels Leaderboard'},
  tier_master:{id:'tier_master',name:'Master',icon:'👑',rarity:'legendary',color:'#e74c3c',desc:'Achieve Master rank (2500+ RP)',category:'Rank Tier'},
  tier_diamond:{id:'tier_diamond',name:'Diamond Duelist',icon:'💎',rarity:'epic',color:'#9b59b6',desc:'Achieve Diamond rank (2000+ RP)',category:'Rank Tier'},
  tier_platinum:{id:'tier_platinum',name:'Platinum Puzzler',icon:'💠',rarity:'rare',color:'#00b4d8',desc:'Achieve Platinum rank (1600+ RP)',category:'Rank Tier'},
  tier_gold:{id:'tier_gold',name:'Gold Grappler',icon:'🥇',rarity:'uncommon',color:'#d4a017',desc:'Achieve Gold rank (1300+ RP)',category:'Rank Tier'},
  wins_1:{id:'wins_1',name:'First Blood',icon:'🩸',rarity:'common',color:'#95a5a6',desc:'Win your first ranked match',category:'Ranked Wins'},
  wins_10:{id:'wins_10',name:'Rising Fighter',icon:'🌟',rarity:'uncommon',color:'#e67e22',desc:'Win 10 ranked matches',category:'Ranked Wins'},
  wins_50:{id:'wins_50',name:'Battle Hardened',icon:'🔥',rarity:'rare',color:'#e74c3c',desc:'Win 50 ranked matches',category:'Ranked Wins'},
  wins_100:{id:'wins_100',name:'Centurion',icon:'💯',rarity:'epic',color:'#c0392b',desc:'Win 100 ranked matches',category:'Ranked Wins'},
  wins_250:{id:'wins_250',name:'Warlord',icon:'⚔️',rarity:'legendary',color:'#922b21',desc:'Win 250 ranked matches',category:'Ranked Wins'},
  levels_10:{id:'levels_10',name:'Beginner',icon:'🌱',rarity:'common',color:'#95a5a6',desc:'Complete 10 levels',category:'Level Milestones'},
  levels_50:{id:'levels_50',name:'Learner',icon:'📝',rarity:'uncommon',color:'#27ae60',desc:'Complete 50 levels',category:'Level Milestones'},
  levels_100:{id:'levels_100',name:'Century Scholar',icon:'💫',rarity:'rare',color:'#2980b9',desc:'Complete 100 levels',category:'Level Milestones'},
  levels_500:{id:'levels_500',name:'Lexicon Lord',icon:'📕',rarity:'epic',color:'#8e44ad',desc:'Complete 500 levels',category:'Level Milestones'},
  levels_1000:{id:'levels_1000',name:'Eternal Scribe',icon:'🪄',rarity:'legendary',color:'#e74c3c',desc:'Complete 1000 levels',category:'Level Milestones'},
};

function computeEarned(user, pos) {
  const e = [];
  if(pos.rank<=1)e.push('ranked_1'); if(pos.rank<=3)e.push('ranked_top3'); if(pos.rank<=10)e.push('ranked_top10'); if(pos.rank<=25)e.push('ranked_top25'); if(pos.rank<=50)e.push('ranked_top50');
  if(pos.xp<=1)e.push('xp_1'); if(pos.xp<=3)e.push('xp_top3'); if(pos.xp<=10)e.push('xp_top10'); if(pos.xp<=25)e.push('xp_top25');
  if(pos.coins<=1)e.push('coins_1'); if(pos.coins<=3)e.push('coins_top3'); if(pos.coins<=10)e.push('coins_top10'); if(pos.coins<=25)e.push('coins_top25');
  if(pos.levels<=1)e.push('levels_1'); if(pos.levels<=3)e.push('levels_top3'); if(pos.levels<=10)e.push('levels_top10'); if(pos.levels<=25)e.push('levels_top25');
  if(user.rank_points>=2500)e.push('tier_master'); if(user.rank_points>=2000)e.push('tier_diamond'); if(user.rank_points>=1600)e.push('tier_platinum'); if(user.rank_points>=1300)e.push('tier_gold');
  if(user.wins>=250)e.push('wins_250'); if(user.wins>=100)e.push('wins_100'); if(user.wins>=50)e.push('wins_50'); if(user.wins>=10)e.push('wins_10'); if(user.wins>=1)e.push('wins_1');
  if(user.levels_completed>=1000)e.push('levels_1000'); if(user.levels_completed>=500)e.push('levels_500'); if(user.levels_completed>=100)e.push('levels_100'); if(user.levels_completed>=50)e.push('levels_50'); if(user.levels_completed>=10)e.push('levels_10');
  return [...new Set(e)];
}

async function getLbPos(userId) {
  const lbs = {
    rank: await all('SELECT id FROM users WHERE is_banned=0 AND (wins+losses)>0 ORDER BY rank_points DESC LIMIT 50'),
    xp: await all('SELECT id FROM users WHERE is_banned=0 AND xp>0 ORDER BY xp DESC LIMIT 50'),
    coins: await all('SELECT id FROM users WHERE is_banned=0 AND levels_completed>0 ORDER BY coins DESC LIMIT 50'),
    levels: await all('SELECT id FROM users WHERE is_banned=0 AND levels_completed>0 ORDER BY levels_completed DESC LIMIT 50'),
  };
  const pos = {};
  for (const [k, rows] of Object.entries(lbs)) {
    const i = rows.findIndex(r => Number(r.id) === userId);
    pos[k] = i >= 0 ? i + 1 : 999;
  }
  return pos;
}

async function awardTitles(userId) {
  const user = await get('SELECT * FROM users WHERE id=?', [userId]);
  if (!user) return [];
  const pos = await getLbPos(userId);
  const earned = computeEarned(user, pos);
  const existing = (await all('SELECT title_id FROM user_titles WHERE user_id=?', [userId])).map(r => r.title_id);
  const newOnes = earned.filter(t => !existing.includes(t));
  for (const id of newOnes) try { await run('INSERT INTO user_titles (user_id,title_id) VALUES (?,?) ON CONFLICT DO NOTHING', [userId, id]); } catch {}
  return newOnes;
}

// ── EVENTS ────────────────────────────────────────────────────────────────────
const EVENTS = {
  // Auto-triggered (mild)
  lucky_find:  {id:'lucky_find', name:'Lucky Find',    icon:'🍀',desc:'+15% XP',         xpMult:1.15,coinMult:1,   scoreMult:1,   duration:10,auto:true},
  coin_shower: {id:'coin_shower',name:'Coin Shower',   icon:'🌧️',desc:'+15% Coins',      xpMult:1,   coinMult:1.15,scoreMult:1,   duration:10,auto:true},
  word_rush:   {id:'word_rush',  name:'Word Rush',     icon:'💨',desc:'+10% Score',       xpMult:1,   coinMult:1,   scoreMult:1.1, duration:10,auto:true},
  small_boost: {id:'small_boost',name:'Small Boost',   icon:'⬆️',desc:'+10% XP & Coins', xpMult:1.1, coinMult:1.1, scoreMult:1,   duration:12,auto:true},
  // Owner-triggered (mid-great)
  xp_storm:    {id:'xp_storm',  name:'XP Storm',      icon:'⚡',desc:'+50% XP',          xpMult:1.5, coinMult:1,   scoreMult:1,   duration:20,auto:false},
  gold_fever:  {id:'gold_fever',name:'Gold Fever',     icon:'💰',desc:'+75% Coins',       xpMult:1,   coinMult:1.75,scoreMult:1,   duration:20,auto:false},
  word_blitz:  {id:'word_blitz',name:'Word Blitz',     icon:'🔥',desc:'+50% Score',       xpMult:1,   coinMult:1,   scoreMult:1.5, duration:20,auto:false},
  double_down: {id:'double_down',name:'Double Down',   icon:'✨',desc:'2x XP & Coins',   xpMult:2,   coinMult:2,   scoreMult:1,   duration:15,auto:false},
  mega_event:  {id:'mega_event',name:'MEGA EVENT',     icon:'🌟',desc:'3x Everything',    xpMult:3,   coinMult:3,   scoreMult:3,   duration:10,auto:false},
  score_surge: {id:'score_surge',name:'Score Surge',   icon:'📈',desc:'2x Score',         xpMult:1,   coinMult:1,   scoreMult:2,   duration:15,auto:false},
};

const activeEvents = {};
const pendingRequests = new Map();
let nextReqId = 1;

function startEvent(eventId, source='auto', customDuration=null) {
  const e = EVENTS[eventId];
  if (!e) return null;
  const dur = (customDuration && customDuration > 0 && customDuration <= 120) ? customDuration : e.duration;
  const eWithDur = {...e, duration: dur};
  if (activeEvents[eventId]) clearTimeout(activeEvents[eventId].timeout);
  const endsAt = Date.now() + dur * 60 * 1000;
  activeEvents[eventId] = {
    event: eWithDur, endsAt,
    timeout: setTimeout(() => { delete activeEvents[eventId]; io.emit('event:ended', {id:eventId}); }, dur * 60 * 1000)
  };
  io.emit('event:started', {event:eWithDur, endsAt, source});
  return eWithDur;
}

function getMultipliers() {
  let xp=1, coin=1, score=1;
  for (const {event} of Object.values(activeEvents)) {
    xp = Math.max(xp, event.xpMult);
    coin = Math.max(coin, event.coinMult);
    score = Math.max(score, event.scoreMult);
  }
  return {xp, coin, score};
}

// Auto-trigger a random mild event every ~25 min, 40% chance
setInterval(() => {
  if (Math.random() < 0.4) {
    const evPool = Object.keys(EVENTS).filter(k => EVENTS[k].auto);
    startEvent(evPool[Math.floor(Math.random()*evPool.length)], 'auto');
  }
}, 25 * 60 * 1000);

// ── AUTH MIDDLEWARE ───────────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || 'crossword_secret_2024';
const ROLE_LEVELS = { user: 0, mod: 1, admin: 2, 'staff-manager': 3, owner: 4 };

function auth(req, res, next) {
  const a = req.headers.authorization;
  if (!a?.startsWith('Bearer ')) return res.status(401).json({ error: 'No token' });
  try { req.user = jwt.verify(a.split(' ')[1], JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'Invalid token' }); }
}
function requireRole(min) {
  return (req, res, next) => {
    if ((ROLE_LEVELS[req.user?.role] ?? 0) < (ROLE_LEVELS[min] ?? 99))
      return res.status(403).json({ error: 'Insufficient permissions' });
    next();
  };
}
async function attachRole(req, res, next) {
  try {
    const u = await get('SELECT role,is_banned FROM users WHERE id=?', [req.user.id]);
    if (!u) return res.status(401).json({ error: 'Not found' });
    if (u.is_banned) return res.status(403).json({ error: 'Banned' });
    req.user.role = u.role; next();
  } catch { res.status(500).json({ error: 'Server error' }); }
}
const sanitize = u => { const { password_hash, ...s } = u; return s; };

// ── APP ───────────────────────────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.json({ limit: '150mb' }));
app.use((req, res, next) => {
  const ip = getIp(req);
  if (bannedIpSet.has(ip)) return res.status(403).json({ error: 'Your IP address is banned.', code: 'IP_BANNED' });
  next();
});

// Auth
app.post('/api/auth/register', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'All fields required' });
  if (username.length < 3 || username.length > 20) return res.status(400).json({ error: 'Username 3-20 chars' });
  if (password.length < 6) return res.status(400).json({ error: 'Password min 6 chars' });
  try {
    if (await get('SELECT id FROM users WHERE username=?', [username]))
      return res.status(409).json({ error: 'Username taken' });
    const hash = await bcrypt.hash(password, 10);
    const { lastInsertRowid: id } = await run('INSERT INTO users (username,password_hash) VALUES (?,?)', [username, hash]);
    const user = await get('SELECT * FROM users WHERE id=?', [id]);
    res.json({ token: jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '7d' }), user: sanitize(user) });
  } catch (e) { console.error(e); res.status(500).json({ error: 'Server error' }); }
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'All fields required' });
  try {
    const user = await get('SELECT * FROM users WHERE username=? OR email=?', [username, username]);
    if (!user || !(await bcrypt.compare(password, user.password_hash)))
      return res.status(401).json({ error: 'Invalid credentials' });
    res.json({ token: jwt.sign({ id: user.id, username: user.username }, JWT_SECRET, { expiresIn: '7d' }), user: sanitize(user) });
  } catch { res.status(500).json({ error: 'Server error' }); }
});

// Game
app.post('/api/game/hint', auth, async (req, res) => {
  const user = await get('SELECT coins FROM users WHERE id=?', [req.user.id]);
  if (!user) return res.status(404).json({ error: 'User not found' });
  if (user.coins < 1000) return res.status(400).json({ error: 'Not enough coins (need 1,000 🪙)' });
  await run('UPDATE users SET coins=coins-1000 WHERE id=?', [req.user.id]);
  const updated = await get('SELECT coins FROM users WHERE id=?', [req.user.id]);
  res.json({ coins: updated.coins });
});

app.post('/api/game/complete', auth, async (req, res) => {
  const { level, score, timeTaken, wordsFound } = req.body;
  const uid = req.user.id;
  const mults = getMultipliers();
  const xp = Math.floor((score * 0.3 + level * 2) * mults.xp);
  const coins = Math.floor((score * 0.08 + level * 1) * mults.coin);
  try {
    await run('INSERT INTO level_completions (user_id,level_number,score,time_taken,words_found) VALUES (?,?,?,?,?)', [uid, level, score, timeTaken, wordsFound]);
    await run('UPDATE users SET xp=xp+?,coins=coins+?,levels_completed=levels_completed+1 WHERE id=?', [xp, coins, uid]);
    const user = await get('SELECT id,username,xp,coins,levels_completed,rank_points,role,active_icon,active_title FROM users WHERE id=?', [uid]);
    if (!user) return res.status(404).json({ error: 'Account not found — please log out and register again' });
    const newIds = await awardTitles(uid).catch(() => []);
    const eventsActive = Object.values(activeEvents).map(({event,endsAt})=>({...event,endsAt}));
    res.json({ xpGain: xp, coinGain: coins, user, newTitles: newIds.map(id => TITLES[id]), eventsActive, mults });
  } catch { res.status(500).json({ error: 'Server error' }); }
});

app.get('/api/events', (req, res) => {
  res.json(Object.values(activeEvents).map(({event,endsAt})=>({...event,endsAt})));
});

app.get('/api/game/leaderboard/:type', async (req, res) => {
  const col = { xp: 'xp', coins: 'coins', levels: 'levels_completed', rank: 'rank_points' }[req.params.type] || 'xp';
  res.json(await all(`SELECT id,username,role,active_title,xp,coins,levels_completed,rank_points,wins,losses FROM users WHERE is_banned=0 ORDER BY ${col} DESC LIMIT 50`));
});

app.get('/api/game/me', auth, async (req, res) => {
  const user = await get('SELECT id,username,email,role,is_banned,xp,coins,levels_completed,rank_points,wins,losses,active_title,created_at FROM users WHERE id=?', [req.user.id]);
  if (user?.is_banned) return res.status(403).json({ error: 'Banned' });
  res.json(user);
});

app.get('/api/game/profile/:id', auth, async (req, res) => {
  const user = await get('SELECT id,username,role,xp,coins,levels_completed,rank_points,wins,losses,active_title,created_at FROM users WHERE id=?', [req.params.id]);
  if (!user) return res.status(404).json({ error: 'Not found' });
  const levels = await all('SELECT * FROM level_completions WHERE user_id=? ORDER BY completed_at DESC LIMIT 10', [req.params.id]);
  res.json({ user, recentLevels: levels });
});

app.get('/api/game/search', auth, async (req, res) => {
  const { q } = req.query;
  if (!q || q.length < 2) return res.json([]);
  res.json(await all('SELECT id,username,xp,rank_points FROM users WHERE username LIKE ? AND id!=? LIMIT 10', [`%${q}%`, req.user.id]));
});

// Friends
app.post('/api/friends/request/:fid', auth, async (req, res) => {
  const uid = req.user.id, fid = +req.params.fid;
  if (fid === uid) return res.status(400).json({ error: "Can't friend yourself" });
  if (await get('SELECT * FROM friendships WHERE (user_id=? AND friend_id=?) OR (user_id=? AND friend_id=?)', [uid, fid, fid, uid]))
    return res.status(409).json({ error: 'Already exists' });
  await run('INSERT INTO friendships (user_id,friend_id,status) VALUES (?,?,?)', [uid, fid, 'pending']);
  res.json({ success: true });
});

app.post('/api/friends/accept/:fid', auth, async (req, res) => {
  await run("UPDATE friendships SET status='accepted' WHERE user_id=? AND friend_id=? AND status='pending'", [+req.params.fid, req.user.id]);
  res.json({ success: true });
});

app.delete('/api/friends/:fid', auth, async (req, res) => {
  const uid = req.user.id, fid = +req.params.fid;
  await run('DELETE FROM friendships WHERE (user_id=? AND friend_id=?) OR (user_id=? AND friend_id=?)', [uid, fid, fid, uid]);
  res.json({ success: true });
});

app.get('/api/friends', auth, async (req, res) => {
  const uid = req.user.id;
  const rows = await all(`SELECT u.id,u.username,u.xp,u.rank_points,u.levels_completed,f.status,CASE WHEN f.user_id=? THEN 'sent' ELSE 'received' END as direction FROM friendships f JOIN users u ON (u.id=CASE WHEN f.user_id=? THEN f.friend_id ELSE f.user_id END) WHERE f.user_id=? OR f.friend_id=?`, [uid, uid, uid, uid]);
  res.json(rows.map(r => ({ ...r, is_online: onlineUsers.has(r.id) })));
});

app.get('/api/friends/messages/:fid', auth, async (req, res) => {
  const uid = req.user.id, fid = +req.params.fid;
  const msgs = await all(`SELECT m.*,u.username as sender_name FROM messages m JOIN users u ON u.id=m.sender_id WHERE (m.sender_id=? AND m.receiver_id=?) OR (m.sender_id=? AND m.receiver_id=?) ORDER BY m.created_at ASC LIMIT 100`, [uid, fid, fid, uid]);
  await run('UPDATE messages SET read=1 WHERE sender_id=? AND receiver_id=?', [fid, uid]);
  res.json(msgs);
});

// Titles
app.get('/api/titles/all', (req, res) => res.json(Object.values(TITLES)));

app.get('/api/titles/my', auth, async (req, res) => {
  const newIds = await awardTitles(req.user.id);
  const rows = await all('SELECT title_id,unlocked_at FROM user_titles WHERE user_id=? ORDER BY unlocked_at DESC', [req.user.id]);
  const user = await get('SELECT active_title FROM users WHERE id=?', [req.user.id]);
  res.json({ unlocked: rows.map(r => ({ ...TITLES[r.title_id], unlocked_at: r.unlocked_at })).filter(t => t.id), activeTitle: user?.active_title ?? null, newTitles: newIds.map(id => TITLES[id]) });
});

app.get('/api/titles/user/:id', async (req, res) => {
  const rows = await all('SELECT title_id,unlocked_at FROM user_titles WHERE user_id=? ORDER BY unlocked_at DESC', [req.params.id]);
  const user = await get('SELECT active_title FROM users WHERE id=?', [req.params.id]);
  res.json({ unlocked: rows.map(r => ({ ...TITLES[r.title_id], unlocked_at: r.unlocked_at })).filter(t => t.id), activeTitle: user?.active_title ?? null });
});

app.post('/api/titles/equip', auth, async (req, res) => {
  const { titleId } = req.body;
  if (titleId && !await get('SELECT id FROM user_titles WHERE user_id=? AND title_id=?', [req.user.id, titleId]))
    return res.status(403).json({ error: "Haven't earned that title" });
  await run('UPDATE users SET active_title=? WHERE id=?', [titleId || null, req.user.id]);
  res.json({ success: true });
});

// Admin
const adm = [auth, attachRole, requireRole('mod')];
app.get('/api/admin/users', ...adm, async (req, res) => {
  const users = await all('SELECT id,username,email,role,is_banned,xp,coins,levels_completed,rank_points,wins,losses,created_at,total_online_seconds,last_ip,last_fingerprint FROM users ORDER BY created_at DESC');
  res.json(users.map(u => ({ ...u, is_online: onlineUsers.has(u.id), session_secs: sessionStarts.has(u.id) ? Math.floor((Date.now()-sessionStarts.get(u.id))/1000) : 0 })));
});
app.get('/api/admin/stats', ...adm, async (req, res) => {
  const [u] = await all('SELECT COUNT(*) c FROM users');
  const [l] = await all('SELECT COUNT(*) c FROM level_completions');
  const [m] = await all("SELECT COUNT(*) c FROM ranked_matches WHERE status='completed'");
  const [msg] = await all('SELECT COUNT(*) c FROM messages');
  const recent = await all('SELECT id,username,role,created_at FROM users ORDER BY created_at DESC LIMIT 10');
  res.json({ totalUsers: u.c, totalLevels: l.c, totalMatches: m.c, totalMessages: msg.c, recentUsers: recent });
});
app.get('/api/admin/messages', ...adm, async (req, res) => {
  const convos = await all(`
    SELECT LEAST(m.sender_id,m.receiver_id) as u1, GREATEST(m.sender_id,m.receiver_id) as u2,
      MAX(m.created_at) as last_at, COUNT(*) as msg_count,
      ua.username as u1_name, ub.username as u2_name
    FROM messages m
    JOIN users us ON us.id=m.sender_id JOIN users ur ON ur.id=m.receiver_id
    JOIN users ua ON ua.id=LEAST(m.sender_id,m.receiver_id)
    JOIN users ub ON ub.id=GREATEST(m.sender_id,m.receiver_id)
    WHERE us.role!='owner' AND ur.role!='owner'
    GROUP BY LEAST(m.sender_id,m.receiver_id),GREATEST(m.sender_id,m.receiver_id),ua.username,ub.username
    ORDER BY last_at DESC LIMIT 50`);
  res.json(convos);
});
app.get('/api/admin/messages/:u1/:u2', ...adm, async (req, res) => {
  const { u1, u2 } = req.params;
  const [r1, r2] = await Promise.all([get('SELECT role FROM users WHERE id=?', [u1]), get('SELECT role FROM users WHERE id=?', [u2])]);
  if (!r1 || !r2 || r1.role==='owner' || r2.role==='owner') return res.status(403).json({ error: 'Cannot view this conversation' });
  const msgs = await all(`SELECT m.*,u.username as sender_name FROM messages m JOIN users u ON u.id=m.sender_id WHERE (m.sender_id=? AND m.receiver_id=?) OR (m.sender_id=? AND m.receiver_id=?) ORDER BY m.created_at ASC LIMIT 200`, [u1,u2,u2,u1]);
  res.json(msgs);
});
app.patch('/api/admin/users/:id/role', ...adm, requireRole('admin'), async (req, res) => {
  const { role } = req.body;
  const validRoles = ['user','mod','admin','staff-manager'];
  if (!validRoles.includes(role)) return res.status(400).json({ error: 'Invalid role' });
  if (role === 'staff-manager' && req.user.role !== 'owner') return res.status(403).json({ error: 'Only owner can assign Staff Manager' });
  const t = await get('SELECT * FROM users WHERE id=?', [req.params.id]);
  if (!t) return res.status(404).json({ error: 'Not found' });
  if (t.role === 'owner') return res.status(403).json({ error: 'Cannot modify owner' });
  if (ROLE_LEVELS[t.role] >= ROLE_LEVELS[req.user.role]) return res.status(403).json({ error: 'Cannot modify equal/higher role' });
  await run('UPDATE users SET role=? WHERE id=?', [role, req.params.id]);
  res.json({ success: true });
});
app.patch('/api/admin/users/:id/ban', ...adm, async (req, res) => {
  const t = await get('SELECT * FROM users WHERE id=?', [req.params.id]);
  if (!t || t.role==='owner') return res.status(403).json({ error: 'Cannot ban' });
  if (t.role==='staff-manager' && req.user.role!=='owner') return res.status(403).json({ error: 'Only owner can ban Staff Manager' });
  if (ROLE_LEVELS[t.role] >= ROLE_LEVELS[req.user.role]) return res.status(403).json({ error: 'Cannot ban equal/higher role' });
  const banning = req.body.banned ? 1 : 0;
  await run('UPDATE users SET is_banned=? WHERE id=?', [banning, req.params.id]);
  if (banning) {
    io.to(`u:${t.id}`).emit('user:banned');
  }
  io.emit('admin:user_updated', { id: t.id, is_banned: banning });
  res.json({ success: true });
});
app.patch('/api/admin/users/:id/coins', ...adm, requireRole('admin'), async (req, res) => {
  if (typeof req.body.amount !== 'number') return res.status(400).json({ error: 'Invalid' });
  await run('UPDATE users SET coins=MAX(0,coins+?) WHERE id=?', [req.body.amount, req.params.id]);
  res.json({ success: true });
});
app.patch('/api/admin/users/:id/xp', ...adm, requireRole('admin'), async (req, res) => {
  if (typeof req.body.amount !== 'number') return res.status(400).json({ error: 'Invalid' });
  await run('UPDATE users SET xp=MAX(0,xp+?) WHERE id=?', [req.body.amount, req.params.id]);
  res.json({ success: true });
});
app.delete('/api/admin/users/:id', ...adm, requireRole('owner'), async (req, res) => {
  try {
    const t = await get('SELECT * FROM users WHERE id=?', [req.params.id]);
    if (!t || t.role==='owner') return res.status(403).json({ error: 'Cannot delete owner' });
    const id = t.id;
    // Clear FK references before deleting
    await run('DELETE FROM level_completions WHERE user_id=?', [id]);
    await run('DELETE FROM friendships WHERE user_id=? OR friend_id=?', [id, id]);
    await run('DELETE FROM messages WHERE sender_id=? OR receiver_id=?', [id, id]);
    await run('DELETE FROM warnings WHERE user_id=?', [id]);
    await run('UPDATE warnings SET issued_by=NULL WHERE issued_by=?', [id]);
    await run('DELETE FROM reports WHERE reporter_id=? OR reported_id=?', [id, id]);
    await run('UPDATE reports SET handled_by=NULL WHERE handled_by=?', [id]);
    await run('DELETE FROM ranked_matches WHERE player1_id=? OR player2_id=?', [id, id]);
    await run('DELETE FROM user_titles WHERE user_id=?', [id]);
    await run('DELETE FROM staff_action_requests WHERE requester_id=? OR target_id=?', [id, id]);
    await run('UPDATE staff_action_requests SET resolved_by=NULL WHERE resolved_by=?', [id]);
    await run('UPDATE forum_posts SET author_id=NULL WHERE author_id=?', [id]);
    await run('UPDATE forum_replies SET author_id=NULL WHERE author_id=?', [id]);
    await run('UPDATE staff_messages SET author_id=NULL WHERE author_id=?', [id]);
    await run('UPDATE banned_ips SET banned_by=NULL WHERE banned_by=?', [id]);
    await run('UPDATE banned_devices SET banned_by=NULL WHERE banned_by=?', [id]);
    await run('DELETE FROM staff_messages WHERE author_id IS NULL AND author_id=?', [id]);
    await run('DELETE FROM users WHERE id=?', [id]);
    io.to(`u:${id}`).emit('user:banned'); // kick if online
    res.json({ success: true });
  } catch(e) { console.error('delete user:', e); res.status(500).json({ error: e.message }); }
});
app.post('/api/admin/announce', auth, attachRole, requireRole('owner'), (req, res) => {
  const { message } = req.body;
  if (!message?.trim()) return res.status(400).json({ error: 'Message required' });
  io.emit('global:announcement', { message: message.trim(), from: req.user.username });
  res.json({ success: true });
});
app.post('/api/admin/event', auth, attachRole, requireRole('owner'), (req, res) => {
  const { eventId, duration } = req.body;
  if (!EVENTS[eventId]) return res.status(400).json({ error: 'Unknown event' });
  const e = startEvent(eventId, req.user.username, duration ? +duration : null);
  res.json({ success: true, event: e });
});
app.get('/api/admin/events_list', auth, attachRole, requireRole('staff-manager'), (req, res) => {
  res.json(Object.values(EVENTS));
});

// Staff Manager request system
app.post('/api/admin/request', auth, attachRole, requireRole('staff-manager'), (req, res) => {
  if (req.user.role === 'owner') return res.status(400).json({ error: 'Owner can act directly' });
  const { type, data } = req.body;
  if (!['announce','event'].includes(type)) return res.status(400).json({ error: 'Invalid type' });
  if (type === 'announce' && !data?.message?.trim()) return res.status(400).json({ error: 'Message required' });
  if (type === 'event' && !EVENTS[data?.eventId]) return res.status(400).json({ error: 'Unknown event' });
  if (type === 'event' && data?.duration) data.duration = Math.min(120, Math.max(1, +data.duration || 0));
  const id = nextReqId++;
  const req_obj = { id, type, data, from: req.user.username, fromId: req.user.id, createdAt: Date.now() };
  pendingRequests.set(id, req_obj);
  io.to('owners').emit('admin:new_request', req_obj);
  res.json({ success: true, id });
});
app.get('/api/admin/requests', auth, attachRole, requireRole('owner'), (req, res) => {
  res.json([...pendingRequests.values()].sort((a,b) => a.createdAt - b.createdAt));
});
app.post('/api/admin/request/:id/approve', auth, attachRole, requireRole('owner'), (req, res) => {
  const id = +req.params.id;
  const r = pendingRequests.get(id);
  if (!r) return res.status(404).json({ error: 'Request not found' });
  pendingRequests.delete(id);
  if (r.type === 'announce') {
    io.emit('global:announcement', { message: r.data.message.trim(), from: r.from });
  } else if (r.type === 'event') {
    startEvent(r.data.eventId, r.from, r.data.duration || null);
  }
  io.to(`u:${r.fromId}`).emit('admin:request_resolved', { id, approved: true, type: r.type });
  res.json({ success: true });
});
app.post('/api/admin/request/:id/deny', auth, attachRole, requireRole('owner'), (req, res) => {
  const id = +req.params.id;
  const r = pendingRequests.get(id);
  if (!r) return res.status(404).json({ error: 'Request not found' });
  pendingRequests.delete(id);
  io.to(`u:${r.fromId}`).emit('admin:request_resolved', { id, approved: false, type: r.type });
  res.json({ success: true });
});

// ── STAFF ACTION REQUESTS ────────────────────────────────────────────────────
app.post('/api/staff/action-request', auth, attachRole, requireRole('staff-manager'), async (req, res) => {
  try {
    if (req.user.role === 'owner') return res.status(400).json({ error: 'Owner can act directly' });
    const { targetId, action, reason, timeoutHours } = req.body;
    if (!['warn','fire','ban','timeout'].includes(action)) return res.status(400).json({ error: 'Invalid action' });
    if (!reason?.trim()) return res.status(400).json({ error: 'Reason required' });
    const target = await get('SELECT id,username,role FROM users WHERE id=?', [+targetId]);
    if (!target) return res.status(404).json({ error: 'User not found' });
    if (target.role === 'owner') return res.status(403).json({ error: 'Cannot act on owner' });
    if (target.role === 'staff-manager') return res.status(403).json({ error: 'Cannot act on Staff Manager' });
    const r = await run(
      'INSERT INTO staff_action_requests (requester_id,target_id,action,reason,timeout_hours) VALUES (?,?,?,?,?)',
      [req.user.id, +targetId, action, reason.trim(), action==='timeout'?(+timeoutHours||24):null]
    );
    const row = await get('SELECT sar.*,u1.username as requester_name,u2.username as target_name FROM staff_action_requests sar JOIN users u1 ON u1.id=sar.requester_id JOIN users u2 ON u2.id=sar.target_id WHERE sar.id=?', [r.lastInsertRowid]);
    io.to('owners').emit('staff:new_action_request', row);
    res.json({ success: true });
  } catch(e) { console.error('staff action-request:', e); res.status(500).json({ error: e.message }); }
});

app.get('/api/staff/action-requests', auth, attachRole, requireRole('owner'), async (req, res) => {
  const rows = await all(`SELECT sar.*,u1.username as requester_name,u2.username as target_name
    FROM staff_action_requests sar
    JOIN users u1 ON u1.id=sar.requester_id
    JOIN users u2 ON u2.id=sar.target_id
    WHERE sar.status='pending' ORDER BY sar.created_at ASC`);
  res.json(rows);
});

app.post('/api/staff/action-requests/:id/approve', auth, attachRole, requireRole('owner'), async (req, res) => {
  try {
    const r = await get('SELECT * FROM staff_action_requests WHERE id=? AND status=?', [+req.params.id, 'pending']);
    if (!r) return res.status(404).json({ error: 'Request not found' });
    await run('UPDATE staff_action_requests SET status=?,resolved_at=NOW(),resolved_by=? WHERE id=?', ['approved', req.user.id, r.id]);
    if (r.action === 'warn') {
      await run('INSERT INTO warnings (user_id,issued_by,reason) VALUES (?,?,?)', [r.target_id, req.user.id, r.reason]);
    } else if (r.action === 'fire') {
      await run("UPDATE users SET role='user' WHERE id=?", [r.target_id]);
    } else if (r.action === 'ban') {
      await run('UPDATE users SET is_banned=1 WHERE id=?', [r.target_id]);
      io.to(`u:${r.target_id}`).emit('user:banned');
      io.emit('admin:user_updated', { id: r.target_id, is_banned: 1 });
    } else if (r.action === 'timeout') {
      const hrs = r.timeout_hours || 24;
      await run("UPDATE users SET timeout_until = NOW() + ($1 * INTERVAL '1 hour') WHERE id=$2", [hrs, r.target_id]);
    }
    const target = await get('SELECT username FROM users WHERE id=?', [r.target_id]);
    io.to(`u:${r.requester_id}`).emit('staff:action_resolved', { id: r.id, approved: true, action: r.action, targetName: target?.username });
    if (r.action !== 'ban') {
      io.to(`u:${r.target_id}`).emit('staff:action_taken', { action: r.action, reason: r.reason, timeoutHours: r.timeout_hours });
    }
    res.json({ success: true });
  } catch(e) { console.error('staff approve:', e); res.status(500).json({ error: e.message }); }
});

app.post('/api/staff/action-requests/:id/deny', auth, attachRole, requireRole('owner'), async (req, res) => {
  try {
    const r = await get('SELECT * FROM staff_action_requests WHERE id=? AND status=?', [+req.params.id, 'pending']);
    if (!r) return res.status(404).json({ error: 'Request not found' });
    await run('UPDATE staff_action_requests SET status=?,resolved_at=NOW(),resolved_by=? WHERE id=?', ['denied', req.user.id, r.id]);
    io.to(`u:${r.requester_id}`).emit('staff:action_resolved', { id: r.id, approved: false, action: r.action });
    res.json({ success: true });
  } catch(e) { console.error('staff deny:', e); res.status(500).json({ error: e.message }); }
});

// ── REPORTS ───────────────────────────────────────────────────────────────────
app.post('/api/report/:id', auth, async (req, res) => {
  const reportedId = +req.params.id;
  const { reason } = req.body;
  if (!reason?.trim()) return res.status(400).json({ error: 'Reason required' });
  if (reportedId === req.user.id) return res.status(400).json({ error: 'Cannot report yourself' });
  const target = await get('SELECT id,role FROM users WHERE id=?', [reportedId]);
  if (!target) return res.status(404).json({ error: 'User not found' });
  if (target.role === 'owner') return res.status(403).json({ error: 'Cannot report owner' });
  const existing = await get('SELECT id FROM reports WHERE reporter_id=? AND reported_id=? AND status=?', [req.user.id, reportedId, 'open']);
  if (existing) return res.status(409).json({ error: 'You already have an open report for this user' });
  await run('INSERT INTO reports (reporter_id,reported_id,reason) VALUES (?,?,?)', [req.user.id, reportedId, reason.trim()]);
  res.json({ success: true });
});
app.get('/api/admin/reports', auth, attachRole, requireRole('mod'), async (req, res) => {
  const rows = await all(`SELECT r.*,u1.username as reporter,u2.username as reported,u3.username as handler
    FROM reports r JOIN users u1 ON u1.id=r.reporter_id JOIN users u2 ON u2.id=r.reported_id
    LEFT JOIN users u3 ON u3.id=r.handled_by ORDER BY r.created_at DESC LIMIT 100`);
  res.json(rows);
});
app.patch('/api/admin/reports/:id', auth, attachRole, requireRole('mod'), async (req, res) => {
  const { status } = req.body;
  if (!['resolved','dismissed'].includes(status)) return res.status(400).json({ error: 'Invalid status' });
  await run('UPDATE reports SET status=?,handled_by=? WHERE id=?', [status, req.user.id, req.params.id]);
  res.json({ success: true });
});

// ── WARNINGS ──────────────────────────────────────────────────────────────────
app.post('/api/admin/users/:id/warn', auth, attachRole, requireRole('mod'), async (req, res) => {
  const targetId = +req.params.id;
  const { reason } = req.body;
  if (!reason?.trim()) return res.status(400).json({ error: 'Reason required' });
  const target = await get('SELECT role FROM users WHERE id=?', [targetId]);
  if (!target) return res.status(404).json({ error: 'User not found' });
  if (target.role === 'owner') return res.status(403).json({ error: 'Cannot warn owner' });
  if (ROLE_LEVELS[target.role] >= ROLE_LEVELS[req.user.role]) return res.status(403).json({ error: 'Cannot warn equal/higher role' });
  await run('INSERT INTO warnings (user_id,issued_by,reason) VALUES (?,?,?)', [targetId, req.user.id, reason.trim()]);
  io.to(`u:${targetId}`).emit('warning:received', { reason: reason.trim(), from: req.user.username });
  res.json({ success: true });
});
app.get('/api/admin/users/:id/warnings', auth, attachRole, requireRole('mod'), async (req, res) => {
  const rows = await all('SELECT w.*,u.username as issuer FROM warnings w JOIN users u ON u.id=w.issued_by WHERE w.user_id=? ORDER BY w.created_at DESC', [req.params.id]);
  res.json(rows);
});
app.delete('/api/admin/warnings/:id', auth, attachRole, requireRole('admin'), async (req, res) => {
  await run('DELETE FROM warnings WHERE id=?', [req.params.id]);
  res.json({ success: true });
});
app.get('/api/game/my-warnings', auth, async (req, res) => {
  const rows = await all('SELECT w.id,w.reason,w.created_at,u.username as issuer FROM warnings w JOIN users u ON u.id=w.issued_by WHERE w.user_id=? ORDER BY w.created_at DESC', [req.user.id]);
  res.json(rows);
});

// ── IP / DEVICE BANS ──────────────────────────────────────────────────────────
app.get('/api/admin/bans/ip', ...adm, async (req, res) => {
  const bans = await all('SELECT bi.*,u.username as banned_by_name FROM banned_ips bi LEFT JOIN users u ON u.id=bi.banned_by ORDER BY bi.created_at DESC');
  res.json(bans);
});
app.post('/api/admin/bans/ip', ...adm, async (req, res) => {
  const { ip, reason } = req.body;
  if (!ip?.trim()) return res.status(400).json({ error: 'IP required' });
  await run('INSERT INTO banned_ips (ip,reason,banned_by) VALUES (?,?,?) ON CONFLICT(ip) DO NOTHING', [ip.trim(), reason||'', req.user.id]);
  bannedIpSet.add(ip.trim());
  res.json({ success: true });
});
app.delete('/api/admin/bans/ip/:ip', ...adm, async (req, res) => {
  const ip = decodeURIComponent(req.params.ip);
  await run('DELETE FROM banned_ips WHERE ip=?', [ip]);
  bannedIpSet.delete(ip);
  res.json({ success: true });
});
app.get('/api/admin/bans/device', ...adm, async (req, res) => {
  const bans = await all('SELECT bd.*,u.username as banned_by_name FROM banned_devices bd LEFT JOIN users u ON u.id=bd.banned_by ORDER BY bd.created_at DESC');
  res.json(bans);
});
app.post('/api/admin/bans/device', ...adm, async (req, res) => {
  const { fingerprint, reason } = req.body;
  if (!fingerprint?.trim()) return res.status(400).json({ error: 'Fingerprint required' });
  await run('INSERT INTO banned_devices (fingerprint,reason,banned_by) VALUES (?,?,?) ON CONFLICT(fingerprint) DO NOTHING', [fingerprint.trim(), reason||'', req.user.id]);
  bannedFpSet.add(fingerprint.trim());
  res.json({ success: true });
});
app.delete('/api/admin/bans/device/:id', ...adm, async (req, res) => {
  const ban = await get('SELECT fingerprint FROM banned_devices WHERE id=?', [req.params.id]);
  await run('DELETE FROM banned_devices WHERE id=?', [req.params.id]);
  if (ban) bannedFpSet.delete(ban.fingerprint);
  res.json({ success: true });
});
app.post('/api/admin/users/:id/ban-ip', ...adm, async (req, res) => {
  const target = await get('SELECT last_ip,role FROM users WHERE id=?', [req.params.id]);
  if (!target?.last_ip) return res.status(400).json({ error: 'No IP on record for this user' });
  if (target.role === 'owner') return res.status(403).json({ error: 'Cannot ban owner IP' });
  await run('INSERT INTO banned_ips (ip,reason,banned_by) VALUES (?,?,?) ON CONFLICT(ip) DO NOTHING', [target.last_ip, req.body.reason||'', req.user.id]);
  bannedIpSet.add(target.last_ip);
  res.json({ success: true, ip: target.last_ip });
});
app.post('/api/admin/users/:id/ban-device', ...adm, async (req, res) => {
  const target = await get('SELECT last_fingerprint,role FROM users WHERE id=?', [req.params.id]);
  if (!target?.last_fingerprint) return res.status(400).json({ error: 'No device fingerprint on record' });
  if (target.role === 'owner') return res.status(403).json({ error: 'Cannot ban owner device' });
  await run('INSERT INTO banned_devices (fingerprint,reason,banned_by) VALUES (?,?,?) ON CONFLICT(fingerprint) DO NOTHING', [target.last_fingerprint, req.body.reason||'', req.user.id]);
  bannedFpSet.add(target.last_fingerprint);
  res.json({ success: true });
});

// ── SOCKET ────────────────────────────────────────────────────────────────────
const waiting = new Map(), matches = new Map();
const casualWaiting = new Map();
const customLobbies = new Map(); // code -> { hostId }
const onlineUsers = new Set();
const sessionStarts = new Map();
io.use(async (socket, next) => {
  try {
    socket.user = jwt.verify(socket.handshake.auth?.token, JWT_SECRET);
    const fp = socket.handshake.auth?.fp;
    const ip = (socket.handshake.headers['x-forwarded-for']?.split(',')[0]?.trim() || socket.handshake.address || '').replace('::ffff:', '');
    if (bannedIpSet.has(ip)) return next(new Error('IP_BANNED'));
    if (fp && bannedFpSet.has(fp)) return next(new Error('DEVICE_BANNED'));
    socket.clientIp = ip;
    socket.fp = fp || null;
    next();
  } catch { next(new Error('Invalid token')); }
});
io.on('connection', async socket => {
  const uid = socket.user.id;
  socket.join(`u:${uid}`);
  onlineUsers.add(uid);
  sessionStarts.set(uid, Date.now());
  if (socket.clientIp || socket.fp) {
    await run('UPDATE users SET last_ip=COALESCE($1,last_ip), last_fingerprint=COALESCE($2,last_fingerprint) WHERE id=$3',
      [socket.clientIp || null, socket.fp || null, uid]).catch(() => {});
  }
  const u = await get('SELECT role FROM users WHERE id=?', [uid]);
  if (u?.role === 'owner') socket.join('owners');
  if (u && ROLE_LEVELS[u.role] >= 1) socket.join('staff');

  socket.on('ranked:join_queue', async () => {
    const opponentEntry = [...waiting.entries()].find(([wid]) => wid !== uid);
    if (opponentEntry) {
      const [wid] = opponentEntry;
      waiting.delete(wid);
      const level = Math.floor(Math.random() * 5) + 1;
      const { lastInsertRowid: mid } = await run('INSERT INTO ranked_matches (player1_id,player2_id,level_number,status) VALUES (?,?,?,?)', [wid, uid, level, 'in_progress']);
      const room = `m:${mid}`;
      socket.join(room); io.to(`u:${wid}`).socketsJoin(room);
      matches.set(mid, { id: mid, p1: wid, p2: uid, s1: 0, s2: 0, w1: [], w2: [], level, status: 'in_progress', type: 'ranked' });
      const [p1, p2] = await Promise.all([get('SELECT id,username,rank_points FROM users WHERE id=?', [wid]), get('SELECT id,username,rank_points FROM users WHERE id=?', [uid])]);
      io.to(room).emit('ranked:match_found', { matchId: mid, level, player1: p1, player2: p2, duration: 120 });
      setTimeout(() => endMatch(mid), 120000);
    } else { waiting.set(uid, socket.id); socket.emit('ranked:waiting'); }
  });

  socket.on('ranked:leave_queue', () => waiting.delete(uid));

  // ── Casual 1v1 (no RP) ────────────────────────────────────────────────────
  socket.on('casual:join_queue', async () => {
    const opponentEntry = [...casualWaiting.entries()].find(([wid]) => wid !== uid);
    if (opponentEntry) {
      const [wid] = opponentEntry;
      casualWaiting.delete(wid);
      const level = Math.floor(Math.random() * 5) + 1;
      const mid = `c${Date.now()}`;
      const room = `m:${mid}`;
      socket.join(room); io.to(`u:${wid}`).socketsJoin(room);
      matches.set(mid, { id: mid, p1: wid, p2: uid, s1: 0, s2: 0, w1: [], w2: [], level, status: 'in_progress', type: 'casual' });
      const [p1, p2] = await Promise.all([
        get('SELECT id,username,rank_points FROM users WHERE id=?', [wid]),
        get('SELECT id,username,rank_points FROM users WHERE id=?', [uid]),
      ]);
      io.to(room).emit('ranked:match_found', { matchId: mid, level, player1: p1, player2: p2, duration: 120 });
      setTimeout(() => endMatch(mid), 120000);
    } else { casualWaiting.set(uid, socket.id); socket.emit('ranked:waiting'); }
  });
  socket.on('casual:leave_queue', () => casualWaiting.delete(uid));

  // ── Custom match (friend invite) ──────────────────────────────────────────
  socket.on('custom:invite', async ({ friendId }) => {
    const host = await get('SELECT id,username FROM users WHERE id=?', [uid]);
    const code = Math.random().toString(36).slice(2, 8).toUpperCase();
    customLobbies.set(code, { hostId: uid });
    io.to(`u:${friendId}`).emit('custom:invite_received', { code, hostUsername: host.username });
    socket.emit('custom:lobby_created', { code });
  });
  socket.on('custom:accept', async ({ code }) => {
    const lobby = customLobbies.get(code);
    if (!lobby || lobby.hostId === uid) return;
    customLobbies.delete(code);
    const level = Math.floor(Math.random() * 5) + 1;
    const mid = `x${Date.now()}`;
    const room = `m:${mid}`;
    socket.join(room); io.to(`u:${lobby.hostId}`).socketsJoin(room);
    matches.set(mid, { id: mid, p1: lobby.hostId, p2: uid, s1: 0, s2: 0, w1: [], w2: [], level, status: 'in_progress', type: 'custom' });
    const [p1, p2] = await Promise.all([
      get('SELECT id,username,rank_points FROM users WHERE id=?', [lobby.hostId]),
      get('SELECT id,username,rank_points FROM users WHERE id=?', [uid]),
    ]);
    io.to(room).emit('ranked:match_found', { matchId: mid, level, player1: p1, player2: p2, duration: 120 });
    setTimeout(() => endMatch(mid), 120000);
  });
  socket.on('custom:decline', ({ code }) => {
    const lobby = customLobbies.get(code);
    if (lobby) { customLobbies.delete(code); io.to(`u:${lobby.hostId}`).emit('custom:declined'); }
  });

  socket.on('ranked:word_found', ({ matchId, word, score }) => {
    const m = matches.get(matchId);
    if (!m || m.status !== 'in_progress') return;
    if (m.p1 === uid) { m.s1 += score; m.w1.push(word); }
    else if (m.p2 === uid) { m.s2 += score; m.w2.push(word); }
    io.to(`m:${matchId}`).emit('ranked:score_update', { p1Score: m.s1, p2Score: m.s2, p1Words: m.w1.length, p2Words: m.w2.length });
  });

  socket.on('match:chat', ({ matchId, message }) => {
    const m = matches.get(matchId);
    if (!m || m.status !== 'in_progress') return;
    if (m.p1 !== uid && m.p2 !== uid) return;
    const clean = String(message || '').slice(0, 100).trim();
    if (!clean) return;
    io.to(`m:${matchId}`).emit('match:chat', { uid, username: socket.user.username, message: clean });
  });

  socket.on('chat:message', async ({ friendId, content }) => {
    if (!content?.trim()) return;
    const { lastInsertRowid: id } = await run('INSERT INTO messages (sender_id,receiver_id,content) VALUES (?,?,?)', [uid, friendId, content.trim()]);
    const msg = await get('SELECT m.*,u.username as sender_name FROM messages m JOIN users u ON u.id=m.sender_id WHERE m.id=?', [id]);
    io.to(`u:${friendId}`).emit('chat:new_message', msg);
    socket.emit('chat:new_message', msg);
  });

  socket.on('disconnect', async () => {
    waiting.delete(uid); casualWaiting.delete(uid);
    for (const [code, lobby] of customLobbies.entries()) {
      if (lobby.hostId === uid) customLobbies.delete(code);
    }
    onlineUsers.delete(uid);
    const secs = Math.floor((Date.now() - (sessionStarts.get(uid) || Date.now())) / 1000);
    sessionStarts.delete(uid);
    if (secs > 0) await run('UPDATE users SET total_online_seconds=total_online_seconds+? WHERE id=?', [secs, uid]).catch(() => {});
  });
});

async function endMatch(mid) {
  const m = matches.get(mid);
  if (!m || m.status !== 'in_progress') return;
  m.status = 'ended';
  matches.delete(mid);
  const win = m.s1 >= m.s2 ? m.p1 : m.p2, lose = win === m.p1 ? m.p2 : m.p1;
  const isRanked = m.type === 'ranked';
  io.to(`m:${mid}`).emit('ranked:match_ended', { winnerId: win, p1Score: m.s1, p2Score: m.s2, rpGain: isRanked ? 25 : 0, rpLoss: isRanked ? 15 : 0 });
  if (!isRanked) return;
  try {
    await run("UPDATE ranked_matches SET status='completed',winner_id=?,player1_score=?,player2_score=?,ended_at=CURRENT_TIMESTAMP WHERE id=?", [win, m.s1, m.s2, mid]);
    await run('UPDATE users SET rank_points=rank_points+25,wins=wins+1 WHERE id=?', [win]);
    await run('UPDATE users SET rank_points=MAX(0,rank_points-15),losses=losses+1 WHERE id=?', [lose]);
    const [wt, lt] = await Promise.all([awardTitles(win).catch(()=>[]), awardTitles(lose).catch(()=>[])]);
    if (wt.length) io.to(`u:${win}`).emit('titles:new', wt.map(id => TITLES[id]));
    if (lt.length) io.to(`u:${lose}`).emit('titles:new', lt.map(id => TITLES[id]));
  } catch(e) { console.error('endMatch DB error:', e.message); }
}

// ── SHOP ──────────────────────────────────────────────────────────────────────
const SHOP_ICONS = [
  {id:'star',emoji:'⭐',name:'Star',price:1500,rarity:'common'},
  {id:'fire',emoji:'🔥',name:'Flame',price:2000,rarity:'common'},
  {id:'lightning',emoji:'⚡',name:'Lightning',price:2000,rarity:'common'},
  {id:'gem',emoji:'💎',name:'Gem',price:3000,rarity:'common'},
  {id:'target',emoji:'🎯',name:'Bullseye',price:2500,rarity:'common'},
  {id:'music',emoji:'🎵',name:'Music Note',price:1800,rarity:'common'},
  {id:'sword',emoji:'⚔️',name:'Crossed Swords',price:6000,rarity:'rare'},
  {id:'shield',emoji:'🛡️',name:'Shield',price:6000,rarity:'rare'},
  {id:'crown',emoji:'👑',name:'Crown',price:12000,rarity:'rare'},
  {id:'trophy',emoji:'🏆',name:'Trophy',price:10000,rarity:'rare'},
  {id:'dragon',emoji:'🐉',name:'Dragon',price:15000,rarity:'rare'},
  {id:'wolf',emoji:'🐺',name:'Wolf',price:8000,rarity:'rare'},
  {id:'skull',emoji:'💀',name:'Skull',price:14000,rarity:'rare'},
  {id:'moon',emoji:'🌙',name:'Moon',price:25000,rarity:'epic'},
  {id:'crystal',emoji:'🔮',name:'Crystal Ball',price:30000,rarity:'epic'},
  {id:'comet',emoji:'☄️',name:'Comet',price:35000,rarity:'epic'},
  {id:'galaxy',emoji:'🌌',name:'Galaxy',price:50000,rarity:'epic'},
  {id:'phoenix',emoji:'🦅',name:'Phoenix',price:40000,rarity:'epic'},
];

const STAFF_ICONS = [
  {id:'staff_mod',    emoji:'🔨', name:'Moderator',     rarity:'staff', requiredRole:'mod'},
  {id:'staff_admin',  emoji:'⚜️', name:'Administrator', rarity:'staff', requiredRole:'admin'},
  {id:'staff_sm',     emoji:'🎖️', name:'Staff Manager', rarity:'staff', requiredRole:'staff-manager'},
  {id:'staff_owner',  emoji:'🔱', name:'Owner',         rarity:'staff', requiredRole:'owner'},
];

app.get('/api/shop/icons', auth, attachRole, async (req, res) => {
  try {
    const owned = await all('SELECT icon_id FROM user_icons WHERE user_id=?', [req.user.id]);
    const ownedSet = new Set(owned.map(r => r.icon_id));
    const user = await get('SELECT coins, active_icon FROM users WHERE id=?', [req.user.id]);
    const userLevel = ROLE_LEVELS[req.user.role] ?? 0;
    const staffIcons = STAFF_ICONS
      .filter(i => userLevel >= (ROLE_LEVELS[i.requiredRole] ?? 99))
      .map(i => ({ ...i, owned: true, staff: true, price: 0 }));
    res.json({ icons: SHOP_ICONS.map(i => ({ ...i, owned: ownedSet.has(i.id) })), staffIcons, coins: user.coins, activeIcon: user.active_icon });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/shop/icons/:id/buy', auth, async (req, res) => {
  try {
    const icon = SHOP_ICONS.find(i => i.id === req.params.id);
    if (!icon) return res.status(404).json({ error: 'Icon not found' });
    const existing = await get('SELECT id FROM user_icons WHERE user_id=? AND icon_id=?', [req.user.id, icon.id]);
    if (existing) return res.status(400).json({ error: 'Already owned' });
    const user = await get('SELECT coins FROM users WHERE id=?', [req.user.id]);
    if (user.coins < icon.price) return res.status(400).json({ error: 'Not enough coins' });
    await run('UPDATE users SET coins=coins-? WHERE id=?', [icon.price, req.user.id]);
    await run('INSERT INTO user_icons (user_id,icon_id) VALUES (?,?)', [req.user.id, icon.id]);
    const updated = await get('SELECT coins FROM users WHERE id=?', [req.user.id]);
    res.json({ success: true, coins: updated.coins });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/shop/icons/:id/equip', auth, attachRole, async (req, res) => {
  try {
    const iconId = req.params.id === 'none' ? null : req.params.id;
    if (iconId) {
      const staffIcon = STAFF_ICONS.find(i => i.id === iconId);
      if (staffIcon) {
        if ((ROLE_LEVELS[req.user.role] ?? 0) < (ROLE_LEVELS[staffIcon.requiredRole] ?? 99))
          return res.status(403).json({ error: 'No longer eligible for this icon' });
      } else {
        const owned = await get('SELECT id FROM user_icons WHERE user_id=? AND icon_id=?', [req.user.id, iconId]);
        if (!owned) return res.status(403).json({ error: 'Not owned' });
      }
    }
    await run('UPDATE users SET active_icon=? WHERE id=?', [iconId, req.user.id]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── STAFF CHAT ────────────────────────────────────────────────────────────────
app.get('/api/staff/messages', auth, attachRole, requireRole('mod'), async (req, res) => {
  try {
    const msgs = await all(`SELECT sm.*,u.username as author_name,u.role as author_role FROM staff_messages sm LEFT JOIN users u ON u.id=sm.author_id ORDER BY sm.created_at DESC LIMIT 100`);
    res.json(msgs.reverse());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/staff/messages', auth, attachRole, requireRole('mod'), async (req, res) => {
  try {
    const { content } = req.body;
    if (!content?.trim()) return res.status(400).json({ error: 'Empty message' });
    const { lastInsertRowid: id } = await run('INSERT INTO staff_messages (author_id,content) VALUES (?,?)', [req.user.id, content.trim()]);
    const msg = await get(`SELECT sm.*,u.username as author_name,u.role as author_role FROM staff_messages sm LEFT JOIN users u ON u.id=sm.author_id WHERE sm.id=?`, [id]);
    io.to('staff').emit('staff:chat:message', msg);
    res.json(msg);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/staff/messages/:id', auth, attachRole, requireRole('admin'), async (req, res) => {
  try {
    await run('DELETE FROM staff_messages WHERE id=?', [+req.params.id]);
    io.to('staff').emit('staff:chat:deleted', { id: +req.params.id });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── FORUM ─────────────────────────────────────────────────────────────────────
app.get('/api/forum/posts', auth, async (req, res) => {
  try {
    const section = req.query.section === 'updates' ? 'updates' : 'general';
    const rows = await all(`
      SELECT fp.id, fp.title, fp.content, fp.section, fp.pinned, fp.created_at,
        u.username as author_name, u.role as author_role,
        (SELECT COUNT(*) FROM forum_replies fr WHERE fr.post_id=fp.id) as reply_count
      FROM forum_posts fp LEFT JOIN users u ON u.id=fp.author_id
      WHERE fp.section=? ORDER BY fp.pinned DESC, fp.created_at DESC`, [section]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/forum/posts', auth, attachRole, async (req, res) => {
  try {
    const { title, content, section, media } = req.body;
    if (!title?.trim() || !content?.trim()) return res.status(400).json({ error: 'Title and content required' });
    const sec = section === 'updates' ? 'updates' : 'general';
    if (sec === 'updates' && req.user.role !== 'owner') return res.status(403).json({ error: 'Only owner can post in Updates' });
    const { lastInsertRowid: id } = await run(
      'INSERT INTO forum_posts (author_id,section,title,content,media) VALUES (?,?,?,?,?)',
      [req.user.id, sec, title.trim(), content.trim(), media || null]
    );
    const post = await get(`SELECT fp.*,u.username as author_name,u.role as author_role FROM forum_posts fp LEFT JOIN users u ON u.id=fp.author_id WHERE fp.id=?`, [id]);
    io.emit('forum:new_post', { section: sec, post });
    res.json(post);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/forum/posts/:id', auth, async (req, res) => {
  try {
    const post = await get(`SELECT fp.*,u.username as author_name,u.role as author_role FROM forum_posts fp LEFT JOIN users u ON u.id=fp.author_id WHERE fp.id=?`, [+req.params.id]);
    if (!post) return res.status(404).json({ error: 'Post not found' });
    const replies = await all(`SELECT fr.*,u.username as author_name,u.role as author_role FROM forum_replies fr LEFT JOIN users u ON u.id=fr.author_id WHERE fr.post_id=? ORDER BY fr.created_at ASC`, [post.id]);
    res.json({ post, replies });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/forum/posts/:id/reply', auth, attachRole, async (req, res) => {
  try {
    const { content, media } = req.body;
    if (!content?.trim()) return res.status(400).json({ error: 'Content required' });
    const post = await get('SELECT * FROM forum_posts WHERE id=?', [+req.params.id]);
    if (!post) return res.status(404).json({ error: 'Post not found' });
    if (post.section === 'updates') return res.status(403).json({ error: 'Cannot reply to announcements' });
    const { lastInsertRowid: id } = await run(
      'INSERT INTO forum_replies (post_id,author_id,content,media) VALUES (?,?,?,?)',
      [post.id, req.user.id, content.trim(), media || null]
    );
    const reply = await get(`SELECT fr.*,u.username as author_name,u.role as author_role FROM forum_replies fr LEFT JOIN users u ON u.id=fr.author_id WHERE fr.id=?`, [id]);
    io.emit(`forum:new_reply:${post.id}`, reply);
    res.json(reply);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/forum/posts/:id', auth, attachRole, async (req, res) => {
  try {
    const post = await get('SELECT * FROM forum_posts WHERE id=?', [+req.params.id]);
    if (!post) return res.status(404).json({ error: 'Not found' });
    if (post.author_id !== req.user.id && ROLE_LEVELS[req.user.role] < 2) return res.status(403).json({ error: 'No permission' });
    await run('DELETE FROM forum_posts WHERE id=?', [post.id]);
    io.emit('forum:post_deleted', { id: post.id, section: post.section });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/forum/replies/:id', auth, attachRole, async (req, res) => {
  try {
    const reply = await get('SELECT * FROM forum_replies WHERE id=?', [+req.params.id]);
    if (!reply) return res.status(404).json({ error: 'Not found' });
    if (reply.author_id !== req.user.id && ROLE_LEVELS[req.user.role] < 2) return res.status(403).json({ error: 'No permission' });
    await run('DELETE FROM forum_replies WHERE id=?', [reply.id]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/forum/posts/:id/pin', auth, attachRole, requireRole('admin'), async (req, res) => {
  try {
    const { pinned } = req.body;
    await run('UPDATE forum_posts SET pinned=? WHERE id=?', [pinned?1:0, +req.params.id]);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── SERVE FRONTEND ────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ── START ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
initDb().then(async () => { await loadBanCaches(); server.listen(PORT, () => console.log(`Running on port ${PORT}`)); }).catch(e => { console.error(e); process.exit(1); });
