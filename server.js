// server.js
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const BAN_FILE = path.join(__dirname, 'bans.json');
const DATA_DIR = path.join(__dirname, 'data');

// اطمینان از وجود پوشه
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ==================== State ====================
const clients = new Map();          // ws -> { id, name, job, token, isBanned, ... }
const players = new Map();          // id -> player data
const vehicles = new Map();         // vehicleId -> vehicle data
let bans = {};                      // id -> { expire, reason, permanent }

// ==================== Load / Save Bans ====================
function loadBans() {
  try {
    if (fs.existsSync(BAN_FILE)) {
      bans = JSON.parse(fs.readFileSync(BAN_FILE, 'utf8'));
      console.log(`✅ Loaded ${Object.keys(bans).length} bans`);
    }
  } catch (e) {
    console.error('Error loading bans:', e.message);
    bans = {};
  }
}

function saveBans() {
  try {
    fs.writeFileSync(BAN_FILE, JSON.stringify(bans, null, 2));
  } catch (e) {
    console.error('Error saving bans:', e.message);
  }
}

function isBanned(id) {
  const ban = bans[id];
  if (!ban) return false;
  if (ban.permanent || ban.expire === -1) return true;
  if (ban.expire && Date.now() / 1000 > ban.expire) {
    delete bans[id];
    saveBans();
    return false;
  }
  return true;
}

function getBanInfo(id) {
  const ban = bans[id];
  if (!ban) return null;
  return {
    expire: ban.permanent ? -1 : (ban.expire || -1),
    reason: ban.reason || 'بن شده توسط بازرسی',
    permanent: !!ban.permanent
  };
}

loadBans();

// ==================== Helpers ====================
function genId() {
  return crypto.randomBytes(8).toString('hex');
}

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, excludeId = null) {
  const msg = JSON.stringify(data);
  for (const [ws, client] of clients) {
    if (client.id === excludeId) continue;
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function broadcastToAll(data) {
  const msg = JSON.stringify(data);
  for (const [ws] of clients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function getFullState() {
  const playersArr = [];
  for (const [id, p] of players) {
    playersArr.push({
      user_id: id,
      id: id,
      name: p.name,
      job: p.job,
      position: p.position,
      rotation: p.rotation,
      anim: p.anim,
      speed: p.speed,
      is_grounded: p.is_grounded
    });
  }

  const vehiclesArr = [];
  for (const [vid, v] of vehicles) {
    vehiclesArr.push({
      id: vid,
      position: v.position,
      rotation: v.rotation,
      speed: v.speed,
      steering: v.steering,
      occupants: v.occupants,
      ownerId: v.ownerId,
      owner_id: v.ownerId,
      type: v.car_type,
      car_type: v.car_type,
      car_db_id: v.car_db_id,
      fuel: v.fuel
    });
  }

  return { type: 'full_state', players: playersArr, vehicles: vehiclesArr };
}

// ==================== Server ====================
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Poppo Multiplayer Server is running\n');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  console.log('🔌 New connection');

  const client = {
    id: null,
    name: 'Unknown',
    job: 'بیکار',
    token: '',
    isBanned: false,
    lastPing: Date.now()
  };
  clients.set(ws, client);

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    if (!data || !data.type) return;

    switch (data.type) {
      case 'set_id':
        handleSetId(ws, client, data);
        break;
      case 'update':
        handlePlayerUpdate(ws, client, data);
        break;
      case 'ping':
        client.lastPing = Date.now();
        send(ws, { type: 'pong' });
        break;
      case 'pong':
        client.lastPing = Date.now();
        break;
      case 'chat':
        handleChat(ws, client, data);
        break;
      case 'create_vehicle':
        handleCreateVehicle(ws, client, data);
        break;
      case 'update_vehicle':
        handleUpdateVehicle(ws, client, data);
        break;
      case 'join_vehicle':
        handleJoinVehicle(ws, client, data);
        break;
      case 'leave_vehicle':
        handleLeaveVehicle(ws, client, data);
        break;
      case 'seat_update':
        handleSeatUpdate(ws, client, data);
        break;
      case 'ban_player':
        handleBanPlayer(ws, client, data);
        break;
      case 'unban_player':
        handleUnbanPlayer(ws, client, data);
        break;
      default:
        break;
    }
  });

  ws.on('close', () => {
    handleDisconnect(ws, client);
  });

  ws.on('error', (err) => {
    console.error('WS error:', err.message);
  });
});

// ==================== Handlers ====================
function handleSetId(ws, client, data) {
  const id = String(data.id || '');
  if (!id) {
    send(ws, { type: 'error', message: 'Invalid id' });
    return;
  }

  // اگر قبلاً با این id وصل بوده، قبلی را پاک کن
  for (const [oldWs, oldClient] of clients) {
    if (oldClient.id === id && oldWs !== ws) {
      try { oldWs.close(); } catch (_) {}
      clients.delete(oldWs);
      players.delete(id);
    }
  }

  client.id = id;
  client.name = String(data.name || 'Player_' + id.substring(0, 6));
  client.job = String(data.job || 'بیکار');
  client.token = String(data.token || '');

  // چک بن
  if (isBanned(id)) {
    client.isBanned = true;
    const banInfo = getBanInfo(id);
    send(ws, {
      type: 'you_are_banned',
      message: banInfo.reason,
      reason: banInfo.reason,
      expire: banInfo.expire,
      duration: banInfo.expire === -1 ? -1 : undefined
    });
    console.log(`🚫 Banned player connected: ${client.name} (${id})`);
  } else {
    client.isBanned = false;
  }

  // ثبت در players
  players.set(id, {
    name: client.name,
    job: client.job,
    position: { x: 0, y: 0, z: 0 },
    rotation: 0,
    anim: 0,
    speed: 0,
    is_grounded: true
  });

  // پاسخ set_id
  send(ws, {
    type: 'set_id',
    id: id,
    data: { token: client.token }
  });

  // ارسال full_state به همه
  broadcastToAll(getFullState());

  console.log(`✅ Player joined: ${client.name} (${id}) | Banned: ${client.isBanned}`);
}

function handlePlayerUpdate(ws, client, data) {
  if (!client.id) return;

  const p = players.get(client.id);
  if (!p) return;

  if (data.position) {
    p.position = {
      x: Number(data.position.x) || 0,
      y: Number(data.position.y) || 0,
      z: Number(data.position.z) || 0
    };
  }
  if (data.rotation !== undefined) p.rotation = Number(data.rotation) || 0;
  if (data.name) {
    p.name = String(data.name);
    client.name = p.name;
  }
  if (data.job) {
    p.job = String(data.job);
    client.job = p.job;
  }
  if (data.anim !== undefined) p.anim = Number(data.anim) || 0;
  if (data.speed !== undefined) p.speed = Number(data.speed) || 0;
  if (data.is_grounded !== undefined) p.is_grounded = !!data.is_grounded;

  // فقط به بقیه بفرست
  broadcast({
    type: 'update',
    user_id: client.id,
    id: client.id,
    name: p.name,
    job: p.job,
    position: p.position,
    rotation: p.rotation,
    anim: p.anim,
    speed: p.speed,
    is_grounded: p.is_grounded
  }, client.id);
}

function handleChat(ws, client, data) {
  if (!client.id) return;
  if (client.isBanned) {
    send(ws, { type: 'error', message: 'شما بن شده‌اید و نمی‌توانید چت کنید' });
    return;
  }

  const message = String(data.message || '').trim();
  if (!message || message.length > 300) return;

  broadcastToAll({
    type: 'chat',
    sender_id: client.id,
    sender_name: client.name,
    message: message
  });
}

function handleCreateVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = genId();
  const vehicle = {
    id: vid,
    position: data.position || { x: 0, y: 0, z: 0 },
    rotation: Number(data.rotation) || 0,
    speed: 0,
    steering: Number(data.steering) || 0,
    occupants: [client.id],          // راننده در صندلی ۰
    ownerId: client.id,
    car_type: String(data.car_type || 'car'),
    car_db_id: String(data.car_db_id || ''),
    fuel: 100
  };

  vehicles.set(vid, vehicle);

  broadcastToAll({
    type: 'vehicle_update',
    vehicles: [{
      id: vid,
      position: vehicle.position,
      rotation: vehicle.rotation,
      speed: vehicle.speed,
      steering: vehicle.steering,
      occupants: vehicle.occupants,
      ownerId: vehicle.ownerId,
      owner_id: vehicle.ownerId,
      type: vehicle.car_type,
      car_type: vehicle.car_type,
      car_db_id: vehicle.car_db_id,
      fuel: vehicle.fuel
    }]
  });

  // به خود بازیکن هم id ماشین را بده (اختیاری)
  send(ws, {
    type: 'vehicle_update',
    vehicle: {
      id: vid,
      position: vehicle.position,
      rotation: vehicle.rotation,
      occupants: vehicle.occupants,
      ownerId: vehicle.ownerId,
      car_type: vehicle.car_type,
      car_db_id: vehicle.car_db_id
    }
  });

  console.log(`🚗 Vehicle created: ${vid} by ${client.name}`);
}

function handleUpdateVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = String(data.vehicleId || data.id || '');
  if (!vid || !vehicles.has(vid)) return;

  const v = vehicles.get(vid);

  // فقط صاحب یا کسی که داخل ماشین است می‌تواند آپدیت کند
  const isOccupant = Array.isArray(v.occupants) && v.occupants.includes(client.id);
  if (v.ownerId !== client.id && !isOccupant) return;

  if (data.position) {
    v.position = {
      x: Number(data.position.x) || 0,
      y: Number(data.position.y) || 0,
      z: Number(data.position.z) || 0
    };
  }
  if (data.rotation !== undefined) v.rotation = Number(data.rotation) || 0;
  if (data.steering !== undefined) v.steering = Number(data.steering) || 0;
  if (data.speed !== undefined) v.speed = Number(data.speed) || 0;
  if (data.fuel !== undefined) v.fuel = Number(data.fuel) || 100;
  if (data.car_type) v.car_type = String(data.car_type);
  if (data.car_db_id) v.car_db_id = String(data.car_db_id);

  broadcast({
    type: 'vehicle_update',
    vehicles: [{
      id: vid,
      position: v.position,
      rotation: v.rotation,
      speed: v.speed,
      steering: v.steering,
      occupants: v.occupants,
      ownerId: v.ownerId,
      owner_id: v.ownerId,
      type: v.car_type,
      car_type: v.car_type,
      car_db_id: v.car_db_id,
      fuel: v.fuel
    }]
  }, client.id);
}

function handleJoinVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = String(data.vehicleId || '');
  const seatIndex = Number(data.seatIndex) || 1;

  if (!vid || !vehicles.has(vid)) {
    send(ws, { type: 'error', message: 'Vehicle not found' });
    return;
  }

  const v = vehicles.get(vid);
  if (!Array.isArray(v.occupants)) v.occupants = [];

  // صندلی‌ها را تا ۵ تا پر کن
  while (v.occupants.length < 5) v.occupants.push(null);

  if (v.occupants[seatIndex]) {
    send(ws, { type: 'error', message: 'Seat is taken' });
    return;
  }

  // از ماشین قبلی خارج کن
  for (const [otherVid, otherV] of vehicles) {
    if (Array.isArray(otherV.occupants)) {
      const idx = otherV.occupants.indexOf(client.id);
      if (idx !== -1) {
        otherV.occupants[idx] = null;
      }
    }
  }

  v.occupants[seatIndex] = client.id;
  if (!v.ownerId) v.ownerId = client.id;

  broadcastToAll({
    type: 'seat_update',
    vehicleId: vid,
    occupants: v.occupants
  });

  console.log(`👤 ${client.name} joined vehicle ${vid} seat ${seatIndex}`);
}

function handleLeaveVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = String(data.vehicleId || '');
  if (!vid || !vehicles.has(vid)) return;

  const v = vehicles.get(vid);
  if (!Array.isArray(v.occupants)) return;

  const idx = v.occupants.indexOf(client.id);
  if (idx !== -1) {
    v.occupants[idx] = null;
  }

  // اگر کسی نماند، ماشین را حذف کن
  const hasAnyone = v.occupants.some(o => o);
  if (!hasAnyone) {
    vehicles.delete(vid);
    broadcastToAll({ type: 'vehicle_removed', vehicleId: vid, id: vid });
  } else {
    if (v.ownerId === client.id) {
      // صاحب را به اولین نفر منتقل کن
      const next = v.occupants.find(o => o);
      v.ownerId = next || '';
    }
    broadcastToAll({
      type: 'seat_update',
      vehicleId: vid,
      occupants: v.occupants
    });
  }
}

function handleSeatUpdate(ws, client, data) {
  if (!client.id) return;
  const vid = String(data.vehicleId || '');
  if (!vid || !vehicles.has(vid)) return;

  const v = vehicles.get(vid);
  if (Array.isArray(data.occupants)) {
    v.occupants = data.occupants;
  }

  broadcastToAll({
    type: 'seat_update',
    vehicleId: vid,
    occupants: v.occupants
  });
}

function handleBanPlayer(ws, client, data) {
  // فقط ادمین‌ها (level >= 1 در کلاینت چک می‌شود، اینجا ساده نگه می‌داریم)
  // اگر می‌خواهی سخت‌گیرانه‌تر کنی، لیست ادمین بساز
  if (!client.id) return;

  const targetId = String(data.targetId || '');
  if (!targetId) return;

  const duration = Number(data.duration);
  const reason = String(data.reason || 'بن شده توسط بازرسی');

  const permanent = duration <= 0;
  const expire = permanent ? -1 : Math.floor(Date.now() / 1000) + duration;

  bans[targetId] = {
    expire: expire,
    reason: reason,
    permanent: permanent,
    bannedBy: client.id,
    bannedAt: Math.floor(Date.now() / 1000)
  };
  saveBans();

  // به خود بازیکن بن‌شده اطلاع بده
  for (const [targetWs, targetClient] of clients) {
    if (targetClient.id === targetId) {
      targetClient.isBanned = true;
      send(targetWs, {
        type: 'you_are_banned',
        message: reason,
        reason: reason,
        expire: expire
      });
      break;
    }
  }

  console.log(`🚫 ${client.name} banned ${targetId} | permanent: ${permanent}`);
}

function handleUnbanPlayer(ws, client, data) {
  if (!client.id) return;

  const targetId = String(data.targetId || '');
  if (!targetId) return;

  if (bans[targetId]) {
    delete bans[targetId];
    saveBans();
  }

  // به بازیکن اطلاع بده
  for (const [targetWs, targetClient] of clients) {
    if (targetClient.id === targetId) {
      targetClient.isBanned = false;
      send(targetWs, { type: 'unbanned' });
      break;
    }
  }

  console.log(`✅ ${client.name} unbanned ${targetId}`);
}

function handleDisconnect(ws, client) {
  if (client.id) {
    players.delete(client.id);

    // از همه ماشین‌ها خارج کن
    for (const [vid, v] of vehicles) {
      if (Array.isArray(v.occupants)) {
        const idx = v.occupants.indexOf(client.id);
        if (idx !== -1) {
          v.occupants[idx] = null;
        }
      }
      if (v.ownerId === client.id) {
        const next = (v.occupants || []).find(o => o);
        v.ownerId = next || '';
      }
    }

    // ماشین‌های خالی را پاک کن
    for (const [vid, v] of [...vehicles]) {
      const hasAnyone = (v.occupants || []).some(o => o);
      if (!hasAnyone) {
        vehicles.delete(vid);
        broadcastToAll({ type: 'vehicle_removed', vehicleId: vid, id: vid });
      } else {
        broadcastToAll({
          type: 'seat_update',
          vehicleId: vid,
          occupants: v.occupants
        });
      }
    }

    broadcastToAll({ type: 'player_left', id: client.id });
    console.log(`👋 Player left: ${client.name} (${client.id})`);
  }
  clients.delete(ws);
}

// ==================== Cleanup & Keepalive ====================
setInterval(() => {
  const now = Date.now();
  for (const [ws, client] of clients) {
    if (now - client.lastPing > 45000) { // 45 ثانیه بدون پینگ
      console.log(`⏱️ Timeout: ${client.name || 'unknown'}`);
      try { ws.close(); } catch (_) {}
    }
  }
}, 15000);

// هر ۳۰ ثانیه full_state بفرست (برای همگام‌سازی)
setInterval(() => {
  if (clients.size > 0) {
    broadcastToAll(getFullState());
  }
}, 30000);

// ==================== Start ====================
server.listen(PORT, () => {
  console.log(`🚀 Poppo Multiplayer Server running on port ${PORT}`);
  console.log(`📁 Ban file: ${BAN_FILE}`);
});
