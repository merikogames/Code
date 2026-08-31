// server.js
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const BAN_FILE = path.join(__dirname, 'bans.json');

// ==================== State ====================
const clients = new Map();
const players = new Map();
const vehicles = new Map();
let bans = {};

// ==================== Ban ====================
function loadBans() {
  try {
    if (fs.existsSync(BAN_FILE)) {
      bans = JSON.parse(fs.readFileSync(BAN_FILE, 'utf8'));
      console.log(`✅ Loaded ${Object.keys(bans).length} bans`);
    }
  } catch (e) {
    bans = {};
  }
}

function saveBans() {
  try {
    fs.writeFileSync(BAN_FILE, JSON.stringify(bans, null, 2));
  } catch (e) {
    console.error('saveBans error:', e.message);
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
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function broadcastToAll(data) {
  const msg = JSON.stringify(data);
  for (const [ws] of clients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  }
}

function formatVehicle(v) {
  return {
    id: v.id,
    position: v.position,
    rotation: v.rotation,
    speed: v.speed || 0,
    steering: v.steering || 0,
    occupants: v.occupants || [null, null, null, null, null],
    ownerId: v.ownerId || '',
    owner_id: v.ownerId || '',
    type: v.car_type || 'car',
    car_type: v.car_type || 'car',
    car_db_id: v.car_db_id || '',
    fuel: v.fuel !== undefined ? v.fuel : 100
  };
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
  for (const [, v] of vehicles) {
    vehiclesArr.push(formatVehicle(v));
  }

  return { type: 'full_state', players: playersArr, vehicles: vehiclesArr };
}

// فقط از صندلی‌ها خارج می‌کند — ماشین را حذف نمی‌کند
function removePlayerFromAllVehicles(playerId) {
  for (const [, v] of vehicles) {
    if (!Array.isArray(v.occupants)) continue;
    for (let i = 0; i < v.occupants.length; i++) {
      if (v.occupants[i] === playerId) {
        v.occupants[i] = null;
      }
    }
  }
}

// ==================== Server ====================
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Poppo Multiplayer Server OK\n');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
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
      case 'set_id':           handleSetId(ws, client, data); break;
      case 'update':          handlePlayerUpdate(ws, client, data); break;
      case 'ping':
        client.lastPing = Date.now();
        send(ws, { type: 'pong' });
        break;
      case 'pong':
        client.lastPing = Date.now();
        break;
      case 'chat':            handleChat(ws, client, data); break;
      case 'create_vehicle':  handleCreateVehicle(ws, client, data); break;
      case 'update_vehicle':  handleUpdateVehicle(ws, client, data); break;
      case 'join_vehicle':    handleJoinVehicle(ws, client, data); break;
      case 'leave_vehicle':   handleLeaveVehicle(ws, client, data); break;
      case 'seat_update':     handleSeatUpdate(ws, client, data); break;
      case 'ban_player':      handleBanPlayer(ws, client, data); break;
      case 'unban_player':    handleUnbanPlayer(ws, client, data); break;
      default: break;
    }
  });

  ws.on('close', () => handleDisconnect(ws, client));
  ws.on('error', (err) => console.error('WS error:', err.message));
});

// ==================== Handlers ====================
function handleSetId(ws, client, data) {
  const id = String(data.id || '');
  if (!id) {
    send(ws, { type: 'error', message: 'Invalid id' });
    return;
  }

  // اتصال قبلی با همین id را ببند
  for (const [oldWs, oldClient] of clients) {
    if (oldClient.id === id && oldWs !== ws) {
      try { oldWs.close(); } catch (_) {}
      clients.delete(oldWs);
      players.delete(id);
      removePlayerFromAllVehicles(id);
    }
  }

  client.id = id;
  client.name = String(data.name || 'Player_' + id.substring(0, 6));
  client.job = String(data.job || 'بیکار');
  client.token = String(data.token || '');

  if (isBanned(id)) {
    client.isBanned = true;
    const banInfo = getBanInfo(id);
    send(ws, {
      type: 'you_are_banned',
      message: banInfo.reason,
      reason: banInfo.reason,
      expire: banInfo.expire
    });
    console.log(`🚫 Banned connected: ${client.name}`);
  } else {
    client.isBanned = false;
  }

  players.set(id, {
    name: client.name,
    job: client.job,
    position: { x: 0, y: 0, z: 0 },
    rotation: 0,
    anim: 0,
    speed: 0,
    is_grounded: true
  });

  send(ws, { type: 'set_id', id, data: { token: client.token } });
  broadcastToAll(getFullState());
  console.log(`✅ Joined: ${client.name} (${id})`);
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
  if (data.name) { p.name = String(data.name); client.name = p.name; }
  if (data.job) { p.job = String(data.job); client.job = p.job; }
  if (data.anim !== undefined) p.anim = Number(data.anim) || 0;
  if (data.speed !== undefined) p.speed = Number(data.speed) || 0;
  if (data.is_grounded !== undefined) p.is_grounded = !!data.is_grounded;

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
    send(ws, { type: 'error', message: 'شما بن شده‌اید' });
    return;
  }
  const message = String(data.message || '').trim();
  if (!message || message.length > 300) return;

  broadcastToAll({
    type: 'chat',
    sender_id: client.id,
    sender_name: client.name,
    message
  });
}

// ---------- ماشین‌ها ----------
function handleCreateVehicle(ws, client, data) {
  if (!client.id) return;

  // اگر قبلاً ماشینی دارد که صاحبش است → همان را آپدیت کن، جدید نساز
  for (const [vid, v] of vehicles) {
    if (v.ownerId === client.id) {
      if (data.position) {
        v.position = {
          x: Number(data.position.x) || 0,
          y: Number(data.position.y) || 0,
          z: Number(data.position.z) || 0
        };
      }
      if (data.rotation !== undefined) v.rotation = Number(data.rotation) || 0;
      if (data.steering !== undefined) v.steering = Number(data.steering) || 0;
      if (data.car_type) v.car_type = String(data.car_type);
      if (data.car_db_id) v.car_db_id = String(data.car_db_id);

      if (!Array.isArray(v.occupants)) v.occupants = [null, null, null, null, null];
      while (v.occupants.length < 5) v.occupants.push(null);

      // اگر داخل هیچ صندلی نیست، بگذار راننده
      if (!v.occupants.includes(client.id)) {
        v.occupants[0] = client.id;
      }

      broadcastToAll({ type: 'vehicle_update', vehicles: [formatVehicle(v)] });
      return;
    }
  }

  // ماشین جدید
  const vid = genId();
  const vehicle = {
    id: vid,
    position: data.position ? {
      x: Number(data.position.x) || 0,
      y: Number(data.position.y) || 0,
      z: Number(data.position.z) || 0
    } : { x: 0, y: 0, z: 0 },
    rotation: Number(data.rotation) || 0,
    speed: 0,
    steering: Number(data.steering) || 0,
    occupants: [client.id, null, null, null, null],
    ownerId: client.id,
    car_type: String(data.car_type || 'car'),
    car_db_id: String(data.car_db_id || ''),
    fuel: 100
  };

  vehicles.set(vid, vehicle);
  broadcastToAll({ type: 'vehicle_update', vehicles: [formatVehicle(vehicle)] });
  console.log(`🚗 Created: ${vid} by ${client.name}`);
}

function handleUpdateVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = String(data.vehicleId || data.id || '');
  if (!vid || !vehicles.has(vid)) return;

  const v = vehicles.get(vid);
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

  broadcastToAll({ type: 'vehicle_update', vehicles: [formatVehicle(v)] });
}

function handleJoinVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = String(data.vehicleId || '');
  let seatIndex = Number(data.seatIndex);
  if (isNaN(seatIndex) || seatIndex < 0) seatIndex = 1;

  if (!vid || !vehicles.has(vid)) {
    send(ws, { type: 'error', message: 'Vehicle not found' });
    return;
  }

  const v = vehicles.get(vid);
  if (!Array.isArray(v.occupants)) v.occupants = [null, null, null, null, null];
  while (v.occupants.length < 5) v.occupants.push(null);

  // از همه ماشین‌ها خارج کن (ماشین‌ها حذف نمی‌شوند)
  removePlayerFromAllVehicles(client.id);

  // صندلی خالی پیدا کن
  if (v.occupants[seatIndex]) {
    let found = -1;
    for (let i = 1; i < v.occupants.length; i++) {
      if (!v.occupants[i]) { found = i; break; }
    }
    if (found === -1) {
      send(ws, { type: 'error', message: 'No free seat' });
      return;
    }
    seatIndex = found;
  }

  v.occupants[seatIndex] = client.id;
  // owner عوض نمی‌شود

  broadcastToAll({ type: 'seat_update', vehicleId: vid, occupants: v.occupants });
  broadcastToAll({ type: 'vehicle_update', vehicles: [formatVehicle(v)] });
  console.log(`👤 ${client.name} joined ${vid} seat ${seatIndex}`);
}

function handleLeaveVehicle(ws, client, data) {
  if (!client.id) return;

  const vid = String(data.vehicleId || '');
  removePlayerFromAllVehicles(client.id);

  // ماشین حذف نمی‌شود — فقط seat آپدیت می‌شود
  if (vid && vehicles.has(vid)) {
    const v = vehicles.get(vid);
    broadcastToAll({ type: 'seat_update', vehicleId: vid, occupants: v.occupants });
    broadcastToAll({ type: 'vehicle_update', vehicles: [formatVehicle(v)] });
  } else {
    for (const [id, v] of vehicles) {
      broadcastToAll({ type: 'seat_update', vehicleId: id, occupants: v.occupants });
    }
  }
  console.log(`🚪 ${client.name} left vehicle`);
}

function handleSeatUpdate(ws, client, data) {
  if (!client.id) return;
  const vid = String(data.vehicleId || '');
  if (!vid || !vehicles.has(vid)) return;

  const v = vehicles.get(vid);
  if (Array.isArray(data.occupants)) v.occupants = data.occupants;

  broadcastToAll({ type: 'seat_update', vehicleId: vid, occupants: v.occupants });
}

function handleBanPlayer(ws, client, data) {
  if (!client.id) return;
  const targetId = String(data.targetId || '');
  if (!targetId) return;

  const duration = Number(data.duration);
  const reason = String(data.reason || 'بن شده توسط بازرسی');
  const permanent = duration <= 0;
  const expire = permanent ? -1 : Math.floor(Date.now() / 1000) + duration;

  bans[targetId] = {
    expire,
    reason,
    permanent,
    bannedBy: client.id,
    bannedAt: Math.floor(Date.now() / 1000)
  };
  saveBans();

  for (const [targetWs, targetClient] of clients) {
    if (targetClient.id === targetId) {
      targetClient.isBanned = true;
      send(targetWs, {
        type: 'you_are_banned',
        message: reason,
        reason,
        expire
      });
      break;
    }
  }
  console.log(`🚫 Ban: ${targetId} by ${client.name}`);
}

function handleUnbanPlayer(ws, client, data) {
  if (!client.id) return;
  const targetId = String(data.targetId || '');
  if (!targetId) return;

  if (bans[targetId]) {
    delete bans[targetId];
    saveBans();
  }

  for (const [targetWs, targetClient] of clients) {
    if (targetClient.id === targetId) {
      targetClient.isBanned = false;
      send(targetWs, { type: 'unbanned' });
      break;
    }
  }
  console.log(`✅ Unban: ${targetId}`);
}

function handleDisconnect(ws, client) {
  if (client.id) {
    players.delete(client.id);
    removePlayerFromAllVehicles(client.id); // فقط از صندلی‌ها خارج می‌شود
    broadcastToAll({ type: 'player_left', id: client.id });
    // ماشین‌ها را broadcast کن تا بقیه ببینند صندلی خالی شده
    for (const [vid, v] of vehicles) {
      broadcastToAll({ type: 'seat_update', vehicleId: vid, occupants: v.occupants });
    }
    console.log(`👋 Left: ${client.name}`);
  }
  clients.delete(ws);
}

// ==================== Keepalive ====================
setInterval(() => {
  const now = Date.now();
  for (const [ws, client] of clients) {
    if (now - client.lastPing > 45000) {
      try { ws.close(); } catch (_) {}
    }
  }
}, 15000);

setInterval(() => {
  if (clients.size > 0) broadcastToAll(getFullState());
}, 30000);

server.listen(PORT, () => {
  console.log(`🚀 Server on port ${PORT}`);
});
