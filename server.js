import { WebSocketServer } from 'ws';
import { randomUUID } from 'crypto';
import fs from 'fs';
import path from 'path';
import config from './config.js';

// ============================================================
// Packet Types
// ============================================================
const PacketType = {
    SET_ID: 'set_id',
    FULL_STATE: 'full_state',
    UPDATE: 'update',
    PLAYER_LEFT: 'player_left',
    CHAT: 'chat',
    PING: 'ping',
    PONG: 'pong',
    ERROR: 'error',
    VEHICLE_STATE: 'vehicle_state',
    SEAT_UPDATE: 'seat_update',
    VEHICLE_REMOVED: 'vehicle_removed',
    CREATE_VEHICLE: 'create_vehicle',
    JOIN_VEHICLE: 'join_vehicle',
    LEAVE_VEHICLE: 'leave_vehicle',
    UPDATE_VEHICLE: 'update_vehicle',
    BAN_PLAYER: 'ban_player',
    UNBAN_PLAYER: 'unban_player',
    BAN_LIST: 'ban_list',
    YOU_ARE_BANNED: 'you_are_banned'
};

// ============================================================
// مدیریت پوشه داده
// ============================================================
if (!fs.existsSync('./data')) {
    fs.mkdirSync('./data');
}

const banFilePath = path.join(process.cwd(), 'data', 'bans.json');
if (!fs.existsSync(banFilePath)) {
    fs.writeFileSync(banFilePath, JSON.stringify([]));
}
let bans = JSON.parse(fs.readFileSync(banFilePath, 'utf8'));

// ============================================================
// کلاس Vehicle
// ============================================================
class Vehicle {
    constructor(id, ownerId, position, rotation, type, car_db_id) {
        this.id = id;
        this.ownerId = ownerId;
        this.position = position || { x: 0, y: 0, z: 0 };
        this.rotation = rotation || 0;
        this.type = type || 'car';
        this.car_db_id = car_db_id || null;
        this.steering = 0;
        this.speed = 0;
        this.fuel = 100;
        this.occupants = new Map();
        this.lastUpdate = Date.now();
    }

    addOccupant(playerId, seatIndex = 0) {
        this.occupants.set(playerId, seatIndex);
    }

    removeOccupant(playerId) {
        this.occupants.delete(playerId);
    }

    update(position, rotation, speed, fuel, steering) {
        if (position) this.position = position;
        if (rotation !== undefined) this.rotation = rotation;
        if (speed !== undefined) this.speed = speed;
        if (fuel !== undefined) this.fuel = Math.max(0, Math.min(100, fuel));
        if (steering !== undefined) this.steering = steering;
        this.lastUpdate = Date.now();
    }

    getState() {
        return {
            id: this.id,
            ownerId: this.ownerId,
            type: this.type,
            car_db_id: this.car_db_id,
            position: this.position,
            rotation: this.rotation,
            steering: this.steering,
            speed: this.speed,
            fuel: this.fuel,
            occupants: Array.from(this.occupants.entries())
        };
    }
}

// ============================================================
// کلاس VehicleManager
// ============================================================
class VehicleManager {
    constructor() {
        this.vehicles = new Map();
        this.nextVehicleId = 1;
    }

    createVehicle(ownerId, position, rotation, type, car_db_id) {
        const id = this.nextVehicleId++;
        const vehicle = new Vehicle(id, ownerId, position, rotation, type, car_db_id);
        this.vehicles.set(id, vehicle);
        return vehicle;
    }

    removeVehicle(id) {
        this.vehicles.delete(id);
    }

    getVehicle(id) {
        // پشتیبانی از هم عدد و هم رشته
        return this.vehicles.get(id) || this.vehicles.get(Number(id)) || null;
    }

    getAllVehicles() {
        return Array.from(this.vehicles.values()).map(v => v.getState());
    }

    updateVehicle(id, position, rotation, speed, fuel, steering) {
        const vehicle = this.getVehicle(id);
        if (vehicle) {
            vehicle.update(position, rotation, speed, fuel, steering);
            return true;
        }
        return false;
    }

    findVehicleByPlayer(playerId) {
        for (const vehicle of this.vehicles.values()) {
            if (vehicle.occupants.has(playerId) || vehicle.ownerId === playerId) {
                return vehicle;
            }
        }
        return null;
    }
}

// ============================================================
// نمونه‌های اصلی
// ============================================================
const wss = new WebSocketServer({ port: config.port || 8080 });
const vehicleManager = new VehicleManager();
const players = new Map();
const userMap = new Map();

// ============================================================
// توابع کمکی
// ============================================================
function broadcast(message, excludeId = null) {
    const data = JSON.stringify(message);
    for (const player of players.values()) {
        if (excludeId && player.id === excludeId) continue;
        if (player.ws.readyState === 1) {
            try {
                player.ws.send(data);
            } catch (e) {}
        }
    }
}

function sendToPlayer(playerId, message) {
    const player = players.get(playerId);
    if (player && player.ws.readyState === 1) {
        try {
            player.ws.send(JSON.stringify(message));
        } catch (e) {}
    }
}

// ============================================================
// مدیریت اتصالات WebSocket
// ============================================================
wss.on('connection', (ws) => {
    const player = {
        id: null,
        user_id: null,
        name: 'Unknown',
        job: 'بیکار',
        position: { x: 0, y: 0, z: 0 },
        rotation: 0,
        ws: ws,
        vehicleId: null,
        send: function (data) {
            if (this.ws.readyState === 1) {
                try {
                    this.ws.send(JSON.stringify(data));
                } catch (e) {}
            }
        },
        close: function (reason) {
            try {
                this.ws.close(1000, reason);
            } catch (e) {}
        }
    };

    const tempId = randomUUID();
    player.id = tempId;
    players.set(tempId, player);

    console.log('🔗 New connection received, waiting for SET_ID...');

    ws.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            const { type } = message;

            if (type === PacketType.SET_ID) {
                const clientId = message.id || message.user_id;
                if (!clientId) {
                    player.send({ type: PacketType.ERROR, message: 'Missing ID' });
                    return;
                }

                const isBanned = bans.some(b => b.id === clientId && (!b.expires || b.expires > Date.now()));
                if (isBanned) {
                    player.send({ type: PacketType.YOU_ARE_BANNED, message: 'You are banned from this server' });
                    player.close('Banned');
                    players.delete(tempId);
                    return;
                }

                const existingPlayer = userMap.get(clientId);
                if (existingPlayer) {
                    const oldPlayer = players.get(existingPlayer);
                    if (oldPlayer) {
                        try {
                            oldPlayer.send({ type: PacketType.ERROR, message: 'Replaced by new connection' });
                        } catch (e) {}
                        oldPlayer.close('Replaced by new connection');
                    }
                    players.delete(existingPlayer);
                }

                players.delete(tempId);
                player.id = clientId;
                player.user_id = clientId;
                if (message.name) player.name = message.name;
                if (message.job) player.job = message.job;
                if (message.position) player.position = message.position;
                if (message.rotation !== undefined) player.rotation = message.rotation;

                players.set(clientId, player);
                userMap.set(clientId, clientId);

                player.send({ type: PacketType.SET_ID, id: clientId });
                console.log(`✅ Client connected with ID: ${clientId}`);

                // ارسال وضعیت کامل
                player.send({
                    type: PacketType.FULL_STATE,
                    players: Array.from(players.values()).map(p => ({
                        id: p.id,
                        user_id: p.user_id || p.id,
                        name: p.name,
                        job: p.job,
                        position: p.position,
                        rotation: p.rotation
                    })),
                    vehicles: vehicleManager.getAllVehicles()
                });

                // اطلاع به بقیه
                broadcast({
                    type: PacketType.UPDATE,
                    id: clientId,
                    user_id: clientId,
                    name: player.name,
                    job: player.job,
                    position: player.position,
                    rotation: player.rotation
                }, clientId);

                return;
            }

            if (!player.user_id) {
                player.send({ type: PacketType.ERROR, message: 'Not authenticated' });
                return;
            }

            handleMessage(player, message);

        } catch (e) {
            console.error('Message parse error:', e);
            try {
                player.send({ type: PacketType.ERROR, message: 'Invalid JSON' });
            } catch (err) {}
        }
    });

    ws.on('close', () => {
        const pid = player.id;
        if (pid) {
            // حذف از ماشین
            if (player.vehicleId) {
                const vehicle = vehicleManager.getVehicle(player.vehicleId);
                if (vehicle) {
                    vehicle.removeOccupant(player.id);
                    // اگر صاحب ماشین بود و کسی نمانده، ماشین را پاک کن
                    if (vehicle.ownerId === player.id && vehicle.occupants.size === 0) {
                        vehicleManager.removeVehicle(vehicle.id);
                        broadcast({
                            type: PacketType.VEHICLE_REMOVED,
                            vehicleId: vehicle.id
                        });
                    } else {
                        broadcast({
                            type: PacketType.SEAT_UPDATE,
                            vehicleId: vehicle.id,
                            occupants: Array.from(vehicle.occupants.entries())
                        });
                    }
                }
            }

            players.delete(pid);
            if (player.user_id) {
                userMap.delete(player.user_id);
            }
            broadcast({ type: PacketType.PLAYER_LEFT, id: pid });
            console.log(`❌ Client disconnected: ${pid}`);
        }
    });

    ws.on('error', (err) => {
        console.error('WebSocket error:', err);
    });
});

// ============================================================
// پردازش پیام‌ها
// ============================================================
function handleMessage(player, message) {
    const { type } = message;

    switch (type) {
        case PacketType.UPDATE: {
            if (message.position) player.position = message.position;
            if (message.rotation !== undefined) player.rotation = message.rotation;
            if (message.name) player.name = message.name;
            if (message.job) player.job = message.job;

            broadcast({
                type: PacketType.UPDATE,
                id: player.id,
                user_id: player.user_id,
                name: player.name,
                job: player.job,
                position: player.position,
                rotation: player.rotation,
                anim: message.anim,
                speed: message.speed,
                is_grounded: message.is_grounded
            }, player.id);
            break;
        }

        case PacketType.CHAT: {
            broadcast({
                type: PacketType.CHAT,
                sender_id: player.id,
                sender_name: player.name,
                message: message.message
            });
            break;
        }

        case PacketType.PING: {
            player.send({ type: PacketType.PONG });
            break;
        }

        case PacketType.PONG:
            break;

        // ============================================================
        // ماشین‌ها
        // ============================================================
        case PacketType.CREATE_VEHICLE: {
            const pos = message.position || { x: 0, y: 0, z: 0 };
            const rot = message.rotation !== undefined ? message.rotation : 0;
            const carType = message.car_type || message.type_name || 'car';
            const carDbId = message.car_db_id || null;
            const steering = message.steering || 0;

            // اگر قبلاً ماشین داشت، پاک کن
            if (player.vehicleId) {
                const old = vehicleManager.getVehicle(player.vehicleId);
                if (old) {
                    vehicleManager.removeVehicle(old.id);
                    broadcast({
                        type: PacketType.VEHICLE_REMOVED,
                        vehicleId: old.id
                    });
                }
            }

            const vehicle = vehicleManager.createVehicle(
                player.id,
                { x: pos.x, y: pos.y, z: pos.z },
                rot,
                carType,
                carDbId
            );
            vehicle.steering = steering;
            vehicle.addOccupant(player.id, 0);
            player.vehicleId = vehicle.id;

            console.log(`🚗 Vehicle created id=${vehicle.id} type=${carType} pos=`, vehicle.position);

            broadcast({
                type: PacketType.VEHICLE_STATE,
                vehicles: vehicleManager.getAllVehicles()
            });
            break;
        }

        case PacketType.UPDATE_VEHICLE: {
            const vid = message.vehicleId || message.id;
            if (!vid || !message.position) break;

            const vehicle = vehicleManager.getVehicle(vid);
            if (!vehicle) break;

            // فقط صاحب یا سرنشین بتواند آپدیت کند
            if (vehicle.ownerId !== player.id && !vehicle.occupants.has(player.id)) {
                break;
            }

            vehicle.update(
                {
                    x: message.position.x,
                    y: message.position.y,
                    z: message.position.z
                },
                message.rotation,
                message.speed,
                message.fuel,
                message.steering
            );

            if (message.car_type) vehicle.type = message.car_type;
            if (message.car_db_id) vehicle.car_db_id = message.car_db_id;

            broadcast({
                type: PacketType.VEHICLE_STATE,
                vehicles: vehicleManager.getAllVehicles()
            });
            break;
        }

        case PacketType.JOIN_VEHICLE: {
            const joinVid = message.vehicleId;
            const joinVehicle = vehicleManager.getVehicle(joinVid);
            if (joinVehicle) {
                const seatIndex = message.seatIndex || 1;
                joinVehicle.addOccupant(player.id, seatIndex);
                player.vehicleId = joinVid;
                broadcast({
                    type: PacketType.SEAT_UPDATE,
                    vehicleId: joinVid,
                    occupants: Array.from(joinVehicle.occupants.entries())
                });
            }
            break;
        }

        case PacketType.LEAVE_VEHICLE: {
            const leaveVid = player.vehicleId || message.vehicleId;
            if (leaveVid) {
                const leaveVehicle = vehicleManager.getVehicle(leaveVid);
                if (leaveVehicle) {
                    leaveVehicle.removeOccupant(player.id);

                    // اگر صاحب رفت و کسی نماند، ماشین را پاک کن
                    if (leaveVehicle.ownerId === player.id && leaveVehicle.occupants.size === 0) {
                        vehicleManager.removeVehicle(leaveVehicle.id);
                        broadcast({
                            type: PacketType.VEHICLE_REMOVED,
                            vehicleId: leaveVehicle.id
                        });
                    } else {
                        broadcast({
                            type: PacketType.SEAT_UPDATE,
                            vehicleId: leaveVid,
                            occupants: Array.from(leaveVehicle.occupants.entries())
                        });
                    }
                }
                player.vehicleId = null;
            }
            break;
        }

        // ============================================================
        // بن
        // ============================================================
        case PacketType.BAN_PLAYER: {
            if (player.job === 'ادمین') {
                const targetId = message.targetId || message.target_id;
                const duration = message.duration || 3600000;
                const reason = message.reason || 'No reason provided';
                const banEntry = {
                    id: targetId,
                    reason: reason,
                    expires: duration === -1 ? null : Date.now() + duration
                };
                bans.push(banEntry);
                fs.writeFileSync(banFilePath, JSON.stringify(bans));

                const targetPlayer = players.get(targetId);
                if (targetPlayer) {
                    targetPlayer.send({
                        type: PacketType.YOU_ARE_BANNED,
                        message: `You are banned: ${reason}`
                    });
                    targetPlayer.close('Banned');
                    players.delete(targetId);
                    userMap.delete(targetId);
                }
                broadcast({ type: PacketType.BAN_LIST, bans: bans });
            }
            break;
        }

        case PacketType.UNBAN_PLAYER: {
            if (player.job === 'ادمین') {
                const targetId = message.targetId || message.target_id;
                bans = bans.filter(b => b.id !== targetId);
                fs.writeFileSync(banFilePath, JSON.stringify(bans));
                broadcast({ type: PacketType.BAN_LIST, bans: bans });
            }
            break;
        }

        case PacketType.BAN_LIST: {
            if (player.job === 'ادمین') {
                player.send({ type: PacketType.BAN_LIST, bans: bans });
            }
            break;
        }

        default:
            console.log(`⚠️ Unknown message type: ${type}`);
    }
}

// ============================================================
// ارسال وضعیت کامل هر ۵۰۰ms
// ============================================================
setInterval(() => {
    const fullState = {
        type: PacketType.FULL_STATE,
        players: Array.from(players.values()).map(p => ({
            id: p.id,
            user_id: p.user_id || p.id,
            name: p.name,
            job: p.job,
            position: p.position,
            rotation: p.rotation
        })),
        vehicles: vehicleManager.getAllVehicles()
    };

    for (const player of players.values()) {
        if (player.ws.readyState === 1) {
            try {
                player.ws.send(JSON.stringify(fullState));
            } catch (e) {}
        }
    }
}, 500);

// ============================================================
// Keep-alive هر ۱۰ ثانیه
// ============================================================
setInterval(() => {
    for (const player of players.values()) {
        if (player.ws.readyState === 1) {
            try {
                player.ws.send(JSON.stringify({ type: PacketType.PING }));
            } catch (e) {}
        }
    }
}, 10000);

// ============================================================
// خاموش شدن
// ============================================================
process.on('SIGTERM', () => {
    console.log('Shutting down...');
    wss.close();
    process.exit(0);
});

console.log(`🚀 Server running on port ${config.port || 8080}`);
console.log('🚀 Server is ready and waiting for connections!');
