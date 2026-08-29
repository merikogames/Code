const fs = require('fs');
const path = require('path');
const BANS_FILE = path.join(__dirname, '../data/bans.json');

class BanManager {
    static loadBans() {
        try {
            if (fs.existsSync(BANS_FILE)) {
                const data = fs.readFileSync(BANS_FILE, 'utf8');
                return JSON.parse(data);
            }
        } catch (e) {
            return {};
        }
    }

    static saveBans(bans) {
        const dir = path.dirname(BANS_FILE);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        fs.writeFileSync(BANS_FILE, JSON.stringify(bans, null, 2));
    }

    static banPlayer(playerId, duration, reason = '') {
        const bans = BanManager.loadBans();
        bans[playerId] = {
            until: duration === -1 ? -1 : Date.now() + duration * 1000,
            reason: reason || 'No reason'
        };
        BanManager.saveBans(bans);
        console.log(`[BAN] ${playerId} banned until ${bans[playerId].until}`);
    }

    static unbanPlayer(playerId) {
        const bans = BanManager.loadBans();
        if (bans[playerId]) {
            delete bans[playerId];
            BanManager.saveBans(bans);
            console.log(`[UNBAN] ${playerId} unbanned`);
        }
    }

    static isBanned(playerId) {
        const bans = BanManager.loadBans();
        const ban = bans[playerId];
        if (!ban) return false;
        if (ban.until === -1) return true;
        if (Date.now() > ban.until) {
            delete bans[playerId];
            BanManager.saveBans(bans);
            return false;
        }
        return true;
    }
}

module.exports = BanManager;
