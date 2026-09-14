/**
 * มดนักสำรวจ - Real-time Multiplayer Server
 */

const path = require('path');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

app.get('/healthz', (req, res) => res.send('ok'));

const rooms = {};

function getOrCreateRoom(roomCode) {
    if (!rooms[roomCode]) {
        rooms[roomCode] = { hostSocketId: null, players: [], started: false, gameState: {} };
    }
    return rooms[roomCode];
}

function publicPlayersList(room) {
    return room.players.map(p => ({ id: p.id, name: p.name, avatar: p.avatar, isHost: !!p.isHost }));
}

io.on('connection', (socket) => {

    socket.on('create-room', ({ roomCode }, ack) => {
        const room = rooms[roomCode];
        if (room && (room.hostSocketId || room.players.length > 0)) {
            if (typeof ack === 'function') ack({ ok: false, reason: 'taken' });
            return;
        }
        getOrCreateRoom(roomCode);
        if (typeof ack === 'function') ack({ ok: true });
    });

    socket.on('check-room', ({ roomCode }, ack) => {
        const room = rooms[roomCode];
        if (!room) {
            if (typeof ack === 'function') ack({ ok: false, reason: 'not_found' });
            return;
        }
        if (room.players.length >= 10) {
            if (typeof ack === 'function') ack({ ok: false, reason: 'full' });
            return;
        }
        if (typeof ack === 'function') ack({ ok: true });
    });

    socket.on('join-room', ({ roomCode, player, isHost }) => {
        if (!roomCode) return;
        const room = getOrCreateRoom(roomCode);

        socket.join(roomCode);
        socket.data.roomCode = roomCode;
        socket.data.groupName = player && player.name;

        if (isHost) room.hostSocketId = socket.id;

        if (player && player.name) {
            const exists = room.players.find(p => p.name === player.name);
            if (!exists) {
                room.players.push({
                    id: room.players.length + 1,
                    name: player.name,
                    avatar: player.avatar || 'ant',
                    isHost: !!isHost
                });
            } else if (isHost) {
                exists.isHost = true;
            }
        }

        io.to(roomCode).emit('lobby-update', { players: publicPlayersList(room) });
    });

    socket.on('start-game', ({ roomCode, players }) => {
        const room = rooms[roomCode];
        if (!room) return;
        if (Array.isArray(players) && players.length > 0) room.players = players;
        room.started = true;
        io.to(roomCode).emit('game-start', { players: room.players });
    });

    // ---- [เพิ่มส่วนนี้] REAL-TIME CARD ACTION & LOCK SYNC ----
    // จัดการการส่งเปิดไพ่ / ล็อกไพ่ ให้เห็นทันทีทุกหน้าจอ
    socket.on('card-action', ({ roomCode, actionType, playerId, cardId, isLocked, cardData }) => {
        if (!roomCode) return;
        
        // ส่งกระจายไปยังทุกคนในห้อง (รวมทั้งคนกดเองและคนอื่น)
        io.to(roomCode).emit('card-updated', {
            actionType, // 'draw' หรือ 'lock'
            playerId,
            cardId,
            isLocked,
            cardData
        });
    });

    // ---- General Game Sync (relay ให้เครื่องอื่น) ----
    socket.on('game-sync', ({ roomCode, ...data }) => {
        if (!roomCode) return;
        socket.to(roomCode).emit('game-sync', data);
    });

    socket.on('leave-room', ({ roomCode }) => {
        handleLeave(socket, roomCode);
    });

    socket.on('disconnect', () => {
        handleLeave(socket, socket.data.roomCode);
    });
});

function handleLeave(socket, roomCode) {
    if (!roomCode) return;
    const room = rooms[roomCode];
    if (!room) return;

    const groupName = socket.data.groupName;
    if (groupName && !room.started) {
        room.players = room.players.filter(p => p.name !== groupName);
    }
    if (room.hostSocketId === socket.id) room.hostSocketId = null;

    io.to(roomCode).emit('lobby-update', { players: publicPlayersList(room) });

    if (room.players.length === 0 && !room.started) {
        delete rooms[roomCode];
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`มดนักสำรวจ server running on port ${PORT}`);
});
