/**
 * มดนักสำรวจ - Real-time Multiplayer Server
 * -------------------------------------------------
 * Express serves the existing front-end (public/) as static files.
 * Socket.IO relays room/lobby/game events between all connected
 * devices (players, host, spectator screen) so the exact same
 * game code that used to run inside one browser (via BroadcastChannel)
 * now works across separate devices on the internet.
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

// Simple health check endpoint (useful for Render)
app.get('/healthz', (req, res) => res.send('ok'));

/**
 * In-memory room store.
 * rooms[roomCode] = {
 *   hostSocketId: string | null,
 *   players: [{ id, name, avatar, isHost }],
 *   started: boolean
 * }
 *
 * NOTE: This is intentionally simple in-memory state, fitting a
 * single-classroom / single-session use case. If the server restarts,
 * active rooms are lost (the same way refreshing all tabs used to
 * reset the old BroadcastChannel-based game).
 */
const rooms = {};

function getOrCreateRoom(roomCode) {
    if (!rooms[roomCode]) {
        rooms[roomCode] = { hostSocketId: null, players: [], started: false };
    }
    return rooms[roomCode];
}

function publicPlayersList(room) {
    return room.players.map(p => ({ id: p.id, name: p.name, avatar: p.avatar, isHost: !!p.isHost }));
}

io.on('connection', (socket) => {

    // ---- Room creation (index.html "create room" flow) ----
    socket.on('create-room', ({ roomCode }, ack) => {
        const room = rooms[roomCode];
        if (room && (room.hostSocketId || room.players.length > 0)) {
            if (typeof ack === 'function') ack({ ok: false, reason: 'taken' });
            return;
        }
        getOrCreateRoom(roomCode);
        if (typeof ack === 'function') ack({ ok: true });
    });

    // ---- Room existence check (index.html "join room" flow) ----
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

    // ---- Join a room's socket.io channel (waiting-room.html & board.html & spectator.html) ----
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

    // ---- Host starts the game ----
    socket.on('start-game', ({ roomCode, players }) => {
        const room = rooms[roomCode];
        if (!room) return;
        if (Array.isArray(players) && players.length > 0) room.players = players;
        room.started = true;
        io.to(roomCode).emit('game-start', { players: room.players });
    });

    // ---- In-game sync: relay every game event to everyone else in the room ----
    // This replaces BroadcastChannel: whichever device performs an action
    // (dice roll, quiz answer, movement...) sends the resulting state here,
    // and every other device in the room (other teams + spectator screen)
    // receives it and re-renders, exactly like the old same-browser tabs did.
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
        // Only auto-remove from the lobby list before the game has started.
        // Once a game is running we keep the roster stable so an accidental
        // refresh/disconnect doesn't reshuffle turn order mid-game.
        room.players = room.players.filter(p => p.name !== groupName);
    }
    if (room.hostSocketId === socket.id) room.hostSocketId = null;

    io.to(roomCode).emit('lobby-update', { players: publicPlayersList(room) });

    // Clean up empty, not-yet-started rooms
    if (room.players.length === 0 && !room.started) {
        delete rooms[roomCode];
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`มดนักสำรวจ server running on port ${PORT}`);
});
