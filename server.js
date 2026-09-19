/**
 * มดนักสำรวจ - Real-time Multiplayer Server
 * -------------------------------------------------
 * Express serves the existing front-end (public/) as static files.
 * Socket.IO relays room/lobby/game events between all connected
 * devices (players, host, spectator screen).
 *
 * IMPORTANT: this server is more than a dumb relay. It also keeps a
 * lightweight "last known state" snapshot per room. That snapshot is
 * what makes multi-device play reliable:
 *   - A spectator screen (or a player who refreshes/reconnects) gets
 *     caught up immediately with the latest state instead of showing
 *     stale/blank info until the next random event happens to arrive.
 *   - The turn order ("wheel" result) is cached the moment the host
 *     decides it, so a client that briefly missed the broadcast (e.g.
 *     it was still finishing its own page navigation) can request it
 *     again instead of getting stuck forever.
 *
 * The turn-by-turn gameplay logic itself still lives in the browser
 * (board.html) exactly like before - this server does not referee
 * dice rolls or quiz answers. What changed is that only ONE writer
 * per shared decision is trusted (the host for turn order, the
 * player-whose-turn-it-is for everything else), and the server
 * remembers what was last decided so every device - however many are
 * connected, up to 10 - converges on the same picture.
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

const MAX_PLAYERS_PER_ROOM = 10;

app.use(express.static(path.join(__dirname, 'public'), {
    // Long-term caching for static assets (images/etc). index.html/board.html
    // etc. are small and change rarely; browsers will still revalidate HTML
    // via ETag. This mainly speeds up repeat loads of the big image assets.
    maxAge: '1d',
    etag: true
}));

// Simple health check endpoint (useful for Render)
app.get('/healthz', (req, res) => res.send('ok'));

/**
 * In-memory room store.
 * rooms[roomCode] = {
 *   hostSocketId: string | null,
 *   players: [{ id, name, avatar, isHost }],
 *   started: boolean,
 *   phase: 'lobby' | 'ordering' | 'playing' | 'finished',
 *   state: {
 *     players: [...] | null,        // latest full players array (positions/scores/etc.)
 *     currentTurnIndex: number,
 *     order: [...] | null,          // the decided turn order (wheel result)
 *     lastEventType: string | null,
 *     lastEventData: object | null  // last broadcast payload, for context on catch-up
 *   }
 * }
 *
 * NOTE: This is intentionally simple in-memory state, fitting a
 * single-classroom / single-session use case. If the server restarts,
 * active rooms are lost.
 */
const rooms = {};

function getOrCreateRoom(roomCode) {
    if (!rooms[roomCode]) {
        rooms[roomCode] = {
            hostSocketId: null,
            players: [],
            started: false,
            phase: 'lobby',
            state: {
                players: null,
                currentTurnIndex: 0,
                order: null,
                lastEventType: null,
                lastEventData: null
            }
        };
    }
    return rooms[roomCode];
}

function publicPlayersList(room) {
    return room.players.map(p => ({ id: p.id, name: p.name, avatar: p.avatar, isHost: !!p.isHost }));
}

/** Build the snapshot payload sent to a client that needs to "catch up". */
function buildSnapshot(room) {
    return {
        type: 'STATE_SNAPSHOT',
        phase: room.phase,
        // Deliberately NOT falling back to room.players (the bare lobby
        // roster) here: that shape lacks pos/score/coins/powerups, and a
        // client would overwrite its own correctly-initialized game state
        // with it. null is safe - the client's generic "if (playersState)"
        // check simply skips the assignment when nothing useful is cached yet.
        playersState: room.state.players || null,
        currentTurnIndex: room.state.currentTurnIndex || 0,
        order: room.state.order || null,
        lastEventType: room.state.lastEventType,
        lastEventData: room.state.lastEventData
    };
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
        if (room.players.length >= MAX_PLAYERS_PER_ROOM) {
            if (typeof ack === 'function') ack({ ok: false, reason: 'full' });
            return;
        }
        if (room.started) {
            if (typeof ack === 'function') ack({ ok: false, reason: 'started' });
            return;
        }
        if (typeof ack === 'function') ack({ ok: true });
    });

    // ---- Join a room's socket.io channel (waiting-room.html & board.html & spectator.html) ----
    socket.on('join-room', ({ roomCode, player, isHost }, ack) => {
        if (!roomCode) return;
        const room = getOrCreateRoom(roomCode);

        socket.join(roomCode);
        socket.data.roomCode = roomCode;
        socket.data.groupName = player && player.name;

        if (isHost) room.hostSocketId = socket.id;

        if (player && player.name) {
            const exists = room.players.find(p => p.name === player.name);
            if (!exists) {
                // Hard cap: never let a room exceed MAX_PLAYERS_PER_ROOM,
                // even under race conditions where several people join at
                // almost the same instant (check-room only checked earlier).
                if (room.players.length >= MAX_PLAYERS_PER_ROOM) {
                    socket.emit('room-full');
                    if (typeof ack === 'function') ack({ ok: false, reason: 'full' });
                    return;
                }
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

        // Catch-up: if this client is joining/rejoining a room whose game
        // has already started (spectator opened late, a player refreshed,
        // a flaky connection reconnected...), immediately send them the
        // latest known state instead of leaving them blank until the next
        // live event happens to arrive.
        if (room.started) {
            socket.emit('game-sync', buildSnapshot(room));
        }

        if (typeof ack === 'function') ack({ ok: true, playerCount: room.players.length });
    });

    // ---- Host starts the game ----
    // (Kept for completeness/back-compat; the current front-end triggers the
    // start via a 'game-sync' START_GAME_DIRECT event, handled below, but a
    // client could also call this directly.)
    socket.on('start-game', ({ roomCode, players }) => {
        const room = rooms[roomCode];
        if (!room) return;
        if (Array.isArray(players) && players.length > 0) room.players = players;
        room.started = true;
        room.phase = 'ordering';
        room.state.players = room.players;
        io.to(roomCode).emit('game-start', { players: room.players });
    });

    // ---- In-game sync: relay every game event to everyone else in the room ----
    // This also updates the server's cached "last known state" for the room,
    // so late joiners / reconnecting clients can be caught up (see join-room
    // above and request-sync below). This does NOT referee the gameplay -
    // whatever the sending client says happened is trusted - but it makes
    // sure everyone (including screens that connect after the fact) ends up
    // looking at the same authoritative picture instead of a locally-diverged one.
    socket.on('game-sync', ({ roomCode, ...data }) => {
        if (!roomCode) return;
        const room = rooms[roomCode];
        if (room) {
            if (data.type === 'START_GAME_DIRECT') {
                room.started = true;
                room.phase = 'ordering';
            }
            if (data.playersState) room.state.players = data.playersState;
            if (data.currentTurnIndex !== undefined) room.state.currentTurnIndex = data.currentTurnIndex;
            if (data.type) room.state.lastEventType = data.type;
            room.state.lastEventData = data;

            if (data.type === 'ORDER_DECIDED' && Array.isArray(data.order)) {
                room.state.order = data.order;
                room.phase = 'ordering';
            } else if (data.type === 'SPIN_ORDER_COMPLETE') {
                room.phase = 'playing';
            } else if (data.type === 'GAME_OVER' || data.type === 'WINNER') {
                room.phase = 'finished';
            } else if (data.type === 'GLOBAL_RESTART') {
                room.phase = 'lobby';
                room.started = false;
                room.state = { players: null, currentTurnIndex: 0, order: null, lastEventType: null, lastEventData: null };
            }
        }
        socket.to(roomCode).emit('game-sync', data);
    });

    // ---- Explicit resync request ----
    // A client asks for this when it suspects it missed something: right
    // after connecting, after a reconnect, or if a key broadcast (like the
    // host's turn-order decision) doesn't arrive within a short timeout.
    socket.on('request-sync', ({ roomCode }, ack) => {
        const room = rooms[roomCode];
        if (!room) {
            if (typeof ack === 'function') ack(null);
            return;
        }
        const snapshot = buildSnapshot(room);
        if (typeof ack === 'function') {
            ack(snapshot);
        } else {
            socket.emit('game-sync', snapshot);
        }
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
