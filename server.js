const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");
const axios = require("axios");

const app = express();
app.use(express.json());
app.use(express.static("public"));
const server = http.createServer(app);
const io = new Server(server);

// Initialize empty channel
const blankChannel = () => ({ tracks: [], currentIndex: -1, playing: false, position: 0, updatedAt: Date.now(), volume: 100 });

// Store rooms: { roomId -> { djToken, channelA, channelB, crossfade } }
const rooms = new Map();

// === CREATE ROOM (called by future DJ) ===
app.post("/api/rooms", (req, res) => {
  const wanted = (req.body.roomId || "").trim().replace(/[^a-z0-9-]/gi, "").slice(0, 32);
  const id = wanted || "room-" + crypto.randomBytes(3).toString("hex");
  
  if (rooms.has(id)) return res.status(409).json({ error: "That room name is taken." });
  
  const djToken = crypto.randomBytes(24).toString("hex");
  rooms.set(id, { 
    djToken, 
    channelA: blankChannel(), 
    channelB: blankChannel(), 
    crossfade: 3 
  });
  
  console.log(`📦 Created room: ${id}`);
  res.json({ roomId: id, djToken });
});

// === PROXY AUDIO FILES (fixes CORS for Tabletop Audio & other sites) ===
app.get("/proxy/audio", async (req, res) => {
  const url = req.query.url;
  
  if (!url || !/^https?:\/\/.*\.(mp3|ogg|wav|m4a)(\?|$)/i.test(url)) {
    return res.status(400).send("Invalid audio URL");
  }
  
  try {
    const response = await axios({
      method: "GET",
      url,
      responseType: "stream",
      timeout: 10000
    });
    
    res.set("Content-Type", response.headers["content-type"]);
    res.set("Access-Control-Allow-Origin", "*");
    response.data.pipe(res);
  } catch (err) {
    console.error("Proxy error:", err.message);
    res.status(500).send("Failed to proxy audio");
  }
});

// === SOCKET.IO CONNECTIONS ===
io.on("connection", (socket) => {
  let roomId = null;

  // Join a room
  socket.on("join", (id) => {
    if (!rooms.has(id)) {
      console.log(`❌ Room ${id} not found!`);
      return socket.emit("roomNotFound");
    }
    roomId = id;
    socket.join(roomId);
    const room = rooms.get(roomId);
    socket.emit("state", room); // Send initial state immediately
    console.log(`🔗 Client joined room: ${roomId}, total: ${io.sockets.adapter.rooms.get(roomId)?.size}`);
    console.log(`📊 Room has ${room.channelA.tracks.length + room.channelB.tracks.length} tracks`);
  });

  // DJ-authenticated actions
  socket.on("dj", ({ token, action, payload }) => {
    const room = rooms.get(roomId);
    if (!room) return console.log("❌ Room not found for DJ action");
    if (token !== room.djToken) return console.log("❌ Invalid DJ token");
    console.log(`🎛️ DJ action: ${action} on channel ${payload?.channel}`);
    handleAction(action, payload, room, roomId);
  });

  // Track ended signal from client (for auto-advance)
  socket.on("trackEnded", (channel) => {
    const room = rooms.get(roomId);
    if (!room) return;
    
    const ch = room[channel];
    const now = Date.now();
    
    // Guard: don't auto-advance if track just started
    if (now - ch.updatedAt < 3000 || ch.currentIndex < 0) return;
    
    // Move to next track
    if (ch.currentIndex < ch.tracks.length - 1) {
      ch.currentIndex++;
      ch.playing = true;
      ch.position = 0;
      ch.updatedAt = now;
      console.log(`🔄 Auto-advance to track ${ch.currentIndex + 1} on channel ${channel}`);
      io.to(roomId).emit("nowPlaying", { 
        channel, 
        ...ch.tracks[ch.currentIndex], 
        playing: true, 
        volume: ch.volume 
      });
    }
  });

  // Force sync state (debugging helper)
  socket.on("sync", ({ token }) => {
    const room = rooms.get(roomId);
    if (!room || token !== room.djToken) return;
    
    ["A", "B"].forEach(chId => {
      const ch = chId === "A" ? room.channelA : room.channelB;
      if (ch.currentIndex >= 0 && ch.tracks[ch.currentIndex]) {
        io.to(roomId).emit("nowPlaying", { 
          channel: chId, 
          ...ch.tracks[ch.currentIndex], 
          playing: ch.playing, 
          volume: ch.volume 
        });
      }
    });
    console.log(`🔄 Forced state sync to ${io.sockets.adapter.rooms.get(roomId)?.size} clients`);
  });

  // Clean up empty rooms after 10 minutes
  socket.on("disconnect", () => {
    if (!roomId) return;
    const stillHere = io.sockets.adapter.rooms.get(roomId)?.size ?? 0;
    if (stillHere === 0) setTimeout(() => {
      if ((io.sockets.adapter.rooms.get(roomId)?.size ?? 0) === 0) {
        rooms.delete(roomId);
        console.log(`🗑️ Deleted empty room: ${roomId}`);
      }
    }, 10 * 60 * 1000);
  });
});

// === HANDLE DJ ACTIONS ===
function handleAction(action, p, room, roomId) {
  if (!p || !p.channel) {
    console.log("⚠️ Missing payload/channel in DJ action");
    return;
  }
  const ch = p.channel === "A" ? room.channelA : room.channelB;
  
  switch (action) {
    case "queue":
      ch.tracks.push({ type: p.type, src: p.src, title: p.title });
      console.log(`➕ Queued "${p.title}" to channel ${p.channel}`);
      break;
      
    case "playTrack":
      if (p.index < 0 || p.index >= ch.tracks.length) {
        console.log(`⚠️ Invalid track index: ${p.index}`);
        return;
      }
      ch.currentIndex = p.index;
      ch.playing = true;
      ch.position = 0;
      ch.updatedAt = Date.now();
      console.log(`▶️ Playing track ${p.index} on channel ${p.channel}`);
      io.to(roomId).emit("nowPlaying", { 
        channel: p.channel, 
        ...ch.tracks[p.index], 
        playing: true, 
        volume: ch.volume 
      });
      break;
      
    case "prev":
      console.log(`⏮ Prev on channel ${p.channel}, currentIndex: ${ch.currentIndex}`);
      if (ch.currentIndex > 0) {
        ch.currentIndex--;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        io.to(roomId).emit("nowPlaying", { 
          channel: p.channel, 
          ...ch.tracks[ch.currentIndex], 
          playing: true, 
          volume: ch.volume 
        });
      } else {
        console.log(`  └─ Already at first track (or no track selected)`);
      }
      break;
      
    case "next":
      console.log(`⏭ Next on channel ${p.channel}, currentIndex: ${ch.currentIndex}`);
      if (ch.currentIndex >= 0 && ch.currentIndex < ch.tracks.length - 1) {
        ch.currentIndex++;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        io.to(roomId).emit("nowPlaying", { 
          channel: p.channel, 
          ...ch.tracks[ch.currentIndex], 
          playing: true, 
          volume: ch.volume 
        });
      } else {
        console.log(`  └─ Already at last track (tracks: ${ch.tracks.length})`);
      }
      break;
      
    case "removeTrack":
      ch.tracks.splice(p.index, 1);
      if (ch.currentIndex >= ch.tracks.length) ch.currentIndex = ch.tracks.length - 1;
      if (ch.currentIndex < 0) ch.currentIndex = -1;
      if (ch.currentIndex === p.index) {
        ch.playing = false;
        io.to(roomId).emit("nowPlaying", { 
          channel: p.channel, 
          src: null, 
          title: null, 
          playing: false 
        });
      }
      console.log(`🗑️ Removed track ${p.index} on channel ${p.channel}`);
      break;
      
    case "toggle":
      // If no track is selected, start from the beginning
      if (ch.currentIndex < 0) {
        if (ch.tracks.length === 0) {
          console.log(`⚠️ No tracks to play on channel ${p.channel}`);
          return;
        }
        ch.currentIndex = 0;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        console.log(`▶️ Auto-started first track on channel ${p.channel}`);
        io.to(roomId).emit("nowPlaying", { 
          channel: p.channel, 
          ...ch.tracks[ch.currentIndex], 
          playing: true, 
          volume: ch.volume 
        });
        break;
      }
      // Otherwise toggle current playback
      ch.playing = !ch.playing;
      ch.updatedAt = Date.now();
      console.log(`⏯ Toggled playback on channel ${p.channel}: ${ch.playing}`);
      io.to(roomId).emit("playbackState", { 
        channel: p.channel, 
        playing: ch.playing 
      });
      break;
      
    case "stop":
      // Keep tracks, only stop playback!
      ch.playing = false;
      ch.currentIndex = -1;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("nowPlaying", { 
        channel: p.channel, 
        src: null, 
        title: null, 
        playing: false 
      });
      console.log(`⏹ Stopped channel ${p.channel}, tracks preserved: ${ch.tracks.length}`);
      break;
      
    case "volume":
      ch.volume = Math.max(0, Math.min(100, p.volume));
      io.to(roomId).emit("volumeChange", { 
        channel: p.channel, 
        volume: ch.volume 
      });
      break;
      
    case "crossfade":
      room.crossfade = p.seconds;
      io.to(roomId).emit("crossfadeUpdate", p.seconds);
      break;
  }
  
  // Broadcast updated playlist to all clients in room
  io.to(roomId).emit("playlistUpdate", { 
    channelA: room.channelA, 
    channelB: room.channelB 
  });
}

// === START SERVER ===
server.listen(process.env.PORT || 3000, () => {
  console.log(`🎲 RPG Sound Room running on http://localhost:${process.env.PORT || 3000}`);
});