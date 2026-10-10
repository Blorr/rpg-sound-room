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

// Initialize empty channel (keeps tracks, clears playback state)
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
    if (!rooms.has(id)) return socket.emit("roomNotFound");
    roomId = id;
    socket.join(roomId);
    socket.emit("state", rooms.get(roomId));
  });

  // DJ-authenticated actions
  socket.on("dj", ({ token, action, payload }) => {
    const room = rooms.get(roomId);
    if (!room || token !== room.djToken) return; // silently ignore non-DJs
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
      io.to(roomId).emit("nowPlaying", { 
        channel, 
        ...ch.tracks[ch.currentIndex], 
        playing: true, 
        volume: ch.volume 
      });
    }
  });

  // Clean up empty rooms after 10 minutes
  socket.on("disconnect", () => {
    if (!roomId) return;
    const stillHere = io.sockets.adapter.rooms.get(roomId)?.size ?? 0;
    if (stillHere === 0) setTimeout(() => {
      if ((io.sockets.adapter.rooms.get(roomId)?.size ?? 0) === 0) rooms.delete(roomId);
    }, 10 * 60 * 1000);
  });
});

// === HANDLE DJ ACTIONS ===
function handleAction(action, p, room, roomId) {
  const ch = p.channel === "A" ? room.channelA : room.channelB;
  
  switch (action) {
    case "queue":
      ch.tracks.push({ type: p.type, src: p.src, title: p.title });
      break;
      
    case "playTrack":
      if (p.index < 0 || p.index >= ch.tracks.length) return;
      ch.currentIndex = p.index;
      ch.playing = true;
      ch.position = 0;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("nowPlaying", { 
        channel: p.channel, 
        ...ch.tracks[p.index], 
        playing: true, 
        volume: ch.volume 
      });
      break;
      
    case "prev":
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
      }
      break;
      
    case "next":
      if (ch.currentIndex < ch.tracks.length - 1) {
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
      break;
      
    case "toggle":
      if (ch.currentIndex < 0) return;
      ch.playing = !ch.playing;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("playbackState", { 
        channel: p.channel, 
        playing: ch.playing 
      });
      break;
      
    case "stop":
      // FIX: Only stop playback, don't delete tracks!
      ch.playing = false;
      ch.currentIndex = -1;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("nowPlaying", { 
        channel: p.channel, 
        src: null, 
        title: null, 
        playing: false 
      });
      // IMPORTANT: Keep the tracks in the playlist!
      io.to(roomId).emit("playlistUpdate", { 
        channelA: room.channelA, 
        channelB: room.channelB 
      });
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