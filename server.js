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

const blankChannel = () => ({ 
  tracks: [], 
  currentIndex: -1, 
  playing: false, 
  position: 0, 
  updatedAt: Date.now(), 
  volume: 100,
  repeat: false
});

const rooms = new Map();
const roomActivity = new Map();

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
  roomActivity.set(id, Date.now());
  
  console.log(`📦 Created room: ${id}`);
  res.json({ roomId: id, djToken });
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", rooms: rooms.size, timestamp: Date.now() });
});

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

io.on("connection", (socket) => {
  let roomId = null;

  socket.on("join", (id) => {
    if (!rooms.has(id)) {
      console.log(`❌ Room ${id} not found!`);
      return socket.emit("roomNotFound");
    }
    roomId = id;
    socket.join(roomId);
    const room = rooms.get(roomId);
    roomActivity.set(id, Date.now());
    socket.emit("state", room);
    
    ["A", "B"].forEach(chId => {
      const ch = chId === "A" ? room.channelA : room.channelB;
      if (ch.currentIndex >= 0 && ch.tracks[ch.currentIndex]) {
        socket.emit("nowPlaying", {
          channel: chId,
          ...ch.tracks[ch.currentIndex],
          playing: ch.playing,
          volume: ch.volume
        });
      }
    });
    
    console.log(`🔗 Client joined room: ${roomId} (total: ${io.sockets.adapter.rooms.get(roomId)?.size})`);
  });

  socket.on("ping", () => {
    socket.emit("pong");
    if (roomId) roomActivity.set(roomId, Date.now());
  });

  socket.on("dj", ({ token, action, payload }) => {
    const room = rooms.get(roomId);
    if (!room) return console.log("❌ Room not found for DJ action");
    if (token !== room.djToken) return console.log("❌ Invalid DJ token");
    if (roomId) roomActivity.set(roomId, Date.now());
    handleAction(action, payload, room, roomId);
  });

  socket.on("trackEnded", (channel) => {
    const room = rooms.get(roomId);
    if (!room) return;
    
    const ch = channel === "A" ? room.channelA : room.channelB;
    const now = Date.now();
    
    if (now - ch.updatedAt < 3000 || ch.currentIndex < 0) return;
    
    if (ch.repeat) {
      ch.currentIndex = ch.currentIndex;
      ch.playing = true;
      ch.position = 0;
      ch.updatedAt = now;
      io.to(roomId).emit("nowPlaying", { 
        channel, 
        ...ch.tracks[ch.currentIndex], 
        playing: true, 
        volume: ch.volume 
      });
    } else if (ch.currentIndex < ch.tracks.length - 1) {
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
  });

  socket.on("disconnect", (reason) => {
    console.log(`❌ Client disconnected from ${roomId}: ${reason}`);
    if (!roomId) return;
    
    const stillHere = io.sockets.adapter.rooms.get(roomId)?.size ?? 0;
    console.log(`📊 Remaining clients in ${roomId}: ${stillHere}`);
    
    if (stillHere === 0) {
      setTimeout(() => {
        const count = io.sockets.adapter.rooms.get(roomId)?.size ?? 0;
        if (count === 0) {
          rooms.delete(roomId);
          roomActivity.delete(roomId);
          console.log(`🗑️ Deleted empty room: ${roomId}`);
        }
      }, 10 * 60 * 1000);
    }
  });
});

setInterval(() => {
  if (rooms.size > 0) {
    console.log(`📊 Active rooms: ${rooms.size}, Activity timestamps:`, [...roomActivity.entries()].map(([r, t]) => `${r}:${t}`).join(", "));
  }
}, 60000);

server.listen(process.env.PORT || 3000, () => {
  console.log(`🎲 RPG Sound Room running on http://localhost:${process.env.PORT || 3000}`);
});