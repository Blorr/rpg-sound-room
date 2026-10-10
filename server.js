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

const blankChannel = () => ({ tracks: [], currentIndex: -1, playing: false, position: 0, updatedAt: Date.now(), volume: 100 });
const rooms = new Map();

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
    socket.emit("state", room);
    
    // FIX: Send nowPlaying for any tracks that ARE ACTUALLY PLAYING
    ["A", "B"].forEach(chId => {
      const ch = chId === "A" ? room.channelA : room.channelB;
      if (ch.currentIndex >= 0 && ch.tracks[ch.currentIndex]) {
        socket.emit("nowPlaying", {
          channel: chId,
          ...ch.tracks[ch.currentIndex],
          playing: ch.playing,  // IMPORTANT: send actual playing state
          volume: ch.volume
        });
        console.log(`📬 Sent nowPlaying to new client for channel ${chId}, playing: ${ch.playing}`);
      }
    });
    
    console.log(`🔗 Client joined room: ${roomId}`);
  });

  socket.on("dj", ({ token, action, payload }) => {
    const room = rooms.get(roomId);
    if (!room) return console.log("❌ Room not found for DJ action");
    if (token !== room.djToken) return console.log("❌ Invalid DJ token");
    console.log(`🎛️ DJ action: ${action} on channel ${payload?.channel}`);
    handleAction(action, payload, room, roomId);
  });

  socket.on("trackEnded", (channel) => {
    const room = rooms.get(roomId);
    if (!room) return;
    
    const ch = room[channel];
    const now = Date.now();
    
    if (now - ch.updatedAt < 3000 || ch.currentIndex < 0) return;
    
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
      console.log(`🔄 Auto-advance to track ${ch.currentIndex + 1} on channel ${channel}`);
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
    console.log(`🔄 Forced state sync to ${io.sockets.adapter.rooms.get(roomId)?.size} clients`);
  });

  socket.on("disconnect", () => {
    if (!roomId) return;
    const stillHere = io.sockets.adapter.rooms.get(roomId)?.size ?? 0;
    if (stillHere === 0) setTimeout(() => {
      if ((io.sockets.adapter_rooms.get(roomId)?.size ?? 0) === 0) {
        rooms.delete(roomId);
        console.log(`🗑️ Deleted empty room: ${roomId}`);
      }
    }, 10 * 60 * 1000);
  });
});

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
      io.to(roomId).emit("nowPlaying", { 
        channel: p.channel, 
        ...ch.tracks[p.index], 
        playing: true, 
        volume: ch.volume 
      });
      console.log(`▶️ Playing track ${p.index} on channel ${p.channel}`);
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
      if (ch.currentIndex < 0) {
        if (ch.tracks.length === 0) return;
        ch.currentIndex = 0;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        io.to(roomId).emit("nowPlaying", { 
          channel: p.channel, 
          ...ch.tracks[ch.currentIndex], 
          playing: true, 
          volume: ch.volume 
        });
        break;
      }
      ch.playing = !ch.playing;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("playbackState", { 
        channel: p.channel, 
        playing: ch.playing 
      });
      break;
      
    case "stop":
      // FIX: Set playing=false WITHOUT clearing currentIndex
      ch.playing = false;
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
  
  io.to(roomId).emit("playlistUpdate", { 
    channelA: room.channelA, 
    channelB: room.channelB 
  });
}

server.listen(process.env.PORT || 3000, () => {
  console.log(`🎲 RPG Sound Room running on http://localhost:${process.env.PORT || 3000}`);
});