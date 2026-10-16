const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");
const axios = require("axios");
const multer = require("multer");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.json());
app.use(express.static("public"));
const server = http.createServer(app);
const io = new Server(server);

// === UPLOAD STORAGE ===
const UPLOAD_DIR = path.join(__dirname, "uploads");
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const upload = multer({
  storage: multer.diskStorage({
    destination: (req, file, cb) => cb(null, UPLOAD_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname).toLowerCase();
      cb(null, "aud-" + crypto.randomBytes(8).toString("hex") + ext);
    }
  }),
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = [".mp3", ".ogg", ".wav", ".m4a"];
    if (ok.includes(path.extname(file.originalname).toLowerCase())) cb(null, true);
    else cb(new Error("Only MP3, OGG, WAV, M4A files allowed"));
  }
});

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

// Normalizes any room name to safe lowercase ID
function normalizeRoomId(raw) {
  return (raw || "").trim().replace(/[^a-zA-Z0-9-]/g, "").toLowerCase().slice(0, 32);
}

app.post("/api/rooms", (req, res) => {
  const wanted = normalizeRoomId(req.body.roomId);
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

// === FILE UPLOAD ENDPOINT (DJ only) ===
app.post("/api/upload", (req, res) => {
  const token = req.headers["x-dj-token"];

  upload.single("audio")(req, res, (err) => {
    if (err) {
      console.error("❌ Upload error:", err.message);
      return res.status(400).json({ error: err.message });
    }
    if (!req.file) return res.status(400).json({ error: "No file received" });

    let validRoom = false;
    for (const [, room] of rooms) {
      if (room.djToken === token) { validRoom = true; break; }
    }
    if (token && !validRoom) {
      fs.unlinkSync(req.file.path);
      return res.status(403).json({ error: "Invalid DJ token" });
    }

    const fileUrl = `${req.protocol}://${req.get("host")}/uploads/${req.file.filename}`;
    console.log(`📤 Uploaded: ${req.file.originalname} → ${req.file.filename}`);
    res.json({ url: fileUrl, filename: req.file.originalname, size: req.file.size });
  });
});

app.use("/uploads", express.static(UPLOAD_DIR));

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
    const normalizedId = typeof id === "string" ? id.toLowerCase() : null;
    if (!normalizedId || !rooms.has(normalizedId)) {
      console.log(`❌ Room "${normalizedId}" not found!`);
      return socket.emit("roomNotFound");
    }
    roomId = normalizedId;
    socket.join(roomId);
    const room = rooms.get(roomId);
    roomActivity.set(roomId, Date.now());
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
    console.log(`📊 Active rooms: ${rooms.size}`);
  }
}, 60000);

// Cleanup uploaded files older than 6 hours
setInterval(() => {
  try {
    const cutoff = Date.now() - 6 * 60 * 60 * 1000;
    fs.readdirSync(UPLOAD_DIR).forEach(f => {
      const fp = path.join(UPLOAD_DIR, f);
      if (fs.statSync(fp).mtimeMs < cutoff) fs.unlinkSync(fp);
    });
  } catch (e) { /* ignore */ }
}, 60 * 60 * 1000);

function handleAction(action, p, room, roomId) {
  if (!p || !p.channel) return;
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
      io.to(roomId).emit("nowPlaying", { channel: p.channel, ...ch.tracks[p.index], playing: true, volume: ch.volume });
      break;
    case "prev":
      if (ch.currentIndex > 0) {
        ch.currentIndex--;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        io.to(roomId).emit("nowPlaying", { channel: p.channel, ...ch.tracks[ch.currentIndex], playing: true, volume: ch.volume });
      }
      break;
    case "next":
      if (ch.currentIndex >= 0 && ch.currentIndex < ch.tracks.length - 1) {
        ch.currentIndex++;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        io.to(roomId).emit("nowPlaying", { channel: p.channel, ...ch.tracks[ch.currentIndex], playing: true, volume: ch.volume });
      }
      break;
    case "repeat":
      ch.repeat = !ch.repeat;
      io.to(roomId).emit("repeatUpdate", { channel: p.channel, repeat: ch.repeat });
      break;
    case "removeTrack":
      ch.tracks.splice(p.index, 1);
      if (ch.currentIndex >= ch.tracks.length) ch.currentIndex = ch.tracks.length - 1;
      if (ch.currentIndex < 0) ch.currentIndex = -1;
      if (ch.currentIndex === p.index) {
        ch.playing = false;
        io.to(roomId).emit("nowPlaying", { channel: p.channel, src: null, title: null, playing: false });
      }
      break;
    case "toggle":
      if (ch.currentIndex < 0) {
        if (ch.tracks.length === 0) return;
        ch.currentIndex = 0;
        ch.playing = true;
        ch.position = 0;
        ch.updatedAt = Date.now();
        io.to(roomId).emit("nowPlaying", { channel: p.channel, ...ch.tracks[ch.currentIndex], playing: true, volume: ch.volume });
        break;
      }
      ch.playing = !ch.playing;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("playbackState", { channel: p.channel, playing: ch.playing });
      break;
    case "stop":
      ch.playing = false;
      ch.updatedAt = Date.now();
      io.to(roomId).emit("nowPlaying", { channel: p.channel, src: null, title: null, playing: false });
      break;
    case "volume":
      ch.volume = Math.max(0, Math.min(100, p.volume));
      io.to(roomId).emit("volumeChange", { channel: p.channel, volume: ch.volume });
      break;
    case "crossfade":
      room.crossfade = p.seconds;
      io.to(roomId).emit("crossfadeUpdate", p.seconds);
      break;
  }

  io.to(roomId).emit("playlistUpdate", { channelA: room.channelA, channelB: room.channelB });
}

server.listen(process.env.PORT || 3000, () => {
  console.log(`🎲 RPG Sound Room running on http://localhost:${process.env.PORT || 3000}`);
});