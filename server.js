require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.static('public', {
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    }
  }
}));

// Database
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Init DB
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS profiles (
        id SERIAL PRIMARY KEY,
        handle VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        report JSONB NOT NULL
      )
    `);
    console.log('✅ Database ready');
  } catch (err) {
    console.error('DB init error:', err.message);
  }
}
initDB();

// ── POST /api/analyze ─────────────────────────────
// Uses SSE streaming to avoid Railway's 60-second HTTP timeout
app.post('/api/analyze', async (req, res) => {
  const { images, prompt } = req.body;
  if (!images || !prompt) return res.status(400).json({ error: 'Missing images or prompt' });

  const limited = images.slice(0, 5);
  const totalKB = Math.round(limited.reduce((s, img) => s + img.data.length * 0.75, 0) / 1024);
  console.log(`Analyze: ${limited.length} images, ~${totalKB}KB total`);

  const content = [];
  for (const img of limited) {
    content.push({
      type: 'image',
      source: { type: 'base64', media_type: img.mediaType || 'image/jpeg', data: img.data }
    });
  }
  content.push({ type: 'text', text: prompt });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  // Keep connection alive while waiting for Anthropic to start responding
  const heartbeat = setInterval(() => {
    if (!res.writableEnded) res.write(': keepalive\n\n');
  }, 15000);

  try {
    const apiRes = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 4000,
        stream: true,
        system: '你是一位人格分析專家。請只回傳純 JSON，直接從 { 開始，到 } 結束。不可有任何 markdown、程式碼區塊、或 JSON 以外的文字。',
        messages: [{ role: 'user', content }]
      })
    });

    if (!apiRes.ok) {
      clearInterval(heartbeat);
      const errData = await apiRes.json().catch(() => ({}));
      res.write(`data: ${JSON.stringify({ error: errData.error?.message || `HTTP ${apiRes.status}` })}\n\n`);
      res.end();
      return;
    }

    let fullText = '';
    let lineBuffer = '';

    apiRes.body.on('data', chunk => {
      lineBuffer += chunk.toString();
      const lines = lineBuffer.split('\n');
      lineBuffer = lines.pop();

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const raw = line.slice(6).trim();
        if (!raw || raw === '[DONE]') continue;
        try {
          const evt = JSON.parse(raw);
          if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
            fullText += evt.delta.text;
          } else if (evt.type === 'message_stop') {
            clearInterval(heartbeat);
            let result;
            try {
              result = JSON.parse(fullText.trim());
            } catch (_) {
              const start = fullText.indexOf('{');
              const end = fullText.lastIndexOf('}');
              if (start === -1 || end === -1) {
                res.write(`data: ${JSON.stringify({ error: '回傳格式錯誤，請重試' })}\n\n`);
                res.end();
                return;
              }
              try {
                result = JSON.parse(fullText.substring(start, end + 1));
              } catch (e) {
                res.write(`data: ${JSON.stringify({ error: '回傳格式錯誤，請重試' })}\n\n`);
                res.end();
                return;
              }
            }
            res.write(`data: ${JSON.stringify({ success: true, result })}\n\n`);
            res.end();
          }
        } catch (_) {}
      }
    });

    apiRes.body.on('error', err => {
      clearInterval(heartbeat);
      console.error('Stream error:', err.message);
      if (!res.writableEnded) {
        res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
        res.end();
      }
    });

    apiRes.body.on('end', () => {
      clearInterval(heartbeat);
      if (!res.writableEnded) res.end();
    });

  } catch (err) {
    clearInterval(heartbeat);
    console.error('Analyze error:', err.message);
    if (!res.writableEnded) {
      res.write(`data: ${JSON.stringify({ error: err.message })}\n\n`);
      res.end();
    }
  }
});

// ── POST /api/profiles ────────────────────────────
app.post('/api/profiles', async (req, res) => {
  try {
    const { handle, report } = req.body;
    if (!handle || !report) return res.status(400).json({ error: 'Missing handle or report' });
    // Upsert: remove duplicate then insert
    await pool.query('DELETE FROM profiles WHERE handle = $1', [handle]);
    const result = await pool.query(
      'INSERT INTO profiles (handle, report) VALUES ($1, $2) RETURNING id, created_at',
      [handle, JSON.stringify(report)]
    );
    res.json({ success: true, id: result.rows[0].id, created_at: result.rows[0].created_at });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/profiles ─────────────────────────────
app.get('/api/profiles', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT id, handle, created_at, report FROM profiles ORDER BY created_at DESC LIMIT 100'
    );
    res.json({ success: true, profiles: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── DELETE /api/profiles/:id ──────────────────────
app.delete('/api/profiles/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM profiles WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Serve frontend
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, () => {
  console.log(`🚀 Observer running on port ${port}`);
});
