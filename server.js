require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fetch = require('node-fetch');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json({ limit: '100mb' }));
app.use(express.static('public'));

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
app.post('/api/analyze', async (req, res) => {
  try {
    const { images, prompt } = req.body;
    if (!images || !prompt) {
      return res.status(400).json({ error: 'Missing images or prompt' });
    }

    const content = [];

    // Add images (max 20)
    for (const img of images.slice(0, 20)) {
      content.push({
        type: 'image',
        source: {
          type: 'base64',
          media_type: img.mediaType || 'image/jpeg',
          data: img.data
        }
      });
    }

    content.push({ type: 'text', text: prompt });

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-6',
        max_tokens: 4000,
        system: '你是一位人格分析專家。請只回傳純 JSON，直接從 { 開始，到 } 結束。不可有任何 markdown、程式碼區塊、或 JSON 以外的文字。',
        messages: [{ role: 'user', content }]
      })
    });

    if (!response.ok) {
      const errData = await response.json().catch(() => ({}));
      throw new Error(errData.error?.message || `HTTP ${response.status}`);
    }

    const data = await response.json();
    const fullText = (data.content || [])
      .filter(c => c.type === 'text')
      .map(c => c.text)
      .join('');

    // Robust JSON extraction: try direct parse first, then extract by braces
    let result;
    try {
      result = JSON.parse(fullText.trim());
    } catch (_) {
      const start = fullText.indexOf('{');
      const end = fullText.lastIndexOf('}');
      if (start === -1 || end === -1) throw new Error('回傳格式錯誤，請重試');
      result = JSON.parse(fullText.substring(start, end + 1));
    }

    res.json({ success: true, result });

  } catch (err) {
    console.error('Analyze error:', err.message);
    res.status(500).json({ error: err.message });
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
