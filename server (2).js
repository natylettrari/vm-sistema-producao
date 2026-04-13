const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.json());

const SHOP = process.env.SHOP_DOMAIN || 'ipttcr-gi.myshopify.com';
const API_KEY = process.env.SHOPIFY_API_KEY;
const API_SECRET = process.env.SHOPIFY_API_SECRET;
const APP_URL = process.env.APP_URL || 'https://producao.vilmamirian.com';
const SCOPES = 'read_orders,read_draft_orders,read_products';
const TOKEN_FILE = '/tmp/shopify_token.json';

function saveToken(token) {
  fs.writeFileSync(TOKEN_FILE, JSON.stringify({ token, savedAt: new Date().toISOString() }));
}

function loadToken() {
  try {
    if (process.env.SHOPIFY_TOKEN) return process.env.SHOPIFY_TOKEN;
    if (fs.existsSync(TOKEN_FILE)) {
      return JSON.parse(fs.readFileSync(TOKEN_FILE)).token;
    }
  } catch(e) {}
  return null;
}

function headers(token) {
  return { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' };
}

// OAuth
app.get('/auth', (req, res) => {
  const state = crypto.randomBytes(16).toString('hex');
  const redirectUri = `${APP_URL}/auth/callback`;
  res.redirect(`https://${SHOP}/admin/oauth/authorize?client_id=${API_KEY}&scope=${SCOPES}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${state}`);
});

app.get('/auth/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).send('Código não recebido');
  try {
    const resp = await fetch(`https://${SHOP}/admin/oauth/access_token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: API_KEY, client_secret: API_SECRET, code })
    });
    const data = await resp.json();
    if (data.access_token) { saveToken(data.access_token); res.redirect('/'); }
    else res.status(400).send('Erro: ' + JSON.stringify(data));
  } catch(e) { res.status(500).send('Erro: ' + e.message); }
});

// ── Parser de observações atualizado ─────────────────────────────────────────
function parseObs(note) {
  if (!note) return { vendedora:null, dataEnvio:null, bordado:null, nomeAlca:null, obsCliente:null };

  const lines = note.split('\n').map(l => l.trim()).filter(Boolean);
  let vendedora = null, dataEnvio = null, bordado = null, nomeAlca = null;
  const extras = [];

  for (const line of lines) {
    // Padrão: CONSULTORA LAURA - ENVIO 20/05
    const m1 = line.match(/^([A-ZÀ-Úa-zà-ú\s]+?)\s*[-–]\s*ENVIO\s+(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/i);
    if (m1) { vendedora = m1[1].trim(); dataEnvio = m1[2].trim(); continue; }

    // Padrão: BORDADO XXXX
    const m2 = line.match(/^BORDADO\s+(.+)/i);
    if (m2) { bordado = m2[1].trim(); continue; }

    // Padrão: NOME ALÇA: Maitê ou -NOME ALÇA: Maitê
    const m3 = line.match(/(?:^[-\s]*)?NOME\s+AL[CÇ]A[:\s]+(.+)/i);
    if (m3) { nomeAlca = m3[1].trim(); bordado = nomeAlca; continue; }

    // Padrão: NOME: Maitê
    const m4 = line.match(/^NOME[:\s]+(.+)/i);
    if (m4) { bordado = m4[1].trim(); continue; }

    // Padrão: - Bolsa Madison: -NOME ALÇA: Maitê (linha com produto e nome)
    const m5 = line.match(/NOME\s+AL[CÇ]A[:\s]+(.+)/i);
    if (m5) { nomeAlca = m5[1].trim(); bordado = nomeAlca; continue; }

    // COLEÇÃO XXXX - não é obs da cliente
    if (line.match(/^COLE[ÇC][ÃA]O\s+/i)) continue;

    // Resto é obs da cliente
    if (line.length > 2) extras.push(line);
  }

  return { vendedora, dataEnvio, bordado, nomeAlca, obsCliente: extras.join(' | ') || null };
}

// ── Extração de modelo base ───────────────────────────────────────────────────
const MODELOS_BASE = [
  'Madison Mini', 'Madison', 'Mala de Rodinhas', 'Mala Rodinhas',
  'Madeleine', 'Mochila 2 em 1', 'Mochila Mummy', 'Louise Mini', 'Louise',
  'Bolsa Cleo', 'Cleo', 'Bolsa Cloé', 'Cloé', 'Cloe',
  'Bolsa Liz', 'Liz', 'Bolsa Kate', 'Kate',
  'Frasqueira', 'Mochila', 'Kit Cristal', 'Necessaire', 'Trocador',
  'Alça', 'Pingente', 'Organizador'
];

function extrairModeloBase(title) {
  if (!title) return 'Outros';
  const t = title.toLowerCase();
  for (const m of MODELOS_BASE) {
    if (t.includes(m.toLowerCase())) return m;
  }
  // Tenta pegar o primeiro substantivo significativo
  return title.split(' ').slice(0, 3).join(' ');
}

function extrairColecaoCor(title, variant) {
  if (!title) return '';
  if (variant && variant !== 'Default Title') return variant;
  const cols = ['Ella','Urban Chic','Nós','Origem','Le Petit','Tressê Palha'];
  const cores = ['Café','Caramelo','Off White','Marinho','Bordô','Cinza','Bege','Preto','Rosé','Marrom','Verde','Nude','Vinho','Rosa','Preta','Azul'];
  let c = '', r = '';
  for (const x of cols) { if (title.toLowerCase().includes(x.toLowerCase())) { c = x; break; } }
  for (const x of cores) { if (title.toLowerCase().includes(x.toLowerCase())) { r = x; break; } }
  return [c, r].filter(Boolean).join(' · ') || title;
}

function fmtDate(d) {
  if (!d) return null;
  return new Date(d).toLocaleDateString('pt-BR', { day:'2-digit', month:'2-digit', year:'numeric' });
}

function calcStatus(tags, dataEnvio, note) {
  // Verifica pronta entrega nas tags ou observações
  const t = (tags || '').toLowerCase();
  const n = (note || '').toLowerCase();
  if (t.includes('pronta-entrega') || t.includes('pronta entrega') || n.includes('pronta entrega')) return 'pronta_entrega';
  if (t.includes('pronto') || t.includes('enviado')) return 'pronto';
  if (t.includes('producao') || t.includes('produção') || t.includes('em-producao')) return 'em_producao';

  if (dataEnvio && dataEnvio !== '—') {
    const p = dataEnvio.split('/');
    if (p.length >= 2) {
      const y = p[2] ? (p[2].length === 2 ? '20'+p[2] : p[2]) : new Date().getFullYear();
      const d = new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
      if (!isNaN(d) && d < new Date()) return 'atrasado';
    }
  }
  return 'aguardando';
}

function mapItem(order, item, isDraft) {
  const obs = parseObs(order.note);
  return {
    id: isDraft ? 'D-'+order.id : order.id,
    numero: isDraft ? '#D-'+String(order.id).slice(-4) : '#'+order.order_number,
    dataPedido: fmtDate(order.created_at),
    dataEnvio: obs.dataEnvio || '—',
    vendedora: obs.vendedora || '—',
    modeloBase: extrairModeloBase(item.title),
    modelo: item.title,
    colecaoCor: extrairColecaoCor(item.title, item.variant_title),
    bordado: obs.bordado || null,
    nomeAlca: obs.nomeAlca || null,
    obsCliente: obs.obsCliente || '—',
    status: calcStatus(order.tags, obs.dataEnvio, order.note),
    isDraft, tags: order.tags || '',
    quantidade: item.quantity || 1,
    noteRaw: order.note || ''
  };
}

// ── APIs ──────────────────────────────────────────────────────────────────────
app.get('/api/ping', (req, res) => {
  const token = loadToken();
  res.json({ ok: true, shop: SHOP, hasToken: !!token, authUrl: token ? null : `${APP_URL}/auth` });
});

app.get('/api/pedidos', async (req, res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({ error: 'Não autorizado', authUrl: `${APP_URL}/auth` });
  try {
    const { data_de, data_ate, filtro_data_tipo } = req.query;
    let dp = '';
    if (data_de && filtro_data_tipo !== 'pedido') dp += `&created_at_min=${new Date(data_de).toISOString()}`;
    if (data_ate && filtro_data_tipo !== 'pedido') dp += `&created_at_max=${new Date(data_ate+'T23:59:59').toISOString()}`;

    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250${dp}`, { headers: headers(token) });
    if (!r.ok) { const t = await r.text(); return res.status(r.status).json({ error: `Shopify ${r.status}: ${t}` }); }
    const d = await r.json();
    let lista = (d.orders || []).flatMap(o => o.line_items.map(i => mapItem(o, i, false)));

    try {
      const dr = await fetch(`https://${SHOP}/admin/api/2024-01/draft_orders.json?status=open&limit=250`, { headers: headers(token) });
      if (dr.ok) {
        const dd = await dr.json();
        lista = lista.concat((dd.draft_orders || []).flatMap(o => o.line_items.map(i => mapItem(o, i, true))));
      }
    } catch(e) {}

    // Filtro por data do pedido
    if (filtro_data_tipo === 'pedido' && data_de) {
      lista = lista.filter(p => {
        if (!p.dataPedido) return false;
        const parts = p.dataPedido.split('/');
        const d = new Date(`${parts[2]}-${parts[1]}-${parts[0]}`);
        if (data_de && d < new Date(data_de)) return false;
        if (data_ate && d > new Date(data_ate+'T23:59:59')) return false;
        return true;
      });
    }

    res.json({ pedidos: lista, total: lista.length });
  } catch(e) { console.error(e); res.status(500).json({ error: e.message }); }
});

app.get('/api/modelos', async (req, res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({ error: 'Não autorizado' });
  try {
    const r = await fetch(`https://${SHOP}/admin/api/2024-01/products.json?fields=title&limit=250`, { headers: headers(token) });
    const d = await r.json();
    // Retorna modelos base únicos
    const modelos = [...new Set((d.products || []).map(p => extrairModeloBase(p.title)))].sort();
    res.json({ modelos });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/vendedoras', async (req, res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({ error: 'Não autorizado' });
  try {
    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250&fields=note`, { headers: headers(token) });
    const d = await r.json();
    const v = new Set();
    for (const o of d.orders || []) {
      const obs = parseObs(o.note);
      if (obs.vendedora) v.add(obs.vendedora);
    }
    res.json({ vendedoras: [...v].sort() });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/pedido/:id/status', async (req, res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({ error: 'Não autorizado' });
  try {
    const { id } = req.params;
    const { novaTag } = req.body;
    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders/${id}.json`, {
      method: 'PUT', headers: headers(token),
      body: JSON.stringify({ order: { id, tags: novaTag } })
    });
    res.json(await r.json());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Serve frontend
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.static(__dirname));
app.get('*', (req, res) => {
  const p1 = path.join(__dirname, 'public', 'index.html');
  const p2 = path.join(__dirname, 'index.html');
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  res.status(404).send('Carregando...');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, '0.0.0.0', () => console.log(`VM Sistema porta ${PORT}`));
