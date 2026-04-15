const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const SHOP = process.env.SHOP_DOMAIN || 'ipttcr-gi.myshopify.com';
const API_KEY = process.env.SHOPIFY_API_KEY;
const API_SECRET = process.env.SHOPIFY_API_SECRET;
const APP_URL = process.env.APP_URL || 'https://producao.vilmamirian.com';
const SCOPES = 'read_orders,read_draft_orders,read_products';
const SENHA = process.env.APP_PASSWORD || 'vilmamirian2025';
const CAPACIDADE_DIARIA = 35;

// ── PostgreSQL ────────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS configuracoes (
      chave TEXT PRIMARY KEY,
      valor TEXT NOT NULL,
      atualizado_em TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS listas_producao (
      id TEXT PRIMARY KEY,
      numero TEXT NOT NULL,
      nome TEXT NOT NULL,
      pedido_ids JSONB DEFAULT '[]',
      data_envio TEXT,
      data_producao TEXT,
      total_pecas INTEGER DEFAULT 0,
      modelos JSONB DEFAULT '[]',
      status TEXT DEFAULT 'em_producao',
      criada_em TIMESTAMP DEFAULT NOW()
    );
  `);
  console.log('DB inicializado');
}

// Token no banco
async function saveToken(t) {
  await pool.query(`INSERT INTO configuracoes(chave,valor) VALUES('shopify_token',$1) ON CONFLICT(chave) DO UPDATE SET valor=$1, atualizado_em=NOW()`, [t]);
  console.log('Token salvo no banco');
}

async function loadToken() {
  try {
    if (process.env.SHOPIFY_TOKEN) return process.env.SHOPIFY_TOKEN;
    const r = await pool.query(`SELECT valor FROM configuracoes WHERE chave='shopify_token'`);
    return r.rows[0]?.valor || null;
  } catch(e) { return null; }
}

// Listas no banco
async function loadListas() {
  try {
    const r = await pool.query(`SELECT * FROM listas_producao ORDER BY criada_em DESC`);
    return r.rows.map(row => ({
      id: row.id, numero: row.numero, nome: row.nome,
      pedidoIds: row.pedido_ids, dataEnvio: row.data_envio,
      dataProducao: row.data_producao, totalPecas: row.total_pecas,
      modelos: row.modelos, status: row.status,
      criadaEm: row.criada_em
    }));
  } catch(e) { console.error('loadListas:', e.message); return []; }
}

async function salvarLista(lista) {
  await pool.query(`
    INSERT INTO listas_producao(id,numero,nome,pedido_ids,data_envio,data_producao,total_pecas,modelos,status)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT(id) DO UPDATE SET
      nome=$3, pedido_ids=$4, data_envio=$5, data_producao=$6,
      total_pecas=$7, modelos=$8, status=$9
  `, [lista.id, lista.numero, lista.nome, JSON.stringify(lista.pedidoIds||[]),
      lista.dataEnvio||null, lista.dataProducao||null, lista.totalPecas||0,
      JSON.stringify(lista.modelos||[]), lista.status||'em_producao']);
}

async function atualizarLista(id, dados) {
  const campos = [];
  const vals = [];
  let i = 1;
  if (dados.nome !== undefined) { campos.push(`nome=$${i++}`); vals.push(dados.nome); }
  if (dados.dataProducao !== undefined) { campos.push(`data_producao=$${i++}`); vals.push(dados.dataProducao); }
  if (dados.dataEnvio !== undefined) { campos.push(`data_envio=$${i++}`); vals.push(dados.dataEnvio); }
  if (dados.status !== undefined) { campos.push(`status=$${i++}`); vals.push(dados.status); }
  if (dados.totalPecas !== undefined) { campos.push(`total_pecas=$${i++}`); vals.push(dados.totalPecas); }
  if (!campos.length) return;
  vals.push(id);
  await pool.query(`UPDATE listas_producao SET ${campos.join(',')} WHERE id=$${i}`, vals);
}

async function deletarLista(id) {
  await pool.query(`DELETE FROM listas_producao WHERE id=$1`, [id]);
}

async function proximoNumeroLista() {
  const r = await pool.query(`SELECT numero FROM listas_producao ORDER BY criada_em DESC LIMIT 1`);
  if (!r.rows.length) return '0001';
  return String(parseInt(r.rows[0].numero||'0') + 1).padStart(4, '0');
}

// ── Sessões ───────────────────────────────────────────────────────────────────
const SESSIONS = new Set();

function gerarSession() {
  const id = crypto.randomBytes(32).toString('hex');
  SESSIONS.add(id);
  return id;
}

function validarSession(req) {
  const cookie = req.headers.cookie || '';
  const match = cookie.match(/vm_session=([a-f0-9]+)/);
  return match && SESSIONS.has(match[1]);
}

function authMiddleware(req, res, next) {
  if (req.path === '/login' || req.path === '/auth' || req.path === '/auth/callback') return next();
  if (!validarSession(req)) return res.redirect('/login');
  next();
}
app.use(authMiddleware);

// Login
app.get('/login', (req, res) => {
  res.send(`<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Vilma Mirian — Produção</title>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;1,400&family=Jost:wght@300;400;500&display=swap" rel="stylesheet">
<style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:'Jost',sans-serif;background:#fafaf8;display:flex;align-items:center;justify-content:center;min-height:100vh;color:#2a2526}.box{background:#fff;border:1px solid #e8dfe0;border-radius:16px;padding:48px 40px;width:360px;text-align:center;box-shadow:0 4px 24px rgba(138,76,82,.08)}.logo{font-family:'Playfair Display',serif;font-style:italic;font-size:28px;color:#8A4C52;margin-bottom:6px}.sub{font-size:12px;color:#9a8a8c;margin-bottom:32px;letter-spacing:.06em;text-transform:uppercase}input{width:100%;padding:12px 16px;border:1px solid #d4c8c9;border-radius:9px;font-size:14px;font-family:'Jost',sans-serif;color:#2a2526;background:#fafaf8;outline:none;transition:border .2s;margin-bottom:12px}input:focus{border-color:#8A4C52}button{width:100%;padding:12px;background:#8A4C52;color:#fff;border:none;border-radius:9px;font-size:13px;font-family:'Jost',sans-serif;letter-spacing:.08em;text-transform:uppercase;cursor:pointer;transition:background .2s}button:hover{background:#7a3d44}.err{color:#c0392b;font-size:12px;margin-bottom:12px;display:none}</style></head>
<body><div class="box"><div class="logo">Vilma Mirian</div><div class="sub">Sistema de Produção</div>
<div class="err" id="err">Senha incorreta</div>
<form onsubmit="entrar(event)"><input type="password" id="senha" placeholder="Digite a senha" autocomplete="current-password"><button type="submit">Entrar</button></form></div>
<script>async function entrar(e){e.preventDefault();const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({senha:document.getElementById('senha').value})});if(r.ok)window.location.href='/';else document.getElementById('err').style.display='block';}</script>
</body></html>`);
});

app.post('/login', (req, res) => {
  if (req.body.senha !== SENHA) return res.status(401).json({ error: 'Senha incorreta' });
  const sid = gerarSession();
  res.setHeader('Set-Cookie', `vm_session=${sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400`);
  res.json({ ok: true });
});

app.post('/logout', (req, res) => {
  const m = (req.headers.cookie||'').match(/vm_session=([a-f0-9]+)/);
  if (m) SESSIONS.delete(m[1]);
  res.setHeader('Set-Cookie', 'vm_session=; Path=/; Max-Age=0');
  res.redirect('/login');
});

// ── OAuth ─────────────────────────────────────────────────────────────────────
app.get('/auth', (req, res) => {
  res.redirect(`https://${SHOP}/admin/oauth/authorize?client_id=${API_KEY}&scope=${SCOPES}&redirect_uri=${encodeURIComponent(APP_URL+'/auth/callback')}&state=${crypto.randomBytes(16).toString('hex')}`);
});
app.get('/auth/callback', async (req, res) => {
  try {
    const resp = await fetch(`https://${SHOP}/admin/oauth/access_token`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: API_KEY, client_secret: API_SECRET, code: req.query.code })
    });
    const data = await resp.json();
    if (data.access_token) { await saveToken(data.access_token); res.redirect('/'); }
    else res.status(400).send('Erro: ' + JSON.stringify(data));
  } catch(e) { res.status(500).send('Erro: ' + e.message); }
});

function shopHeaders(t) { return { 'X-Shopify-Access-Token': t, 'Content-Type': 'application/json' }; }

// ── Modelos ───────────────────────────────────────────────────────────────────
const MODELOS_MAP = [
  { chave:'Madison Mini', termo:'madison mini' },
  { chave:'Madison', termo:'madison' },
  { chave:'Mala de Rodinhas', termo:'rodinhas' },
  { chave:'Madeleine', termo:'madeleine' },
  { chave:'Mochila 2 em 1', termo:'mochila 2 em 1' },
  { chave:'Mochila 2 em 1', termo:'mochila 2em1' },
  { chave:'Mochila 2 em 1', termo:'2 em 1' },
  { chave:'Mochila 2 em 1', termo:'2em1' },
  { chave:'Mochila Mummy', termo:'mummy' },
  { chave:'Louise Mini', termo:'louise mini' },
  { chave:'Louise', termo:'louise' },
  { chave:'Bolsa Cleo', termo:'cleo' },
  { chave:'Bolsa Cloé', termo:'cloé' },
  { chave:'Bolsa Cloé', termo:'cloe' },
  { chave:'Bolsa Liz', termo:'bolsa liz' },
  { chave:'Bolsa Kate', termo:'kate' },
  { chave:'Frasqueira', termo:'frasqueira' },
  { chave:'Trocador', termo:'trocador' },
  { chave:'Necessaire', termo:'necessaire' },
  { chave:'Porta Documentos', termo:'porta documento' },
  { chave:'Porta Look', termo:'porta look' },
  { chave:'Porta Chupetas', termo:'porta chupeta' },
  { chave:'Porta Vacinas', termo:'porta vacina' },
  { chave:'Porta Vacinas', termo:'vacina' },
  { chave:'Pingente', termo:'pingente' },
  { chave:'Organizador', termo:'organizador' },
  { chave:'Saquinho', termo:'saquinho' },
  { chave:'Alça', termo:'alça' },
  { chave:'Alça', termo:'alca' },
  { chave:'Capa da Mala', termo:'capa da mala' },
  { chave:'Capa da Mala', termo:'capa mala' },
  { chave:'Laço', termo:'laço' },
  { chave:'Laço', termo:'laco' },
];

function extrairModeloCristal(title) {
  const t = title.toLowerCase();
  if (!t.includes('cristal')) return null;
  if (t.includes('kit cristal') || t.includes('kit de cristal')) return 'Kit Cristal';
  if (t.includes('pp')) return 'Cristal PP';
  if (t.match(/cristal\s*(tamanho\s*)?g\b/i)) return 'Cristal G';
  if (t.match(/cristal\s*(tamanho\s*)?m\b/i)) return 'Cristal M';
  if (t.match(/cristal\s*(tamanho\s*)?p\b/i)) return 'Cristal P';
  return 'Kit Cristal';
}

function extrairModeloBase(title) {
  if (!title) return 'Outros';
  const t = title.toLowerCase();
  if (t.includes('cristal')) { const c = extrairModeloCristal(title); if (c) return c; }
  for (const m of MODELOS_MAP) { if (t.includes(m.termo)) return m.chave; }
  return title.replace(/\b(bolsa|mochila|mala|maternidade|ella|urban chic|nós|nos|origem|le petit|tressê palha|bege|preto|marinho|caramelo|café|cafe|cinza|bordô|bordo|off white|rosé|rose|marrom|verde|nude|vinho|rosa|azul|preta)\b/gi,'').replace(/\s+/g,' ').trim() || title;
}

function extrairCorDoTitulo(title) {
  const cols = ['Ella','Urban Chic','Nós','Origem','Le Petit','Tressê Palha'];
  const cores = ['Café','Caramelo','Off White','Marinho','Bordô','Cinza','Bege','Preto','Rosé','Marrom','Verde','Nude','Vinho','Rosa','Azul','Preta'];
  let c = '', r = '';
  for (const x of cols) { if (title.toLowerCase().includes(x.toLowerCase())) { c = x; break; } }
  for (const x of cores) { if (title.toLowerCase().includes(x.toLowerCase())) { r = x; break; } }
  return [c, r].filter(Boolean).join(' ');
}

function extrairColecaoCor(title, variant) {
  if (!title) return '';
  if (variant && variant !== 'Default Title') return variant;
  return extrairCorDoTitulo(title) || title;
}

function expandirKit(item) {
  const title = item.title || '';
  if (!title.toLowerCase().includes('kit')) return [item];
  const parteDesc = title.replace(/^kit\s+[\w\sÀ-ú]+[-:]/i, '') || title;
  const partes = parteDesc.split(/,|\se\s/i).map(p => p.trim()).filter(p => p.length > 2);
  const itens = [];
  for (const parte of partes) {
    const m = MODELOS_MAP.find(m => parte.toLowerCase().includes(m.termo));
    if (m) itens.push({ ...item, title: m.chave + ' ' + extrairCorDoTitulo(title), isKitItem: true, kitOriginal: title });
  }
  return itens.length > 0 ? itens : [item];
}

function limparVendedora(nome) {
  return nome ? nome.replace(/^(CONSULTORA|CONSULTOR|VENDEDORA|VENDEDOR|ATENDENTE|REP|REPRESENTANTE)\s+/i, '').trim() : null;
}

function normalizarData(str) {
  if (!str) return null;
  const d = str.replace(/[.\-]/g, '/').trim();
  const parts = d.split('/');
  if (parts.length < 2) return d;
  const dia = parts[0].padStart(2,'0');
  const mes = parts[1].padStart(2,'0');
  if (parts.length >= 3) {
    const ano = parts[2].length === 2 ? '20' + parts[2] : parts[2];
    return `${dia}/${mes}/${ano}`;
  }
  const hoje = new Date();
  const anoAtual = hoje.getFullYear();
  const dataComAnoAtual = new Date(`${anoAtual}-${mes}-${dia}`);
  const seisM = new Date(hoje); seisM.setMonth(hoje.getMonth() - 6);
  const anoFinal = dataComAnoAtual < seisM ? anoAtual + 1 : anoAtual;
  return `${dia}/${mes}/${anoFinal}`;
}

function parseObs(note) {
  if (!note) return { vendedora:null, dataEnvio:null, bordadoGeral:null, bordadosPorModelo:{}, obsCliente:null, isProntaEntrega:false, isUrgente:false };
  const lines = note.split('\n').map(l => l.trim()).filter(Boolean);
  let vendedora=null, dataEnvio=null, bordadoGeral=null, isProntaEntrega=false;
  const bordadosPorModelo = {}, extras = [];

  for (const line of lines) {
    if (line.match(/^(PRONTA\s*ENTREGA|PE)$/i)) { isProntaEntrega = true; continue; }
    const mPed = line.match(/^PEDIDO[-:\s]+([A-ZÀ-Úa-zà-ú\s]+)$/i);
    if (mPed && !vendedora) { vendedora = limparVendedora(mPed[1].trim()); continue; }
    const mData = line.match(/^DATA\s+(?:DE\s+)?ENVIO[:\s]+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (mData) { dataEnvio = normalizarData(mData[1]); continue; }
    const m1 = line.match(/^([A-ZÀ-Úa-zà-ú\s]+?)\s*[-–]\s*(?:ENVIO|ENV|ENTREGA|ENVIAR)\s+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1) { vendedora = limparVendedora(m1[1].trim()); dataEnvio = normalizarData(m1[2]); continue; }
    const m1b = line.match(/^(?:ENVIO|ENV|ENTREGA|ENVIAR)[:\s]+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1b && !dataEnvio) { dataEnvio = normalizarData(m1b[1]); continue; }
    const m2 = line.match(/^([A-ZÀ-Úa-zà-ú\s0-9]+?):\s*(?:BORDADO[:\s]+)?(.+)/i);
    if (m2) {
      const key = m2[1].trim().toLowerCase();
      // Remove "BORDADO:" do início do valor se vier duplicado
      let val = m2[2].trim().replace(/^BORDADO[:\s]+/i, '').trim();
      const isModelo = MODELOS_MAP.some(m => key.includes(m.termo)) ||
        ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','alça','alca','frasqueira','pingente','kate','liz','cleo','cloé','cristal'].some(k => key.includes(k));
      if (isModelo) { bordadosPorModelo[key] = val.match(/^(sem\s*bordado|s\/bordado|sem|s\/|s\/b)$/i) ? null : val; continue; }
    }
    const m3 = line.match(/^BORDADO\s+(.+)/i);
    if (m3) { bordadoGeral = m3[1].trim(); continue; }
    const m4 = line.match(/NOME\s+AL[CÇ]A[:\s]+(.+)/i);
    if (m4) { bordadoGeral = m4[1].trim(); continue; }
    const m5 = line.match(/^NOME[:\s]+(.+)/i);
    if (m5 && !bordadoGeral) { bordadoGeral = m5[1].trim(); continue; }
    if (line.match(/^COLE[ÇC][ÃA]O\s+/i)) continue;
    if (line.match(/^BRINDE/i)) continue;
    if (line.length > 2) extras.push(line);
  }
  const isUrgente = note.match(/urgente/i) !== null;
  return { vendedora, dataEnvio, bordadoGeral, bordadosPorModelo, obsCliente: extras.join(' | ')||null, isProntaEntrega, isUrgente };
}

function getBordado(obs, modeloBase) {
  if (!obs) return null;
  const mb = (modeloBase||'').toLowerCase();
  const bpm = obs.bordadosPorModelo || {};

  // Busca exata primeiro
  for (const [key, val] of Object.entries(bpm)) {
    if (mb === key) return val;
  }

  // Busca por inclusão — modeloBase contém a chave ou vice-versa
  for (const [key, val] of Object.entries(bpm)) {
    if (mb.includes(key) || key.includes(mb)) return val;
  }

  // Busca pela primeira palavra significativa do modelo
  const primeiraPalavra = mb.split(' ').find(w => w.length > 2);
  if (primeiraPalavra) {
    for (const [key, val] of Object.entries(bpm)) {
      if (key.includes(primeiraPalavra)) return val;
    }
  }

  return obs.bordadoGeral || null;
}

function calcStatus(tags, dataEnvio, isProntaEntrega, fulfillmentStatus) {
  const t = (tags||'').toLowerCase();
  if (fulfillmentStatus === 'fulfilled') return 'enviado';
  if (isProntaEntrega || t.includes('pronta-entrega') || t.includes('pronta entrega')) return 'pronta_entrega';
  if (t.includes('enviado')) return 'enviado';
  if (t.includes('pronto')) return 'pronto';
  if (t.includes('em_producao') || t.includes('em-producao') || t.includes('produção') || t.includes('producao')) return 'em_producao';
  if (dataEnvio && dataEnvio !== '—') {
    const p = dataEnvio.split('/');
    if (p.length >= 2) {
      const y = p[2] ? (p[2].length===2?'20'+p[2]:p[2]) : new Date().getFullYear();
      const d = new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
      if (!isNaN(d) && d < new Date()) {
        const dias = Math.round((new Date()-d)/(1000*60*60*24));
        return dias > 60 ? 'enviado' : 'atrasado';
      }
    }
  }
  return 'aguardando';
}

function fmtDate(d) {
  if (!d) return null;
  return new Date(d).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'});
}

function diasUteisAte(dataStr) {
  if (!dataStr || dataStr === '—') return null;
  const p = dataStr.split('/');
  if (p.length < 2) return null;
  const y = p[2] ? (p[2].length===2?'20'+p[2]:p[2]) : new Date().getFullYear();
  const alvo = new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
  const hoje = new Date(); hoje.setHours(0,0,0,0);
  if (alvo < hoje) return -1;
  let d = new Date(hoje), uteis = 0;
  while (d < alvo) { d.setDate(d.getDate()+1); if(d.getDay()!==0&&d.getDay()!==6) uteis++; }
  return uteis;
}

async function mapItem(order, item, isDraft) {
  const obs = parseObs(order.note);
  const modeloBase = extrairModeloBase(item.title);
  const dataEnvio = obs.dataEnvio || '—';
  const listas = await loadListas();
  const lista = listas.find(l => l.pedidoIds && l.pedidoIds.includes(String(order.id)));
  return {
    id: isDraft ? 'D-'+order.id : order.id,
    orderId: String(order.id),
    numero: isDraft ? '#D-'+String(order.id).slice(-4) : '#'+order.order_number,
    dataPedido: fmtDate(order.created_at),
    dataEnvio, diasUteisRestantes: diasUteisAte(dataEnvio),
    vendedora: obs.vendedora || '—',
    modeloBase, modelo: item.title,
    colecaoCor: extrairColecaoCor(item.title, item.variant_title),
    bordado: getBordado(obs, modeloBase),
    obsCliente: obs.obsCliente || '—',
    noteRaw: order.note || '',
    status: calcStatus(order.tags, dataEnvio, obs.isProntaEntrega, order.fulfillment_status),
    isUrgente: obs.isUrgente || (order.tags||'').toLowerCase().includes('urgente'),
    isDraft, isKitItem: item.isKitItem||false, kitOriginal: item.kitOriginal||null,
    tags: order.tags||'', quantidade: item.quantity||1,
    listaNumero: lista ? lista.numero : null,
    listaDataProducao: lista ? lista.dataProducao : null,
  };
}

// Cache
let CACHE_PEDIDOS = [], CACHE_TS = 0;
const CACHE_TTL = 30 * 60 * 1000;

async function buscarTodosPedidos(token, params={}) {
  const { data_de, data_ate, filtro_data_tipo } = params;
  let dp = '';
  if (data_de && filtro_data_tipo!=='pedido') dp+=`&created_at_min=${new Date(data_de).toISOString()}`;
  if (data_ate && filtro_data_tipo!=='pedido') dp+=`&created_at_max=${new Date(data_ate+'T23:59:59').toISOString()}`;

  let allOrders = [];
  let url = `https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250${dp}&fields=id,order_number,created_at,note,tags,line_items,fulfillment_status`;
  while (url) {
    const r = await fetch(url, { headers: shopHeaders(token) });
    if (!r.ok) throw new Error(`Shopify ${r.status}`);
    const d = await r.json();
    allOrders = allOrders.concat(d.orders||[]);
    const link = r.headers.get('link')||'';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }

  let lista = [];
  for (const o of allOrders) {
    for (const i of o.line_items) {
      for (const ei of expandirKit(i)) {
        lista.push(await mapItem(o, ei, false));
      }
    }
  }

  try {
    const dr = await fetch(`https://${SHOP}/admin/api/2024-01/draft_orders.json?status=open&limit=250`, { headers: shopHeaders(token) });
    if (dr.ok) {
      const dd = await dr.json();
      for (const o of dd.draft_orders||[]) {
        for (const i of o.line_items) {
          for (const ei of expandirKit(i)) {
            lista.push(await mapItem(o, ei, true));
          }
        }
      }
    }
  } catch(e) {}

  if (filtro_data_tipo==='pedido' && data_de) {
    lista = lista.filter(p => {
      if (!p.dataPedido) return false;
      const pts = p.dataPedido.split('/');
      const d = new Date(`${pts[2]}-${pts[1]}-${pts[0]}`);
      if (data_de && d < new Date(data_de)) return false;
      if (data_ate && d > new Date(data_ate+'T23:59:59')) return false;
      return true;
    });
  }
  return lista;
}

async function getPedidos(token, params={}) {
  const temFiltros = params.data_de||params.data_ate||params.filtro_data_tipo;
  if (!temFiltros && CACHE_PEDIDOS.length && (Date.now()-CACHE_TS) < CACHE_TTL) return CACHE_PEDIDOS;
  const pedidos = await buscarTodosPedidos(token, params);
  if (!temFiltros) { CACHE_PEDIDOS = pedidos; CACHE_TS = Date.now(); }
  return pedidos;
}

setInterval(async () => {
  const token = await loadToken();
  if (!token) return;
  try { CACHE_PEDIDOS = await buscarTodosPedidos(token); CACHE_TS = Date.now(); console.log('Cache atualizado:', CACHE_PEDIDOS.length); }
  catch(e) { console.error('Auto-update:', e.message); }
}, CACHE_TTL);

function gerarFichaCorte(pedidos) {
  const excluir = ['trocador','necessaire','pingente','organizador','saquinho','porta look','porta documento','porta chupeta','porta vacina','kit cristal','alça','alca','capa da mala','capa mala','laço','laco'];
  const ficha = {};
  for (const p of pedidos) {
    if (excluir.some(k => (p.modeloBase||'').toLowerCase().includes(k))) continue;
    const cor = p.colecaoCor || 'Sem cor';
    if (!ficha[cor]) ficha[cor] = {};
    ficha[cor][p.modeloBase] = (ficha[cor][p.modeloBase]||0) + (p.quantidade||1);
  }
  return Object.entries(ficha)
    .map(([cor,mods]) => ({cor, modelos:Object.entries(mods).map(([modelo,qtd])=>({modelo,qtd})).sort((a,b)=>b.qtd-a.qtd), total:Object.values(mods).reduce((s,v)=>s+v,0)}))
    .sort((a,b)=>b.total-a.total);
}

function simularPrazo(pedidos, dataLoteStr) {
  const total = pedidos.reduce((s,p)=>s+(p.quantidade||1),0);
  const diasNec = Math.ceil(total/CAPACIDADE_DIARIA);
  let diasUteisSobram = null;
  if (dataLoteStr) {
    const p = dataLoteStr.split('/');
    const y = p[2]?(p[2].length===2?'20'+p[2]:p[2]):new Date().getFullYear();
    const alvo = new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
    let d = new Date(), u = 0;
    while(d<alvo){d.setDate(d.getDate()+1);if(d.getDay()!==0&&d.getDay()!==6)u++;}
    diasUteisSobram = u;
  }
  const ok = diasUteisSobram===null||diasUteisSobram>=diasNec;
  return { totalPecas:total, diasNecessarios:diasNec, capacidadeDiaria:CAPACIDADE_DIARIA, diasUteisSobram, status:ok?'ok':'atencao',
    mensagem: diasUteisSobram!==null?(ok?`✅ ${total} peças · ${diasNec} dias úteis · folga de ${diasUteisSobram-diasNec} dias`:`⚠️ ATENÇÃO: ${total} peças precisam de ${diasNec} dias úteis mas só restam ${diasUteisSobram}!`):`${total} peças · ${diasNec} dias úteis necessários` };
}

// ── Rotas ─────────────────────────────────────────────────────────────────────
app.get('/api/ping', async (req,res) => {
  const t = await loadToken();
  res.json({ok:true,shop:SHOP,hasToken:!!t,cacheSize:CACHE_PEDIDOS.length});
});

// Força limpeza do cache e resincroniza
app.post('/api/sync', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    CACHE_TS = 0;
    CACHE_PEDIDOS = [];
    CACHE_PEDIDOS = await buscarTodosPedidos(token);
    CACHE_TS = Date.now();
    res.json({ok:true, total:CACHE_PEDIDOS.length});
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/pedidos', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado',authUrl:`${APP_URL}/auth`});
  try { res.json({pedidos:await getPedidos(token,req.query)}); }
  catch(e){console.error(e);res.status(500).json({error:e.message});}
});

app.get('/api/modelos', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try { const l=await getPedidos(token); res.json({modelos:[...new Set(l.map(p=>p.modeloBase))].sort()}); }
  catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/vendedoras', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try { const l=await getPedidos(token); const v=new Set(l.map(p=>p.vendedora).filter(x=>x&&x!=='—')); res.json({vendedoras:[...v].sort()}); }
  catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/ficha-corte', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const lista = await getPedidos(token);
    const filtrado = req.query.dataEnvio ? lista.filter(p=>p.dataEnvio===req.query.dataEnvio) : lista;
    res.json({ficha:gerarFichaCorte(filtrado),simulador:simularPrazo(filtrado,req.query.dataEnvio||null),total:filtrado.length});
  } catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/painel-bordado', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const lista = await getPedidos(token);
    const filtrado = req.query.dataEnvio ? lista.filter(p=>p.dataEnvio===req.query.dataEnvio) : lista;
    const comBordado = filtrado.filter(p=>p.bordado).sort((a,b)=>{
      const da=a.dataEnvio!=='—'?a.dataEnvio.split('/').reverse().join(''):'99999999';
      const db=b.dataEnvio!=='—'?b.dataEnvio.split('/').reverse().join(''):'99999999';
      return da.localeCompare(db);
    });
    res.json({bordados:comBordado,total:comBordado.length});
  } catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/listas', async (req,res) => { res.json({listas:await loadListas()}); });

app.post('/api/listas', async (req,res) => {
  const { nome, pedidoIds, dataEnvio, totalPecas, modelos } = req.body;
  const numero = await proximoNumeroLista();
  const lista = { id:crypto.randomBytes(8).toString('hex'), numero, nome:nome||`Lista #${numero}`,
    pedidoIds:pedidoIds||[], dataEnvio:dataEnvio||null, dataProducao:null,
    totalPecas:totalPecas||0, modelos:modelos||[], status:'em_producao', criadaEm:new Date().toISOString() };
  await salvarLista(lista);
  CACHE_TS = 0; // invalida cache para atualizar listaNumero
  res.json({lista});
});

app.put('/api/listas/:id', async (req,res) => {
  await atualizarLista(req.params.id, req.body);
  const listas = await loadListas();
  res.json({lista:listas.find(l=>l.id===req.params.id)});
});

app.delete('/api/listas/:id', async (req,res) => {
  await deletarLista(req.params.id);
  res.json({ok:true});
});

app.post('/api/pedido/:id/status', async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const {id}=req.params; const {novaTag}=req.body;
    const r=await fetch(`https://${SHOP}/admin/api/2024-01/orders/${id}.json`,{
      method:'PUT',headers:shopHeaders(token),body:JSON.stringify({order:{id,tags:novaTag}})
    });
    CACHE_TS=0;
    res.json(await r.json());
  } catch(e){res.status(500).json({error:e.message});}
});

app.use(express.static(path.join(__dirname,'public')));
app.use(express.static(__dirname));
app.get('*',(req,res)=>{
  const p1=path.join(__dirname,'public','index.html');
  const p2=path.join(__dirname,'index.html');
  if(fs.existsSync(p1)) return res.sendFile(p1);
  if(fs.existsSync(p2)) return res.sendFile(p2);
  res.status(404).send('Carregando...');
});

const PORT = process.env.PORT||3000;
initDB().then(()=>{
  app.listen(PORT,'0.0.0.0',()=>console.log(`VM Sistema porta ${PORT}`));
}).catch(e=>{
  console.error('Erro ao inicializar DB:', e.message);
  app.listen(PORT,'0.0.0.0',()=>console.log(`VM Sistema porta ${PORT} (sem DB)`));
});
