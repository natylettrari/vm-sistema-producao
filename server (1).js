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
const APP_URL = process.env.APP_URL || 'https://vm-sistema-producao-production.up.railway.app';
const SCOPES = 'read_orders,read_draft_orders,read_products';
const TOKEN_FILE = '/tmp/shopify_token.json';

// Salvar e carregar token
function saveToken(token) {
  fs.writeFileSync(TOKEN_FILE, JSON.stringify({ token, savedAt: new Date().toISOString() }));
}

function loadToken() {
  try {
    if (process.env.SHOPIFY_TOKEN) return process.env.SHOPIFY_TOKEN;
    if (fs.existsSync(TOKEN_FILE)) {
      const data = JSON.parse(fs.readFileSync(TOKEN_FILE));
      return data.token;
    }
  } catch(e) {}
  return null;
}

function shopifyHeaders(token) {
  return { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' };
}

// ── OAuth: iniciar autorização ────────────────────────────────────────────────
app.get('/auth', (req, res) => {
  const state = crypto.randomBytes(16).toString('hex');
  const redirectUri = `${APP_URL}/auth/callback`;
  const authUrl = `https://${SHOP}/admin/oauth/authorize?client_id=${API_KEY}&scope=${SCOPES}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${state}`;
  res.redirect(authUrl);
});

// ── OAuth: callback ───────────────────────────────────────────────────────────
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
    if (data.access_token) {
      saveToken(data.access_token);
      console.log('Token OAuth obtido com sucesso!');
      res.redirect('/');
    } else {
      res.status(400).send('Erro ao obter token: ' + JSON.stringify(data));
    }
  } catch(e) {
    res.status(500).send('Erro: ' + e.message);
  }
});

// ── Middleware: verifica token ────────────────────────────────────────────────
function getToken() { return loadToken(); }

// ── Helpers ───────────────────────────────────────────────────────────────────
function parseObs(note) {
  if (!note) return { vendedora:null, dataEnvio:null, bordado:null, obsCliente:null };
  const lines = note.split('\n').map(l=>l.trim()).filter(Boolean);
  let vendedora=null, dataEnvio=null, bordado=null;
  const extras=[];
  for (const line of lines) {
    const m1 = line.match(/^([A-ZÀ-Ú][A-ZÀ-Úa-zà-ú\s]*?)\s*[-–]\s*ENVIO\s+(\d{1,2}\/\d{1,2}(?:\/\d{2,4})?)/i);
    if (m1) { vendedora=m1[1].trim(); dataEnvio=m1[2].trim(); continue; }
    const m2 = line.match(/^BORDADO\s+(.+)/i);
    if (m2) { bordado=m2[1].trim(); continue; }
    extras.push(line);
  }
  return { vendedora, dataEnvio, bordado, obsCliente: extras.join(' | ')||null };
}

function extrairModelo(title) {
  if (!title) return 'Produto';
  return title.replace(/\b(Ella|Urban Chic|Nós|Origem|Le Petit|Tressê Palha|Maternidade)\b/gi,'').replace(/\s+/g,' ').trim()||title;
}

function extrairColecaoCor(title, variant) {
  if (!title) return '';
  if (variant && variant!=='Default Title') return variant;
  const cols=['Ella','Urban Chic','Nós','Origem','Le Petit','Tressê Palha'];
  const cores=['Café','Caramelo','Off White','Marinho','Bordô','Cinza','Bege','Preto','Rosé','Marrom','Verde','Nude','Vinho','Rosa'];
  let c='', r='';
  for (const x of cols) { if (title.toLowerCase().includes(x.toLowerCase())) { c=x; break; } }
  for (const x of cores) { if (title.toLowerCase().includes(x.toLowerCase())) { r=x; break; } }
  return [c,r].filter(Boolean).join(' · ')||title;
}

function fmtDate(d) {
  if (!d) return null;
  return new Date(d).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'});
}

function calcStatus(tags, dataEnvio) {
  if (!tags) return 'aguardando';
  const t=tags.toLowerCase();
  if (t.includes('pronto')||t.includes('enviado')) return 'pronto';
  if (t.includes('producao')||t.includes('produção')||t.includes('em-producao')) return 'em_producao';
  if (t.includes('pronta-entrega')||t.includes('pronta entrega')) return 'pronta_entrega';
  if (dataEnvio && dataEnvio!=='—') {
    const p=dataEnvio.split('/');
    if (p.length>=2) {
      const y=p[2]?(p[2].length===2?'20'+p[2]:p[2]):new Date().getFullYear();
      const d=new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
      if (!isNaN(d)&&d<new Date()) return 'atrasado';
    }
  }
  return 'aguardando';
}

function mapItem(order, item, isDraft) {
  const obs=parseObs(order.note);
  return {
    id: isDraft?'D-'+order.id:order.id,
    numero: isDraft?'#D-'+String(order.id).slice(-4):'#'+order.order_number,
    dataPedido: fmtDate(order.created_at),
    dataEnvio: obs.dataEnvio||'—',
    vendedora: obs.vendedora||'—',
    modelo: extrairModelo(item.title),
    produtoCompleto: item.title,
    colecaoCor: extrairColecaoCor(item.title, item.variant_title),
    bordado: obs.bordado||null,
    obsCliente: obs.obsCliente||'—',
    status: calcStatus(order.tags, obs.dataEnvio),
    isDraft, tags: order.tags||'',
    quantidade: item.quantity||1
  };
}

// ── API: status ───────────────────────────────────────────────────────────────
app.get('/api/ping', (req,res) => {
  const token = getToken();
  res.json({ ok:true, shop:SHOP, hasToken:!!token, authUrl: token ? null : `${APP_URL}/auth` });
});

// ── API: pedidos ──────────────────────────────────────────────────────────────
app.get('/api/pedidos', async (req,res) => {
  const token = getToken();
  if (!token) return res.status(401).json({ error:'Não autorizado', authUrl:`${APP_URL}/auth` });
  try {
    const { data_de, data_ate, filtro_data_tipo } = req.query;
    let dp='';
    if (data_de && filtro_data_tipo!=='pedido') dp+=`&created_at_min=${new Date(data_de).toISOString()}`;
    if (data_ate && filtro_data_tipo!=='pedido') dp+=`&created_at_max=${new Date(data_ate+'T23:59:59').toISOString()}`;

    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250${dp}`, { headers: shopifyHeaders(token) });
    if (!r.ok) { const t=await r.text(); return res.status(r.status).json({error:`Shopify ${r.status}: ${t}`}); }
    const d = await r.json();
    let lista = (d.orders||[]).flatMap(o=>o.line_items.map(i=>mapItem(o,i,false)));

    try {
      const dr = await fetch(`https://${SHOP}/admin/api/2024-01/draft_orders.json?status=open&limit=250`,{headers:shopifyHeaders(token)});
      if (dr.ok) { const dd=await dr.json(); lista=lista.concat((dd.draft_orders||[]).flatMap(o=>o.line_items.map(i=>mapItem(o,i,true)))); }
    } catch(e){}

    res.json({ pedidos:lista, total:lista.length });
  } catch(e) { console.error(e); res.status(500).json({error:e.message}); }
});

app.get('/api/modelos', async (req,res) => {
  const token = getToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const r=await fetch(`https://${SHOP}/admin/api/2024-01/products.json?fields=title&limit=250`,{headers:shopifyHeaders(token)});
    const d=await r.json();
    res.json({modelos:[...new Set((d.products||[]).map(p=>extrairModelo(p.title)))].sort()});
  } catch(e){res.status(500).json({error:e.message});}
});

app.get('/api/vendedoras', async (req,res) => {
  const token = getToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const r=await fetch(`https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250&fields=note`,{headers:shopifyHeaders(token)});
    const d=await r.json();
    const v=new Set();
    for (const o of d.orders||[]) { const obs=parseObs(o.note); if(obs.vendedora) v.add(obs.vendedora); }
    res.json({vendedoras:[...v].sort()});
  } catch(e){res.status(500).json({error:e.message});}
});

// ── Serve frontend ────────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname,'public')));
app.use(express.static(__dirname));
app.get('*',(req,res)=>{
  const p1=path.join(__dirname,'public','index.html');
  const p2=path.join(__dirname,'index.html');
  if(fs.existsSync(p1)) return res.sendFile(p1);
  if(fs.existsSync(p2)) return res.sendFile(p2);
  res.status(404).send('Carregando...');
});

const PORT=process.env.PORT||3000;
app.listen(PORT,'0.0.0.0',()=>console.log(`VM Sistema porta ${PORT} — shop:${SHOP}`));
