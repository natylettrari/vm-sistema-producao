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
const LISTAS_FILE = '/tmp/listas_producao.json';
const CAPACIDADE_DIARIA = 35;

// ── Token ─────────────────────────────────────────────────────────────────────
function saveToken(t) { fs.writeFileSync(TOKEN_FILE, JSON.stringify({ token:t })); }
function loadToken() {
  try {
    if (process.env.SHOPIFY_TOKEN) return process.env.SHOPIFY_TOKEN;
    if (fs.existsSync(TOKEN_FILE)) return JSON.parse(fs.readFileSync(TOKEN_FILE)).token;
  } catch(e) {}
  return null;
}
function shopHeaders(t) { return { 'X-Shopify-Access-Token': t, 'Content-Type': 'application/json' }; }

// ── Listas de produção ────────────────────────────────────────────────────────
function loadListas() {
  try { if (fs.existsSync(LISTAS_FILE)) return JSON.parse(fs.readFileSync(LISTAS_FILE)); } catch(e) {}
  return [];
}
function saveListas(listas) { fs.writeFileSync(LISTAS_FILE, JSON.stringify(listas, null, 2)); }
function proximoNumeroLista() {
  const listas = loadListas();
  if (!listas.length) return '0001';
  const ultimo = Math.max(...listas.map(l => parseInt(l.numero)||0));
  return String(ultimo + 1).padStart(4, '0');
}

// ── OAuth ─────────────────────────────────────────────────────────────────────
app.get('/auth', (req, res) => {
  const redirectUri = `${APP_URL}/auth/callback`;
  res.redirect(`https://${SHOP}/admin/oauth/authorize?client_id=${API_KEY}&scope=${SCOPES}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${crypto.randomBytes(16).toString('hex')}`);
});
app.get('/auth/callback', async (req, res) => {
  try {
    const resp = await fetch(`https://${SHOP}/admin/oauth/access_token`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: API_KEY, client_secret: API_SECRET, code: req.query.code })
    });
    const data = await resp.json();
    if (data.access_token) { saveToken(data.access_token); res.redirect('/'); }
    else res.status(400).send('Erro: ' + JSON.stringify(data));
  } catch(e) { res.status(500).send('Erro: ' + e.message); }
});

// ── Modelos ───────────────────────────────────────────────────────────────────
const MODELOS_MAP = [
  { chave:'Madison Mini', termo:'madison mini' },
  { chave:'Madison', termo:'madison' },
  { chave:'Mala de Rodinhas', termo:'rodinhas' },
  { chave:'Madeleine', termo:'madeleine' },
  { chave:'Mochila 2 em 1', termo:'mochila 2 em 1' },
  { chave:'Mochila 2 em 1', termo:'mochila 2em1' },
  { chave:'Mochila Mummy', termo:'mummy' },
  { chave:'Louise Mini', termo:'louise mini' },
  { chave:'Louise', termo:'louise' },
  { chave:'Bolsa Cleo', termo:'cleo' },
  { chave:'Bolsa Cloé', termo:'cloé' },
  { chave:'Bolsa Cloé', termo:'cloe' },
  { chave:'Bolsa Liz', termo:'bolsa liz' },
  { chave:'Bolsa Kate', termo:'bolsa kate' },
  { chave:'Frasqueira', termo:'frasqueira' },
  { chave:'Trocador', termo:'trocador' },
  { chave:'Necessaire', termo:'necessaire' },
  { chave:'Porta Documentos', termo:'porta documento' },
  { chave:'Porta Look', termo:'porta look' },
  { chave:'Kit Cristal', termo:'kit cristal' },
  { chave:'Pingente', termo:'pingente' },
  { chave:'Organizador', termo:'organizador' },
  { chave:'Saquinho', termo:'saquinho' },
  { chave:'Alça', termo:'alça' },
  { chave:'Alça', termo:'alca' },
];

function extrairModeloBase(title) {
  if (!title) return 'Outros';
  const t = title.toLowerCase();
  for (const m of MODELOS_MAP) { if (t.includes(m.termo)) return m.chave; }
  return title.replace(/\b(bolsa|mochila|mala|maternidade|ella|urban chic|nós|nos|origem|le petit|tressê palha|bege|preto|marinho|caramelo|café|cafe|cinza|bordô|bordo|off white|rosé|rose|marrom|verde|nude|vinho|rosa|azul)\b/gi,'').replace(/\s+/g,' ').trim() || title;
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

// ── Expansão de kits ──────────────────────────────────────────────────────────
function expandirKit(item) {
  const title = item.title || '';
  if (!title.toLowerCase().includes('kit')) return [item];
  const parteDesc = title.replace(/^kit\s+[\w\s]+[-:]/i, '') || title;
  const partes = parteDesc.split(/,|\se\s/i).map(p => p.trim()).filter(p => p.length > 2);
  const itens = [];
  for (const parte of partes) {
    const m = MODELOS_MAP.find(m => parte.toLowerCase().includes(m.termo));
    if (m) {
      const cor = extrairCorDoTitulo(title);
      itens.push({ ...item, title: m.chave + (cor?' '+cor:''), isKitItem:true, kitOriginal:title });
    }
  }
  return itens.length > 0 ? itens : [item];
}

// ── Parser observações ────────────────────────────────────────────────────────
function limparVendedora(nome) {
  if (!nome) return null;
  return nome.replace(/^(CONSULTORA|CONSULTOR|VENDEDORA|VENDEDOR|ATENDENTE|REP|REPRESENTANTE)\s+/i, '').trim();
}

function normalizarData(str) {
  if (!str) return null;
  const d = str.replace(/[.\-]/g, '/').trim();
  const parts = d.split('/');
  if (parts.length >= 2) return parts[0].padStart(2,'0') + '/' + parts[1].padStart(2,'0');
  return d;
}

function parseObs(note) {
  if (!note) return { vendedora:null, dataEnvio:null, bordadoGeral:null, bordadosPorModelo:{}, obsCliente:null, isProntaEntrega:false };
  const lines = note.split('\n').map(l => l.trim()).filter(Boolean);
  let vendedora=null, dataEnvio=null, bordadoGeral=null, isProntaEntrega=false;
  const bordadosPorModelo = {};
  const extras = [];

  for (const line of lines) {
    if (line.match(/^(PRONTA\s*ENTREGA|PE)$/i)) { isProntaEntrega = true; continue; }

    // Padrão: PEDIDO-MARI ou PEDIDO MARI (nome após PEDIDO)
    const mPed = line.match(/^PEDIDO[-:\s]+([A-ZÀ-Úa-zà-ú\s]+)$/i);
    if (mPed && !vendedora) { vendedora = limparVendedora(mPed[1].trim()); continue; }

    // Padrão: DATA DE ENVIO 20/05 ou DATA ENVIO 20/05
    const mData = line.match(/^DATA\s+(?:DE\s+)?ENVIO[:\s]+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (mData) { dataEnvio = normalizarData(mData[1]); continue; }

    // Padrão principal: NOME - ENVIO DD/MM
    const m1 = line.match(/^([A-ZÀ-Úa-zà-ú\s]+?)\s*[-–]\s*(?:ENVIO|ENV|ENTREGA|ENVIAR)\s+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1) { vendedora = limparVendedora(m1[1].trim()); dataEnvio = normalizarData(m1[2]); continue; }

    // Padrão: ENVIO DD/MM (sem nome)
    const m1b = line.match(/^(?:ENVIO|ENV|ENTREGA|ENVIAR|DATA\s+DE\s+ENVIO)\s+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1b && !dataEnvio) { dataEnvio = normalizarData(m1b[1]); continue; }

    // Padrão: ENVIO: 20/05
    const m1c = line.match(/^(?:ENVIO|ENTREGA)[:\s]+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1c && !dataEnvio) { dataEnvio = normalizarData(m1c[1]); continue; }
    // Bordado por modelo: MALA RODINHAS: BORDADO Maria ou MOCHILA: INICIAL M
    const m2 = line.match(/^([A-ZÀ-Úa-zà-ú\s0-9]+?):\s*(?:BORDADO\s+)?(.+)/i);
    if (m2) {
      const key = m2[1].trim().toLowerCase();
      const val = m2[2].trim();
      const isModelo = MODELOS_MAP.some(m => key.includes(m.termo)) ||
        ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','alça','alca','frasqueira','pingente'].some(k => key.includes(k));
      if (isModelo) { bordadosPorModelo[key] = val.match(/^(sem\s*bordado|s\/bordado|sem|s\/)$/i) ? null : val; continue; }
    }
    const m3 = line.match(/^BORDADO\s+(.+)/i);
    if (m3) { bordadoGeral = m3[1].trim(); continue; }
    const m4 = line.match(/NOME\s+AL[CÇ]A[:\s]+(.+)/i);
    if (m4) { bordadoGeral = m4[1].trim(); continue; }
    const m5 = line.match(/^NOME[:\s]+(.+)/i);
    if (m5 && !bordadoGeral) { bordadoGeral = m5[1].trim(); continue; }
    if (line.match(/^COLE[ÇC][ÃA]O\s+/i)) continue;
    if (line.length > 2) extras.push(line);
  }
  return { vendedora, dataEnvio, bordadoGeral, bordadosPorModelo, obsCliente: extras.join(' | ')||null, isProntaEntrega };
}

function getBordado(obs, modeloBase) {
  if (!obs) return null;
  const mb = (modeloBase||'').toLowerCase();
  for (const [key, val] of Object.entries(obs.bordadosPorModelo||{})) {
    if (mb.includes(key) || key.includes(mb.split(' ')[0])) return val;
  }
  return obs.bordadoGeral || null;
}

function calcStatus(tags, dataEnvio, isProntaEntrega) {
  const t = (tags||'').toLowerCase();
  if (isProntaEntrega || t.includes('pronta-entrega') || t.includes('pronta entrega')) return 'pronta_entrega';
  if (t.includes('pronto') || t.includes('enviado')) return 'pronto';
  if (t.includes('em_producao') || t.includes('em-producao') || t.includes('produção') || t.includes('producao')) return 'em_producao';
  if (dataEnvio && dataEnvio !== '—') {
    const p = dataEnvio.split('/');
    if (p.length >= 2) {
      const y = p[2] ? (p[2].length===2?'20'+p[2]:p[2]) : new Date().getFullYear();
      const d = new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
      if (!isNaN(d) && d < new Date()) return 'atrasado';
    }
  }
  return 'aguardando';
}

function fmtDate(d) {
  if (!d) return null;
  return new Date(d).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'numeric'});
}

function mapItem(order, item, isDraft) {
  const obs = parseObs(order.note);
  const modeloBase = extrairModeloBase(item.title);
  const listas = loadListas();
  const listaDoItem = listas.find(l => l.pedidoIds && l.pedidoIds.includes(String(order.id)));
  return {
    id: isDraft ? 'D-'+order.id : order.id,
    orderId: String(order.id),
    numero: isDraft ? '#D-'+String(order.id).slice(-4) : '#'+order.order_number,
    dataPedido: fmtDate(order.created_at),
    dataEnvio: obs.dataEnvio || '—',
    vendedora: obs.vendedora || '—',
    modeloBase,
    modelo: item.title,
    colecaoCor: extrairColecaoCor(item.title, item.variant_title),
    bordado: getBordado(obs, modeloBase),
    obsCliente: obs.obsCliente || '—',
    status: calcStatus(order.tags, obs.dataEnvio, obs.isProntaEntrega),
    isDraft, isKitItem: item.isKitItem||false,
    kitOriginal: item.kitOriginal||null,
    tags: order.tags||'',
    quantidade: item.quantity||1,
    listaNumero: listaDoItem ? listaDoItem.numero : null,
    listaDataProducao: listaDoItem ? listaDoItem.dataProducao : null,
  };
}

// ── Buscar pedidos ────────────────────────────────────────────────────────────
async function buscarPedidos(token, params={}) {
  const { data_de, data_ate, filtro_data_tipo } = params;
  let dp = '';
  if (data_de && filtro_data_tipo!=='pedido') dp += `&created_at_min=${new Date(data_de).toISOString()}`;
  if (data_ate && filtro_data_tipo!=='pedido') dp += `&created_at_max=${new Date(data_ate+'T23:59:59').toISOString()}`;

  const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250${dp}`, { headers:shopHeaders(token) });
  if (!r.ok) throw new Error(`Shopify ${r.status}`);
  const d = await r.json();
  let lista = (d.orders||[]).flatMap(o => o.line_items.flatMap(i => expandirKit(i).map(ei => mapItem(o,ei,false))));

  try {
    const dr = await fetch(`https://${SHOP}/admin/api/2024-01/draft_orders.json?status=open&limit=250`, { headers:shopHeaders(token) });
    if (dr.ok) {
      const dd = await dr.json();
      lista = lista.concat((dd.draft_orders||[]).flatMap(o => o.line_items.flatMap(i => expandirKit(i).map(ei => mapItem(o,ei,true)))));
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

// ── Ficha de corte ────────────────────────────────────────────────────────────
function gerarFichaCorte(pedidos) {
  const excluir = ['trocador','necessaire','pingente','organizador','saquinho','porta look','porta documento','kit cristal'];
  const ficha = {};
  for (const p of pedidos) {
    if (excluir.some(k => (p.modeloBase||'').toLowerCase().includes(k))) continue;
    const cor = p.colecaoCor || 'Sem cor';
    if (!ficha[cor]) ficha[cor] = {};
    const m = p.modeloBase || 'Outros';
    ficha[cor][m] = (ficha[cor][m]||0) + (p.quantidade||1);
  }
  return Object.entries(ficha)
    .map(([cor,modelos]) => ({ cor, modelos: Object.entries(modelos).map(([modelo,qtd])=>({modelo,qtd})).sort((a,b)=>b.qtd-a.qtd), total: Object.values(modelos).reduce((s,v)=>s+v,0) }))
    .sort((a,b)=>b.total-a.total);
}

// ── Simulador de prazo ────────────────────────────────────────────────────────
function simularPrazo(pedidos, dataLoteStr) {
  const total = pedidos.reduce((s,p)=>s+(p.quantidade||1),0);
  const diasNec = Math.ceil(total/CAPACIDADE_DIARIA);
  let diasUteisSobram = null;
  if (dataLoteStr) {
    const p = dataLoteStr.split('/');
    const y = p[2]?(p[2].length===2?'20'+p[2]:p[2]):new Date().getFullYear();
    const dataLote = new Date(`${y}-${p[1].padStart(2,'0')}-${p[0].padStart(2,'0')}`);
    let d = new Date(), uteis = 0;
    while (d < dataLote) { d.setDate(d.getDate()+1); if(d.getDay()!==0&&d.getDay()!==6) uteis++; }
    diasUteisSobram = uteis;
  }
  const ok = diasUteisSobram===null || diasUteisSobram>=diasNec;
  return {
    totalPecas:total, diasNecessarios:diasNec, capacidadeDiaria:CAPACIDADE_DIARIA,
    diasUteisSobram, status:ok?'ok':'atencao',
    mensagem: diasUteisSobram!==null
      ? (ok ? `✅ ${total} peças · ${diasNec} dias úteis · folga de ${diasUteisSobram-diasNec} dias`
             : `⚠️ ATENÇÃO: ${total} peças precisam de ${diasNec} dias úteis mas só restam ${diasUteisSobram}!`)
      : `${total} peças · ${diasNec} dias úteis necessários`
  };
}

// ── Rotas API ─────────────────────────────────────────────────────────────────
app.get('/api/ping', (req,res) => { const t=loadToken(); res.json({ok:true,shop:SHOP,hasToken:!!t}); });

app.get('/api/pedidos', async (req,res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({ error:'Não autorizado', authUrl:`${APP_URL}/auth` });
  try { res.json({ pedidos: await buscarPedidos(token,req.query) }); }
  catch(e) { console.error(e); res.status(500).json({error:e.message}); }
});

app.get('/api/modelos', async (req,res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const lista = await buscarPedidos(token);
    res.json({ modelos: [...new Set(lista.map(p=>p.modeloBase))].sort() });
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/vendedoras', async (req,res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250&fields=note`, {headers:shopHeaders(token)});
    const d = await r.json();
    const v = new Set();
    for (const o of d.orders||[]) { const obs=parseObs(o.note); if(obs.vendedora) v.add(obs.vendedora); }
    res.json({ vendedoras:[...v].sort() });
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/ficha-corte', async (req,res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const lista = await buscarPedidos(token, req.query);
    const filtrado = req.query.dataEnvio ? lista.filter(p=>p.dataEnvio===req.query.dataEnvio) : lista;
    res.json({ ficha:gerarFichaCorte(filtrado), simulador:simularPrazo(filtrado,req.query.dataEnvio||null), total:filtrado.length });
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.get('/api/painel-bordado', async (req,res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const lista = await buscarPedidos(token, req.query);
    const filtrado = req.query.dataEnvio ? lista.filter(p=>p.dataEnvio===req.query.dataEnvio) : lista;
    const comBordado = filtrado.filter(p=>p.bordado).sort((a,b)=>{
      const da = a.dataEnvio!=='—'?a.dataEnvio.split('/').reverse().join(''):'99999999';
      const db = b.dataEnvio!=='—'?b.dataEnvio.split('/').reverse().join(''):'99999999';
      return da.localeCompare(db);
    });
    res.json({ bordados:comBordado, total:comBordado.length });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// ── Rotas de Listas de Produção ───────────────────────────────────────────────
app.get('/api/listas', (req,res) => { res.json({ listas:loadListas() }); });

app.post('/api/listas', (req,res) => {
  const { nome, pedidoIds, dataEnvio, totalPecas, modelos } = req.body;
  const listas = loadListas();
  const numero = proximoNumeroLista();
  const novaLista = {
    id: crypto.randomBytes(8).toString('hex'),
    numero,
    nome: nome || `Lista #${numero}`,
    pedidoIds: pedidoIds || [],
    dataEnvio: dataEnvio || null,
    dataProducao: null,
    totalPecas: totalPecas || 0,
    modelos: modelos || [],
    criadaEm: new Date().toISOString(),
    status: 'em_producao'
  };
  listas.push(novaLista);
  saveListas(listas);
  res.json({ lista: novaLista });
});

app.put('/api/listas/:id', (req,res) => {
  const listas = loadListas();
  const idx = listas.findIndex(l => l.id === req.params.id);
  if (idx === -1) return res.status(404).json({error:'Lista não encontrada'});
  listas[idx] = { ...listas[idx], ...req.body };
  saveListas(listas);
  res.json({ lista: listas[idx] });
});

app.delete('/api/listas/:id', (req,res) => {
  const listas = loadListas();
  const novas = listas.filter(l => l.id !== req.params.id);
  saveListas(novas);
  res.json({ ok:true });
});

app.post('/api/pedido/:id/status', async (req,res) => {
  const token = loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const {id} = req.params; const {novaTag} = req.body;
    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders/${id}.json`, {
      method:'PUT', headers:shopHeaders(token),
      body:JSON.stringify({order:{id,tags:novaTag}})
    });
    res.json(await r.json());
  } catch(e) { res.status(500).json({error:e.message}); }
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
app.listen(PORT,'0.0.0.0',()=>console.log(`VM Sistema porta ${PORT}`));
