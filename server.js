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
// Token da integração com o sistema de estoque. Definido SOMENTE via variável de
// ambiente (no Railway) — sem valor padrão no código, para o segredo não vazar pelo repositório.
const ESTOQUE_TOKEN = process.env.ESTOQUE_TOKEN || null;

// ── Senhas (hash seguro com scrypt nativo) ────────────────────────────────────
function hashSenha(senha) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derivada = crypto.scryptSync(String(senha), salt, 64).toString('hex');
  return salt + ':' + derivada;
}
function verificarSenha(senha, hashGuardado) {
  if (!hashGuardado || !hashGuardado.includes(':')) return false;
  const [salt, derivada] = hashGuardado.split(':');
  const teste = crypto.scryptSync(String(senha), salt, 64).toString('hex');
  // Comparação em tempo constante
  const a = Buffer.from(teste, 'hex');
  const b = Buffer.from(derivada, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// Papéis e suas permissões
const PAPEIS = {
  admin:      { label:'Administrador', podeEditar:true,  podeVerUsuarios:true,  somenteLeitura:false },
  producao:   { label:'Produção',      podeEditar:true,  podeVerUsuarios:false, somenteLeitura:false },
  vendedora:  { label:'Vendedora',     podeEditar:true,  podeVerUsuarios:false, somenteLeitura:false },
  edicao:     { label:'Edição',        podeEditar:true,  podeVerUsuarios:false, somenteLeitura:false },
  leitura:    { label:'Somente leitura',podeEditar:false, podeVerUsuarios:false, somenteLeitura:true },
};

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
    CREATE TABLE IF NOT EXISTS historico_pedidos (
    id SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    order_numero TEXT,
    item_id TEXT,
    campo TEXT NOT NULL,
    valor_anterior TEXT,
    valor_novo TEXT,
    alterado_por TEXT NOT NULL,
    alterado_em TIMESTAMP DEFAULT NOW()
  );
  CREATE TABLE IF NOT EXISTS pedidos_editados (
    order_id TEXT PRIMARY KEY,
    modelo_override TEXT,
    colecao_cor_override TEXT,
    bordado_override TEXT,
    data_envio_override TEXT,
    vendedora_override TEXT,
    atualizado_em TIMESTAMP DEFAULT NOW()
  );
  CREATE TABLE IF NOT EXISTS itens_editados (
    item_id TEXT PRIMARY KEY,
    order_id TEXT,
    modelo_override TEXT,
    colecao_cor_override TEXT,
    bordado_override TEXT,
    data_envio_override TEXT,
    vendedora_override TEXT,
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
  CREATE TABLE IF NOT EXISTS itens_producao (
      item_id TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'em_producao',
      lista_numero TEXT,
      atualizado_em TIMESTAMP DEFAULT NOW()
    );
  CREATE TABLE IF NOT EXISTS pronta_entrega (
      id TEXT PRIMARY KEY,
      modelo TEXT NOT NULL,
      colecao_cor TEXT,
      bordado TEXT,
      data_envio TEXT,
      obs TEXT,
      vendida BOOLEAN DEFAULT FALSE,
      numero_pedido TEXT,
      vendedora TEXT,
      criada_em TIMESTAMP DEFAULT NOW(),
      vendida_em TIMESTAMP
    );
  CREATE TABLE IF NOT EXISTS listas_pe (
      id TEXT PRIMARY KEY,
      numero TEXT NOT NULL,
      modelo TEXT NOT NULL,
      data_envio TEXT,
      data_producao TEXT,
      criada_em TIMESTAMP DEFAULT NOW()
    );
  CREATE TABLE IF NOT EXISTS pecas_pe (
      id TEXT PRIMARY KEY,
      lista_id TEXT NOT NULL,
      modelo TEXT NOT NULL,
      colecao_cor TEXT,
      bordado TEXT,
      obs TEXT,
      vendida BOOLEAN DEFAULT FALSE,
      numero_pedido TEXT,
      vendedora TEXT,
      criada_em TIMESTAMP DEFAULT NOW(),
      vendida_em TIMESTAMP
    );
  CREATE TABLE IF NOT EXISTS usuarios_v2 (
      id TEXT PRIMARY KEY,
      nome TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      whatsapp TEXT,
      senha_hash TEXT NOT NULL,
      papel TEXT NOT NULL DEFAULT 'leitura',
      ativo BOOLEAN DEFAULT TRUE,
      criado_em TIMESTAMP DEFAULT NOW()
    );
  `);
  // Garante a coluna item_id no histórico (para bancos que já existiam antes)
  try {
    await pool.query(`ALTER TABLE historico_pedidos ADD COLUMN IF NOT EXISTS item_id TEXT`);
  } catch(e) { console.error('ALTER historico_pedidos:', e.message); }
  // Garante a coluna data_producao em listas_pe (para bancos que já existiam antes)
  try {
    await pool.query(`ALTER TABLE listas_pe ADD COLUMN IF NOT EXISTS data_producao TEXT`);
  } catch(e) { console.error('ALTER listas_pe:', e.message); }
  // Garante colunas de status manual e observação interna em itens_editados
  try {
    await pool.query(`ALTER TABLE itens_editados ADD COLUMN IF NOT EXISTS status_override TEXT`);
    await pool.query(`ALTER TABLE itens_editados ADD COLUMN IF NOT EXISTS obs_interna TEXT`);
  } catch(e) { console.error('ALTER itens_editados:', e.message); }
  // Cria um admin inicial se não houver nenhum usuário ainda
  try {
    const r = await pool.query(`SELECT COUNT(*)::int AS n FROM usuarios_v2`);
    if (r.rows[0].n === 0) {
      const emailAdmin = (process.env.ADMIN_EMAIL || 'admin@vilmamirian.com').toLowerCase();
      const senhaAdmin = process.env.APP_PASSWORD || 'vilmamirian2025';
      await pool.query(
        `INSERT INTO usuarios_v2(id, nome, email, whatsapp, senha_hash, papel, ativo)
         VALUES($1,$2,$3,$4,$5,'admin',TRUE)`,
        [crypto.randomBytes(8).toString('hex'), 'Administrador', emailAdmin, null, hashSenha(senhaAdmin)]
      );
      console.log('Admin inicial criado:', emailAdmin);
    }
  } catch(e) { console.error('criar admin inicial:', e.message); }
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

// ── Status de produção por item (não se perde ao sincronizar) ─────────────────
// Carrega um mapa { itemId: status } de todos os itens marcados em produção
async function loadStatusItens() {
  try {
    const r = await pool.query(`SELECT item_id, status FROM itens_producao`);
    const mapa = {};
    for (const row of r.rows) mapa[row.item_id] = row.status;
    return mapa;
  } catch(e) { console.error('loadStatusItens:', e.message); return {}; }
}

// Marca uma lista de itemIds com um status (default em_producao)
async function marcarItensProducao(itemIds, listaNumero, status='em_producao') {
  if (!Array.isArray(itemIds) || !itemIds.length) return;
  for (const itemId of itemIds) {
    await pool.query(`
      INSERT INTO itens_producao(item_id, status, lista_numero, atualizado_em)
      VALUES($1, $2, $3, NOW())
      ON CONFLICT(item_id) DO UPDATE SET status=$2, lista_numero=$3, atualizado_em=NOW()
    `, [String(itemId), status, listaNumero || null]);
  }
}

// ── Sessões ───────────────────────────────────────────────────────────────────
// Mapa sessionId -> { userId, papel, nome, email }
const SESSIONS = new Map();

function gerarSession(usuario) {
  const id = crypto.randomBytes(32).toString('hex');
  SESSIONS.set(id, { userId: usuario.id, papel: usuario.papel, nome: usuario.nome, email: usuario.email });
  return id;
}

function sessionAtual(req) {
  const cookie = req.headers.cookie || '';
  const match = cookie.match(/vm_session=([a-f0-9]+)/);
  if (!match) return null;
  return SESSIONS.get(match[1]) || null;
}

function validarSession(req) {
  return sessionAtual(req) !== null;
}

// Bloqueia ações de escrita para usuários "somente leitura"
function exigirEdicao(req, res, next) {
  const s = sessionAtual(req);
  if (!s) return res.status(401).json({ error: 'Não autenticado' });
  const papel = PAPEIS[s.papel];
  if (papel && papel.somenteLeitura) return res.status(403).json({ error: 'Seu acesso é somente leitura' });
  next();
}

// Exige que o usuário seja admin
function exigirAdmin(req, res, next) {
  const s = sessionAtual(req);
  if (!s) return res.status(401).json({ error: 'Não autenticado' });
  if (s.papel !== 'admin') return res.status(403).json({ error: 'Apenas administradores' });
  next();
}

function authMiddleware(req, res, next) {
  if (req.path === '/login' || req.path === '/auth' || req.path === '/auth/callback') return next();
  // Rota de integração com o estoque: autenticada por token próprio (Bearer), não por sessão
  if (req.path === '/api/listas-estoque') return next();
  if (!validarSession(req)) {
    // Para chamadas de API responde 401; para páginas redireciona ao login
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Sessão expirada' });
    return res.redirect('/login');
  }
  next();
}
app.use(authMiddleware);

// Login
app.get('/login', (req, res) => {
  res.send(`<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Vilma Mirian — Produção</title>
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;1,400&family=Jost:wght@300;400;500&display=swap" rel="stylesheet">
<style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:'Jost',sans-serif;background:#fafaf8;display:flex;align-items:center;justify-content:center;min-height:100vh;color:#2a2526}.box{background:#fff;border:1px solid #e8dfe0;border-radius:16px;padding:48px 40px;width:360px;text-align:center;box-shadow:0 4px 24px rgba(138,76,82,.08)}.logo{font-family:'Playfair Display',serif;font-style:italic;font-size:28px;color:#8A4C52;margin-bottom:6px}.sub{font-size:12px;color:#9a8a8c;margin-bottom:32px;letter-spacing:.06em;text-transform:uppercase}input{width:100%;padding:12px 16px;border:1px solid #d4c8c9;border-radius:9px;font-size:14px;font-family:'Jost',sans-serif;color:#2a2526;background:#fafaf8;outline:none;transition:border .2s;margin-bottom:12px}input:focus{border-color:#8A4C52}button{width:100%;padding:12px;background:#8A4C52;color:#fff;border:none;border-radius:9px;font-size:13px;font-family:'Jost',sans-serif;letter-spacing:.08em;text-transform:uppercase;cursor:pointer;transition:background .2s}button:hover{background:#7a3d44}.err{color:#c0392b;font-size:12px;margin-bottom:12px;display:none}.hint{font-size:11px;color:#9a8a8c;margin-top:16px}</style></head>
<body><div class="box"><div class="logo">Vilma Mirian</div><div class="sub">Sistema de Produção</div>
<div class="err" id="err">Email ou senha incorretos</div>
<form onsubmit="entrar(event)">
  <input type="email" id="email" placeholder="Email" autocomplete="username">
  <input type="password" id="senha" placeholder="Senha" autocomplete="current-password" required>
  <button type="submit">Entrar</button>
</form>
<div class="hint">Esqueceu a senha? Peça ao administrador para redefinir.</div></div>
<script>async function entrar(e){e.preventDefault();const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:document.getElementById('email').value,senha:document.getElementById('senha').value})});if(r.ok)window.location.href='/';else document.getElementById('err').style.display='block';}</script>
</body></html>`);
});

app.post('/login', async (req, res) => {
  try {
    const email = (req.body.email||'').toLowerCase().trim();
    const senha = req.body.senha||'';

    // 0) ACESSO FIXO DE EMERGÊNCIA — sempre funciona, mesmo sem banco/usuário.
    //    Email: admin@vilmamirian.com   Senha: vilma2026
    const EMAIL_FIXO = 'admin@vilmamirian.com';
    const SENHA_FIXA = 'vilma2026';
    if (senha.trim() === SENHA_FIXA && (email === '' || email === EMAIL_FIXO)) {
      // Tenta garantir/registrar um admin no banco (mas não bloqueia o login se falhar)
      let adm = null;
      try {
        adm = (await pool.query(`SELECT * FROM usuarios_v2 WHERE papel='admin' AND ativo=TRUE ORDER BY criado_em ASC LIMIT 1`)).rows[0];
        if (!adm) {
          const id = crypto.randomBytes(8).toString('hex');
          await pool.query(
            `INSERT INTO usuarios_v2(id, nome, email, whatsapp, senha_hash, papel, ativo)
             VALUES($1,'Administrador',$2,NULL,$3,'admin',TRUE)`,
            [id, EMAIL_FIXO, hashSenha(SENHA_FIXA)]
          );
          adm = (await pool.query(`SELECT * FROM usuarios_v2 WHERE id=$1`, [id])).rows[0];
        }
      } catch(e) { console.error('acesso emergência (criar admin):', e.message); }
      // Sessão admin garantida (usa o do banco se existir, senão uma sintética)
      const usuarioSessao = adm || { id:'emergencia', papel:'admin', nome:'Administrador', email:EMAIL_FIXO };
      const sid = gerarSession(usuarioSessao);
      res.setHeader('Set-Cookie', `vm_session=${sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400`);
      return res.json({ ok: true });
    }

    // 1) Login normal: email + senha contra um usuário do banco
    if (email) {
      const u = (await pool.query(`SELECT * FROM usuarios_v2 WHERE email=$1 AND ativo=TRUE`, [email])).rows[0];
      if (u && verificarSenha(senha, u.senha_hash)) {
        const sid = gerarSession(u);
        res.setHeader('Set-Cookie', `vm_session=${sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400`);
        return res.json({ ok: true });
      }
    }

    // 2) Senha-mestra da variável de ambiente (se configurada e diferente da fixa)
    if (senha === SENHA) {
      let adm = (await pool.query(`SELECT * FROM usuarios_v2 WHERE papel='admin' AND ativo=TRUE ORDER BY criado_em ASC LIMIT 1`)).rows[0];
      if (!adm) {
        const emailAdmin = email || EMAIL_FIXO;
        const id = crypto.randomBytes(8).toString('hex');
        try {
          await pool.query(
            `INSERT INTO usuarios_v2(id, nome, email, whatsapp, senha_hash, papel, ativo)
             VALUES($1,'Administrador',$2,NULL,$3,'admin',TRUE)`,
            [id, emailAdmin, hashSenha(SENHA)]
          );
        } catch(e) { console.error('bootstrap admin:', e.message); }
        adm = (await pool.query(`SELECT * FROM usuarios_v2 WHERE papel='admin' AND ativo=TRUE ORDER BY criado_em ASC LIMIT 1`)).rows[0];
      }
      if (adm) {
        const sid = gerarSession(adm);
        res.setHeader('Set-Cookie', `vm_session=${sid}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400`);
        return res.json({ ok: true });
      }
    }

    return res.status(401).json({ error: 'Email ou senha incorretos' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/logout', (req, res) => {
  const m = (req.headers.cookie||'').match(/vm_session=([a-f0-9]+)/);
  if (m) SESSIONS.delete(m[1]);
  res.setHeader('Set-Cookie', 'vm_session=; Path=/; Max-Age=0');
  res.redirect('/login');
});

// Quem sou eu (para o front saber papel e permissões)
app.get('/api/me', (req, res) => {
  const s = sessionAtual(req);
  if (!s) return res.status(401).json({ error: 'Não autenticado' });
  const perm = PAPEIS[s.papel] || PAPEIS.leitura;
  res.json({ nome: s.nome, email: s.email, papel: s.papel, permissoes: perm });
});

// ── Gestão de usuários (somente admin) ────────────────────────────────────────
app.get('/api/usuarios', exigirAdmin, async (req, res) => {
  try {
    const r = await pool.query(`SELECT id, nome, email, whatsapp, papel, ativo, criado_em FROM usuarios_v2 ORDER BY criado_em ASC`);
    res.json({ usuarios: r.rows, papeis: PAPEIS });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/usuarios', exigirAdmin, async (req, res) => {
  try {
    const { nome, email, whatsapp, senha, papel } = req.body;
    if (!nome || !email || !senha) return res.status(400).json({ error: 'Nome, email e senha são obrigatórios' });
    if (!PAPEIS[papel]) return res.status(400).json({ error: 'Papel inválido' });
    const emailL = email.toLowerCase().trim();
    const existe = (await pool.query(`SELECT 1 FROM usuarios_v2 WHERE email=$1`, [emailL])).rows[0];
    if (existe) return res.status(409).json({ error: 'Já existe um usuário com esse email' });
    const id = crypto.randomBytes(8).toString('hex');
    await pool.query(
      `INSERT INTO usuarios_v2(id, nome, email, whatsapp, senha_hash, papel, ativo) VALUES($1,$2,$3,$4,$5,$6,TRUE)`,
      [id, nome.trim(), emailL, (whatsapp||'').trim()||null, hashSenha(senha), papel]
    );
    res.json({ ok: true, id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/usuarios/:id', exigirAdmin, async (req, res) => {
  try {
    const { nome, email, whatsapp, papel, ativo } = req.body;
    if (papel && !PAPEIS[papel]) return res.status(400).json({ error: 'Papel inválido' });
    const u = (await pool.query(`SELECT * FROM usuarios_v2 WHERE id=$1`, [req.params.id])).rows[0];
    if (!u) return res.status(404).json({ error: 'Usuário não encontrado' });
    // Impede remover o último admin ativo
    if (u.papel === 'admin' && (papel && papel !== 'admin' || ativo === false)) {
      const nAdmins = (await pool.query(`SELECT COUNT(*)::int AS n FROM usuarios_v2 WHERE papel='admin' AND ativo=TRUE`)).rows[0].n;
      if (nAdmins <= 1) return res.status(400).json({ error: 'Não é possível alterar o último administrador ativo' });
    }
    const emailL = email ? email.toLowerCase().trim() : u.email;
    if (emailL !== u.email) {
      const existe = (await pool.query(`SELECT 1 FROM usuarios_v2 WHERE email=$1 AND id<>$2`, [emailL, u.id])).rows[0];
      if (existe) return res.status(409).json({ error: 'Já existe um usuário com esse email' });
    }
    await pool.query(
      `UPDATE usuarios_v2 SET nome=$2, email=$3, whatsapp=$4, papel=$5, ativo=$6 WHERE id=$1`,
      [u.id, (nome||u.nome).trim(), emailL, (whatsapp!==undefined?whatsapp:u.whatsapp)||null, papel||u.papel, ativo!==undefined?ativo:u.ativo]
    );
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Admin redefine a senha de um usuário direto
app.post('/api/usuarios/:id/senha', exigirAdmin, async (req, res) => {
  try {
    const { senha } = req.body;
    if (!senha || senha.length < 4) return res.status(400).json({ error: 'A senha deve ter ao menos 4 caracteres' });
    const u = (await pool.query(`SELECT * FROM usuarios_v2 WHERE id=$1`, [req.params.id])).rows[0];
    if (!u) return res.status(404).json({ error: 'Usuário não encontrado' });
    await pool.query(`UPDATE usuarios_v2 SET senha_hash=$2 WHERE id=$1`, [u.id, hashSenha(senha)]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/usuarios/:id', exigirAdmin, async (req, res) => {
  try {
    const u = (await pool.query(`SELECT * FROM usuarios_v2 WHERE id=$1`, [req.params.id])).rows[0];
    if (!u) return res.status(404).json({ error: 'Usuário não encontrado' });
    if (u.papel === 'admin') {
      const nAdmins = (await pool.query(`SELECT COUNT(*)::int AS n FROM usuarios_v2 WHERE papel='admin' AND ativo=TRUE`)).rows[0].n;
      if (nAdmins <= 1) return res.status(400).json({ error: 'Não é possível excluir o último administrador' });
    }
    await pool.query(`DELETE FROM usuarios_v2 WHERE id=$1`, [u.id]);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
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
// Apelidos de produtos: nomes diferentes que devem ser tratados como o MESMO produto.
// Cada grupo lista todas as formas equivalentes (em minúsculas, sem acento).
// Para adicionar um novo apelido, basta incluir uma nova linha com as variações.
const APELIDOS_PRODUTOS = [
  ['kit porta look', 'kit organizadores porta look'],
  ['kit cristal', 'kit organizadores cristal'],
  ['kit documentos', 'kit porta documentos'],
];
// Verifica se dois nomes são equivalentes segundo a tabela de apelidos.
function _normApelido(s) {
  return (s||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/[^a-z0-9]+/g,' ').replace(/\s+/g,' ').trim();
}
function saoEquivalentesPorApelido(a, b) {
  const na = _normApelido(a), nb = _normApelido(b);
  if (!na || !nb) return false;
  // Palavras "vazias" que não distinguem produto (não contam no casamento)
  const stop = new Set(['kit','de','da','do','e','ella','cafe','bege','preto','marinho','caramelo','rose']);
  const palavrasSignificativas = nome => _normApelido(nome).split(' ').filter(w => w.length>2 && !stop.has(w));
  // Um nome "casa" com um apelido do grupo se TODAS as palavras significativas do apelido
  // estão presentes no nome (em qualquer ordem). Evita casar por fragmento solto.
  const nomeCasaApelido = (nome, apelido) => {
    const pa = palavrasSignificativas(apelido);
    if (!pa.length) return false;
    const pn = _normApelido(nome).split(' ');
    return pa.every(w => pn.includes(w));
  };
  return APELIDOS_PRODUTOS.some(grupo => {
    const baterA = grupo.some(g => nomeCasaApelido(a, g));
    const baterB = grupo.some(g => nomeCasaApelido(b, g));
    return baterA && baterB;
  });
}

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
  { chave:'Necessaire', termo:'nécessaire' },
  { chave:'Necessaire', termo:'necesaire' },
  { chave:'Porta Documentos', termo:'porta documento' },
  { chave:'Porta Documentos', termo:'documento' },
  { chave:'Porta Look', termo:'porta look' },
  { chave:'Porta Look', termo:'organizadores porta look' },
  { chave:'Porta Look', termo:'kit porta look' },
  { chave:'Saquinho', termo:'kit saquinho' },
  { chave:'Saquinho', termo:'saquinhos' },
  { chave:'Necessaire', termo:'kit necessaire' },
  { chave:'Necessaire', termo:'kit de necessaire' },
  { chave:'Necessaire', termo:'kit nécessaire' },
  { chave:'Necessaire', termo:'kit de nécessaire' },
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

// Normaliza todas as variações de Necessaire para: Necessaire P / M / G ou Kit Necessaire.
// Aceita "necessaire"/"necessarie"/"nécessaire" (com/sem acento e com/sem i),
// tamanhos por letra (P/M/G) ou por extenso (pequena/media/grande).
function extrairModeloNecessaire(title) {
  // tira acentos para comparar
  const t = (title||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  // precisa conter necessaire/necessarie (qualquer grafia com "necess")
  if (!t.includes('necess')) return null;
  // Kit de necessaires (completo) — "kit necessaire", "kit de necessaires"
  if (/kit\s*(de\s*)?necess/.test(t)) return 'Kit Necessaire';
  // Tamanhos: por extenso ou por letra isolada
  if (/pequena|\bp\b/.test(t)) return 'Necessaire P';
  if (/media|\bm\b/.test(t)) return 'Necessaire M';
  if (/grande|\bg\b/.test(t)) return 'Necessaire G';
  // Sem tamanho identificado: trata como Necessaire genérica
  return 'Necessaire';
}

function extrairModeloBase(title) {
  if (!title) return 'Outros';
  const t = title.toLowerCase();
  // Unifica todas as variações de documentos (porta documentos, kit documentos, etc.)
  if (t.includes('documento')) return 'Porta Documentos';
  if (t.includes('cristal')) { const c = extrairModeloCristal(title); if (c) return c; }
  if (t.normalize('NFD').replace(/[\u0300-\u036f]/g,'').includes('necess')) { const n = extrairModeloNecessaire(title); if (n) return n; }
  for (const m of MODELOS_MAP) { if (t.includes(m.termo)) return m.chave; }
  return title.replace(/\b(bolsa|mochila|mala|maternidade|ella|urban chic|nós|nos|origem|le petit|tressê palha|bege|preto|marinho|caramelo|café|cafe|cinza|bordô|bordo|off white|rosé|rose|marrom|verde|nude|vinho|rosa|azul|preta)\b/gi,'').replace(/\s+/g,' ').trim() || title;
}

function extrairCorDoTitulo(title) {
  const cols = ['Linho','Ella','Urban Chic','Nós','Origem','Le Petit','Tressê Palha','Glam'];
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
  const tl = title.toLowerCase();
  // Um kit "de verdade" traz uma LISTA de peças (vírgulas ou " e " separando vários produtos).
  const temListaDePecas = /,/.test(tl) || /\se\s/.test(tl);
  // Kit Porta Documentos / Kit Documentos sozinho é um único produto — não quebrar.
  // Mas se vier dentro de um kit com lista de peças, deve quebrar normalmente.
  if (!temListaDePecas && tl.includes('documento')) return [item];
  // Kits de SEO que são UM produto só (a palavra "organizadores" é só do site, não é peça):
  // "Kit Organizadores Cristal", "Kit Organizadores Porta Look" → não quebrar.
  // MAS: se o título traz uma LISTA de peças, é um kit de verdade com vários produtos → quebrar.
  if (!temListaDePecas && (
      tl.includes('organizadores cristal') || tl.includes('organizador cristal') ||
      tl.includes('organizadores porta look') || tl.includes('organizador porta look') ||
      tl.includes('kit cristal') || tl.includes('kit porta look'))) {
    return [item];
  }
  if (!title.toLowerCase().includes('kit')) return [item];
  const parteDesc = title.replace(/^kit\s+[\w\sÀ-ú]+[-:]/i, '') || title;
  const partes = parteDesc.split(/,|\se\s/i).map(p => p.trim()).filter(p => p.length > 2);
  const itens = [];
  for (const parte of partes) {
    // Cristal é tratado por função própria (não está no MODELOS_MAP)
    const cristal = extrairModeloCristal(parte);
    if (cristal) {
      itens.push({ ...item, title: cristal + ' ' + extrairCorDoTitulo(title), isKitItem: true, kitOriginal: title });
      continue;
    }
    // Necessaire também tem função própria (normaliza tamanho/kit)
    const necessaire = extrairModeloNecessaire(parte);
    if (necessaire) {
      itens.push({ ...item, title: necessaire + ' ' + extrairCorDoTitulo(title), isKitItem: true, kitOriginal: title });
      continue;
    }
    const m = MODELOS_MAP.find(m => parte.toLowerCase().includes(m.termo));
    if (m) itens.push({ ...item, title: m.chave + ' ' + extrairCorDoTitulo(title), isKitItem: true, kitOriginal: title });
  }
  return itens.length > 0 ? itens : [item];
}

function limparVendedora(nome) {
  return nome ? nome.replace(/^(CONSULTORA|CONSULTOR|VENDEDORA|VENDEDOR|ATENDENTE|REP|REPRESENTANTE)\s+/i, '').trim() : null;
}

// Expande um item com quantidade > 1 em várias unidades de quantidade 1,
// para que cada peça vire uma linha própria (e possa ter seu próprio bordado/status).
function expandirQuantidade(item) {
  const qtd = parseInt(item.quantity, 10) || 1;
  if (qtd <= 1) return [item];
  const unidades = [];
  for (let k = 0; k < qtd; k++) {
    unidades.push({ ...item, quantity: 1, _unidade: k + 1, _totalUnidades: qtd });
  }
  return unidades;
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

  // Acumula bordados do mesmo modelo como LISTA (ex: 3 pingentes -> [Joaquim, EVA, Rizzo]),
  // para depois distribuir um por unidade na ordem em que aparecem.
  const semBordadoRe = /^(sem\s*bordado|s\/bordado|sem|s\/|s\/b)$/i;
  const addBordado = (key, val) => {
    const limpo = (val == null || semBordadoRe.test(String(val).trim())) ? null : String(val).trim();
    if (!Array.isArray(bordadosPorModelo[key])) bordadosPorModelo[key] = [];
    if (limpo == null) return; // "sem bordado" não adiciona nome, só garante a chave
    // Não duplica valor idêntico consecutivo
    const jaTem = bordadosPorModelo[key].map(s=>s.toLowerCase());
    if (!jaTem.includes(limpo.toLowerCase())) bordadosPorModelo[key].push(limpo);
  };

  for (let line of lines) {
    // Remove hífen ou traço no início da linha (ex: "-Mala Rodinha: BORDADO L")
    line = line.replace(/^[-–•*]\s*/, '').trim();
    if (!line) continue;
    if (line.match(/^(PRONTA\s*ENTREGA|PE)$/i)) { isProntaEntrega = true; continue; }
    const mPed = line.match(/^PEDIDO[-:\s]+([A-ZÀ-Úa-zà-ú\s]+)$/i);
    if (mPed && !vendedora) { vendedora = limparVendedora(mPed[1].trim()); continue; }
    const mData = line.match(/^DATA\s+(?:DE\s+)?ENVIO[:\s]+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (mData) { dataEnvio = normalizarData(mData[1]); continue; }
    const m1 = line.match(/^([A-ZÀ-Úa-zà-ú\s]+?)\s*[-–]\s*(?:ENVIO|ENV|ENTREGA|ENVIAR)\s+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1) { vendedora = limparVendedora(m1[1].trim()); dataEnvio = normalizarData(m1[2]); continue; }

    // Vendedora sem data: "NOME - ENVIO ..." ou "NOME - ENTREGA ..." (ex: "LAURA - ENVIO PRONTA ENTREGA").
    // Captura o NOME como vendedora mesmo quando não há data na sequência.
    const m1c = line.match(/^([A-ZÀ-Úa-zà-ú\s]+?)\s*[-–]\s*(?:ENVIO|ENV|ENTREGA|ENVIAR)\b/i);
    if (m1c && !vendedora) {
      const cand = limparVendedora(m1c[1].trim());
      const candLow = (cand||'').toLowerCase().trim();
      // Não captura se o "nome" for na verdade um modelo ou palavra de controle (pronta/entrega/envio)
      const ehModelo = cand && ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','frasqueira','pingente','kate','liz','cleo','cloé','cristal','alça','alca','laço','laco','capa','organizador','saquinho','documento','rodinha'].some(k => candLow.includes(k));
      const ehControle = ['pronta','entrega','envio','enviar','env','pronta entrega','pronta-entrega'].includes(candLow);
      if (cand && !ehModelo && !ehControle) { vendedora = cand; continue; }
    }

    // Padrão: MODELO - BORDADO X (ex: "CRISTAL PP - BORDADO LA")
    const mBordModelo = line.match(/^([A-ZÀ-Úa-zà-ú\s0-9]+?)\s*[-–]\s*(?:BORDADO|INICIAL|NOME)[:\s]+(.+)/i);
    if (mBordModelo) {
      const key = mBordModelo[1].trim().toLowerCase();
      const val = mBordModelo[2].trim().replace(/^BORDADO[:\s]+/i,'').trim();
      const isModelo = MODELOS_MAP.some(m => key.includes(m.termo)) ||
        ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','alça','alca','frasqueira','pingente','kate','liz','cleo','cloé','cristal'].some(k => key.includes(k));
      if (isModelo) { addBordado(key, val); continue; }
    }

    // Formato solto: "MODELO na alça NOME" (personalização de alça, vira "Alça: NOME")
    const mAlca = line.match(/^([A-ZÀ-Úa-zà-ú\s0-9]+?)\s*[-–]?\s*na\s+al[çc]a\s+(.+)/i);
    if (mAlca) {
      const key = mAlca[1].trim().toLowerCase();
      const isModelo = MODELOS_MAP.some(m => key.includes(m.termo)) ||
        ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','frasqueira','pingente','kate','liz','cleo','cloé','cristal','documento','rodinha'].some(k => key.includes(k));
      if (isModelo) { addBordado(key, 'Alça: ' + mAlca[2].trim()); continue; }
    }

    // Formato solto: "MODELO BORDADO VALOR" sem separador (ex: "Mala Rodinha BORDADO LA")
    const mBordSolto = line.match(/^([A-ZÀ-Úa-zà-ú\s0-9]+?)\s+BORDADO\s+(.+)/i);
    if (mBordSolto) {
      const key = mBordSolto[1].trim().toLowerCase().replace(/^\d+\s+/, ''); // remove "1 " do início
      const val = mBordSolto[2].trim();
      const isModelo = MODELOS_MAP.some(m => key.includes(m.termo)) ||
        ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','frasqueira','pingente','kate','liz','cleo','cloé','cristal','documento','rodinha'].some(k => key.includes(k));
      if (isModelo) { addBordado(key, val); continue; }
    }
    const m1b = line.match(/^(?:ENVIO|ENV|ENTREGA|ENVIAR)[:\s]+(?:DIA\s+)?(\d{1,2}[\/.\-]\d{1,2}(?:[\/.\-]\d{2,4})?)/i);
    if (m1b && !dataEnvio) { dataEnvio = normalizarData(m1b[1]); continue; }
    const m2 = line.match(/^([A-ZÀ-Úa-zà-ú\s0-9]+?):\s*(?:BORDADO[:\s]+)?(.+)/i);
    if (m2) {
      const key = m2[1].trim().toLowerCase();
      // Remove "BORDADO:" do início do valor se vier duplicado
      let val = m2[2].trim().replace(/^BORDADO[:\s]+/i, '').trim();
      const isModelo = MODELOS_MAP.some(m => key.includes(m.termo)) ||
        ['mala','mochila','bolsa','madison','louise','trocador','necessaire','porta','kit','alça','alca','frasqueira','pingente','kate','liz','cleo','cloé','cristal'].some(k => key.includes(k));
      if (isModelo) { addBordado(key, val); continue; }
    }
    const m3 = line.match(/^BORDADO[:\s\-–]+(.+)/i);
    if (m3) { const v=m3[1].trim(); bordadoGeral = v.match(/^(sem\s*bordado|s\/bordado|sem|s\/|s\/b)$/i) ? null : v; continue; }
    const m4 = line.match(/NOME\s+AL[CÇ]A[:\s]+(.+)/i);
    if (m4) { bordadoGeral = m4[1].trim(); continue; }
    const m5 = line.match(/^NOME[:\s]+(.+)/i);
    if (m5 && !bordadoGeral) { bordadoGeral = m5[1].trim(); continue; }
    if (line.match(/^COLE[ÇC][ÃA]O\s+/i)) continue;
    if (line.match(/^BRINDE/i)) continue;
    if (line.length > 2) extras.push(line);
  }
  const isUrgente = note.match(/urgente/i) !== null;
  const isPrioridade = note.match(/prioridade/i) !== null;
  // Detecta "pronta entrega" em qualquer lugar do texto (não só numa linha isolada).
  // Cobre formatos como "LAURA - ENVIO PRONTA ENTREGA", "envio: pronta entrega", etc.
  if (!isProntaEntrega && note.match(/pronta[\s\-]*entrega/i)) isProntaEntrega = true;
  return { vendedora, dataEnvio, bordadoGeral, bordadosPorModelo, obsCliente: extras.join(' | ')||null, isProntaEntrega, isUrgente, isPrioridade };
}

function getBordado(obs, modeloBase, unidade) {
  if (!obs) return null;
  const mb = (modeloBase||'').toLowerCase();
  const bpm = obs.bordadosPorModelo || {};

  // Encontra a LISTA de bordados que corresponde a este modelo
  let lista = null;
  const removerAcentos = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const mbSemAcento = removerAcentos(mb);

  // 1) Exato
  for (const [key, val] of Object.entries(bpm)) { if (mb === key) { lista = val; break; } }
  // 2) Inclusão significativa
  if (lista === null) for (const [key, val] of Object.entries(bpm)) {
    if (key.length >= 4 && (mb.includes(key) || key.includes(mb))) { lista = val; break; }
  }
  // 3) Sem acento
  if (lista === null) for (const [key, val] of Object.entries(bpm)) {
    const keySemAcento = removerAcentos(key);
    if (mbSemAcento.includes(keySemAcento) || keySemAcento.includes(mbSemAcento)) { lista = val; break; }
  }
  // 4) Aliases
  if (lista === null) {
    const aliases = {
      'madison': ['bolsa madison','madison mini','bolsa maternidade madison'],
      'mala de rodinhas': ['mala rodinha','mala rodinhas','mala de rodinha'],
      'necessaire': ['kit de necessaire','kit necessaire','kit de nécessaire','kit nécessaire'],
      'kit cristal': ['kit cristal','cristal kit'],
      'bolsa cleo': ['cleo','bolsa cleo'],
      'porta look': ['kit porta look','porta look'],
      'trocador': ['trocador portátil','trocador portatil'],
      'kit documentos': ['kit de documentos','kit documentos','kit documento','porta documentos','porta documento','documento'],
    };
    for (const [modelo, alts] of Object.entries(aliases)) {
      if (mb.includes(modelo) || alts.some(a => mb.includes(a))) {
        for (const [key, val] of Object.entries(bpm)) {
          if (alts.some(a => key.includes(a)) || key.includes(modelo)) { lista = val; break; }
        }
      }
      if (lista !== null) break;
    }
  }
  // 5) Apelidos de produto (kits)
  if (lista === null) for (const [key, val] of Object.entries(bpm)) {
    if (saoEquivalentesPorApelido(mb, key)) { lista = val; break; }
  }

  // Normaliza para array (retrocompatibilidade caso venha string)
  if (lista == null) return obs.bordadoGeral || null;
  const arr = Array.isArray(lista) ? lista : [lista];
  if (!arr.length) return null;

  // Distribui por unidade: 1ª peça pega o 1º bordado, 2ª o 2º, etc.
  const u = parseInt(unidade, 10);
  if (u && u >= 1) {
    if (u <= arr.length) return arr[u - 1];
    // Mais peças do que bordados informados: as peças extras ficam sem bordado
    return null;
  }
  // Sem unidade (item único): se há só um bordado, retorna ele; se há vários, junta para visão geral
  return arr.length === 1 ? arr[0] : arr.join(', ');
}

// Distribui os bordados entre as peças do MESMO modelo dentro de um pedido.
// Ex: 3 pingentes + observação "Joaquim / EVA / Rizzo" -> 1ª=Joaquim, 2ª=EVA, 3ª=Rizzo.
// Numera por posição entre peças do mesmo modelo (robusto mesmo com linhas e quantidades misturadas).
// Respeita bordados editados manualmente (não sobrescreve quem tem override).
// Processa "Alça Personalizada": associa cada alça do pedido à bolsa citada na observação.
// Observação no padrão: "Alça [Bolsa] - BORDADO: [nome]" (ex: "Alça Madison - BORDADO: Cecília").
// A alça vira "Alça [Bolsa]", herda a COR da bolsa correspondente no pedido (se houver),
// e recebe o bordado escrito. A associação é por ORDEM (1ª alça -> 1ª linha de alça da obs).
function processarAlcasPersonalizadas(order, itensPedido) {
  // Itens que são alça (modeloBase "Alça"), na ordem em que aparecem
  const alcas = itensPedido.filter(it => (it.modeloBase||'').toLowerCase() === 'alça' || (it.modeloBase||'').toLowerCase() === 'alca');
  if (!alcas.length) return;

  // Bolsas que podem ter alça personalizada
  const BOLSAS_ALCA = ['madison mini','madison','madeleine','louise mini','louise','frasqueira'];

  // Lê as linhas de alça da observação, na ordem
  const note = order.note || '';
  const linhas = note.split(/\r?\n/).map(l => l.trim()).filter(Boolean);
  const semAcento = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const linhasAlca = [];
  for (const linha of linhas) {
    const ln = semAcento(linha);
    if (!ln.startsWith('alca')) continue; // só linhas que começam com "Alça"
    // Qual bolsa? (testa nomes mais específicos primeiro: "madison mini" antes de "madison")
    let bolsa = null;
    for (const b of BOLSAS_ALCA) { if (ln.includes(semAcento(b))) { bolsa = b; break; } }
    // Bordado: o que vem depois de "BORDADO:" ou "BORDADO -"
    let bordado = null;
    const mB = linha.match(/bordado\s*[:\-]\s*(.+)$/i);
    if (mB) bordado = mB[1].trim();
    linhasAlca.push({ bolsa, bordado });
  }
  if (!linhasAlca.length) return;

  // Mapa de cor por bolsa, a partir das bolsas presentes no pedido
  const corPorBolsa = {};
  for (const it of itensPedido) {
    const mbl = semAcento(it.modeloBase||'');
    for (const b of BOLSAS_ALCA) {
      if (mbl === semAcento(b) && it.colecaoCor) { corPorBolsa[b] = corPorBolsa[b] || it.colecaoCor; }
    }
  }

  // Associa cada alça do pedido (na ordem) com a linha de alça correspondente (na ordem)
  alcas.forEach((it, i) => {
    if (it.foiEditado) return; // respeita edição manual
    const info = linhasAlca[i];
    if (!info) return;
    if (info.bolsa) {
      const nomeBolsa = info.bolsa.split(' ').map(w => w.charAt(0).toUpperCase()+w.slice(1)).join(' ');
      it.modeloBase = 'Alça ' + nomeBolsa;
      it.colecaoCor = corPorBolsa[info.bolsa] || '';
    }
    if (info.bordado) it.bordado = info.bordado;
  });
}

function distribuirBordadosPorModelo(order, itensPedido) {
  const obs = parseObs(order.note);
  const bpm = obs.bordadosPorModelo || {};
  // Agrupa itens por modeloBase, na ordem em que aparecem
  const porModelo = {};
  for (const it of itensPedido) {
    const k = (it.modeloBase||'').toLowerCase();
    (porModelo[k] = porModelo[k] || []).push(it);
  }
  for (const [modeloKey, itens] of Object.entries(porModelo)) {
    // Acha a lista de bordados desse modelo (reusa a lógica do getBordado por unidade)
    // Se só há 1 peça, mantém o que o getBordado já resolveu (não força).
    if (itens.length <= 1) continue;
    // Descobre a lista de bordados disponível para o modelo
    const exemplo = getBordadoLista(obs, modeloKey);
    if (!exemplo || !exemplo.length) continue;
    let pos = 0;
    for (const it of itens) {
      // Não mexe em quem foi editado manualmente
      if (it.foiEditado) { continue; }
      it.bordado = pos < exemplo.length ? exemplo[pos] : null;
      pos++;
    }
  }
}

// Retorna apenas a LISTA de bordados (array) que casa com o modelo, sem escolher unidade.
function getBordadoLista(obs, modeloBase) {
  const mb = (modeloBase||'').toLowerCase();
  const bpm = obs.bordadosPorModelo || {};
  const removerAcentos = s => s.normalize('NFD').replace(/[\u0300-\u036f]/g,'');
  const mbSemAcento = removerAcentos(mb);
  let lista = null;
  for (const [key, val] of Object.entries(bpm)) { if (mb === key) { lista = val; break; } }
  if (lista === null) for (const [key, val] of Object.entries(bpm)) {
    if (key.length >= 4 && (mb.includes(key) || key.includes(mb))) { lista = val; break; }
  }
  if (lista === null) for (const [key, val] of Object.entries(bpm)) {
    const keySemAcento = removerAcentos(key);
    if (mbSemAcento.includes(keySemAcento) || keySemAcento.includes(mbSemAcento)) { lista = val; break; }
  }
  if (lista === null) for (const [key, val] of Object.entries(bpm)) {
    if (saoEquivalentesPorApelido(mb, key)) { lista = val; break; }
  }
  if (lista == null) return null;
  return Array.isArray(lista) ? lista : [lista];
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
      // Data de envio já passou e o pedido NÃO foi processado na Shopify → está atrasado.
      // (O "enviado" só vem do fulfillment real da Shopify ou da tag manual, acima.)
      if (!isNaN(d) && d < new Date()) {
        return 'atrasado';
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

function mapItem(order, item, isDraft, listasCache, idx) {
  const obs = parseObs(order.note);
  const modeloBase = extrairModeloBase(item.title);
  const dataEnvio = obs.dataEnvio || '—';

  // ID único do item
  const itemId = (isDraft ? 'D-' : '') + String(order.id) + '__' + String(item.id || 'li') + '__' + String(idx == null ? 0 : idx);

  // Lista que contém ESTE item (por itemId)
  const lista = (listasCache||[]).find(l => l.pedidoIds && l.pedidoIds.some(id => String(id) === String(itemId)));

  // Overrides: por item tem prioridade; cai no de pedido só como retaguarda (edições antigas)
  const ovItem = (listasCache||[])._overridesItem?.[itemId] || null;
  const ovPed = (listasCache||[])._overrides?.[String(order.id)] || null;
  const pick = (campo) => {
    if (ovItem && ovItem[campo] !== undefined && ovItem[campo] !== null) return ovItem[campo];
    return undefined;
  };
  let modeloFinal = pick('modelo_override') || modeloBase;
  // Normaliza nomes antigos de Necessaire/Cristal mesmo quando vieram de edição manual antiga,
  // para o catálogo ficar uniforme (Necessaire P/M/G/Kit). Só age se o texto contém esses termos.
  if (modeloFinal) {
    const mfLow = modeloFinal.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
    if (mfLow.includes('necess')) { const n = extrairModeloNecessaire(modeloFinal); if (n) modeloFinal = n; }
    else if (mfLow.includes('cristal')) { const c = extrairModeloCristal(modeloFinal); if (c) modeloFinal = c; }
  }
  const colecaoCorFinal = pick('colecao_cor_override') || extrairColecaoCor(item.title, item.variant_title);
  const bordadoFinal = (ovItem && ovItem.bordado_override !== undefined && ovItem.bordado_override !== null)
    ? ovItem.bordado_override : getBordado(obs, modeloBase, item._unidade);
  const dataEnvioFinal = pick('data_envio_override') || dataEnvio;
  const vendedoraFinal = pick('vendedora_override') || obs.vendedora || '—';
  const foiEditado = !!ovItem;

  // Status base (calculado das tags da Shopify)
  let statusFinal = calcStatus(order.tags, dataEnvio, obs.isProntaEntrega, order.fulfillment_status);
  // Status de produção por item (banco) sobrepõe — mas nunca rebaixa quem já saiu/pronto
  const statusItem = (listasCache||[])._statusItens?.[itemId];
  if (statusItem && !['enviado','pronto','pronta_entrega'].includes(statusFinal)) {
    statusFinal = statusItem;
  }
  // Status definido manualmente na edição do pedido tem prioridade sobre tudo
  if (ovItem && ovItem.status_override) {
    statusFinal = ovItem.status_override;
  }

  return {
    id: isDraft ? 'D-'+order.id : order.id,
    orderId: String(order.id),
    itemId: itemId,
    numero: isDraft ? '#D-'+String(order.id).slice(-4) : '#'+order.order_number,
    cliente: order.customer ? `${order.customer.first_name||''} ${order.customer.last_name||''}`.trim() : '—',
    email: order.customer?.email || '—',
    dataPedido: fmtDate(order.created_at),
    dataEnvio: dataEnvioFinal, diasUteisRestantes: diasUteisAte(dataEnvioFinal),
    vendedora: vendedoraFinal,
    modeloBase: modeloFinal, modelo: (ovItem && ovItem.modelo_override) || item.title,
    colecaoCor: colecaoCorFinal,
    bordado: bordadoFinal,
    foiEditado: foiEditado,
    obsCliente: obs.obsCliente || '—',
    obsInterna: (ovItem && ovItem.obs_interna) || '',
    noteRaw: order.note || '',
    status: statusFinal,
    isUrgente: obs.isUrgente || (order.tags||'').toLowerCase().includes('urgente'),
    isPrioridade: obs.isPrioridade || (order.tags||'').toLowerCase().includes('prioridade'),
    isDraft, isKitItem: item.isKitItem||false, kitOriginal: item.kitOriginal||null,
    tags: order.tags||'', quantidade: item.quantity||1,
    unidade: item._unidade || null, totalUnidades: item._totalUnidades || null,
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
  let url = `https://${SHOP}/admin/api/2024-01/orders.json?status=any&limit=250${dp}&fields=id,order_number,created_at,note,tags,line_items,fulfillment_status,financial_status,customer`;
  while (url) {
    const r = await fetch(url, { headers: shopHeaders(token) });
    if (!r.ok) {
      // Não derruba o sistema inteiro: registra e para a paginação com o que já tem.
      const corpo = await r.text().catch(()=> '');
      console.error(`Shopify retornou ${r.status} ao buscar pedidos:`, corpo.slice(0,300));
      if (allOrders.length === 0) throw new Error(`Shopify ${r.status}`);
      break;
    }
    const d = await r.json();
    allOrders = allOrders.concat(d.orders||[]);
    const link = r.headers.get('link')||'';
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
  }

  // Só vão para produção pedidos PAGOS (inclui pagamento parcial).
  // Exclui não pagos/expirados (pending, expired, voided, refunded, etc.) e cancelados.
  const STATUS_PAGOS = ['paid', 'partially_paid', 'partially_refunded'];
  allOrders = allOrders.filter(o => {
    if (o.cancelled_at) return false; // pedido cancelado nunca entra
    const fs = (o.financial_status || '').toLowerCase();
    return STATUS_PAGOS.includes(fs);
  });

  // Carrega listas e overrides UMA VEZ para todos os pedidos
  const listasCache = await loadListas();
  try {
    const ovR = await pool.query(`SELECT * FROM pedidos_editados`);
    listasCache._overrides = {};
    for (const ov of ovR.rows) listasCache._overrides[ov.order_id] = ov;
  } catch(e) { listasCache._overrides = {}; }
  // Overrides por item (cada item editado individualmente)
  try {
    const ovItem = await pool.query(`SELECT * FROM itens_editados`);
    listasCache._overridesItem = {};
    for (const ov of ovItem.rows) listasCache._overridesItem[ov.item_id] = ov;
  } catch(e) { listasCache._overridesItem = {}; }
  // Status de produção por item (banco) — sobrevive às sincronizações
  listasCache._statusItens = await loadStatusItens();

  let lista = [];
  for (const o of allOrders) {
    let __idx = 0;
    const itensPedido = [];
    for (const i of o.line_items) {
      for (const ei of expandirKit(i)) {
        for (const u of expandirQuantidade(ei)) {
          itensPedido.push(mapItem(o, u, false, listasCache, __idx++));
        }
      }
    }
    distribuirBordadosPorModelo(o, itensPedido);
    processarAlcasPersonalizadas(o, itensPedido);
    lista.push(...itensPedido);
  }

  // Rascunhos (draft orders) NÃO vão para produção — são orçamentos não pagos.
  // (Desativado por decisão: só pedido pago real entra. Para reativar, troque "false" por "true".)
  if (false) {
   try {
    const dr = await fetch(`https://${SHOP}/admin/api/2024-01/draft_orders.json?status=open&limit=250`, { headers: shopHeaders(token) });
    if (dr.ok) {
      const dd = await dr.json();
      for (const o of dd.draft_orders||[]) {
        let __idx = 0;
        const itensPedido = [];
        for (const i of o.line_items) {
          for (const ei of expandirKit(i)) {
            for (const u of expandirQuantidade(ei)) {
              itensPedido.push(mapItem(o, u, true, listasCache, __idx++));
            }
          }
        }
        distribuirBordadosPorModelo(o, itensPedido);
        lista.push(...itensPedido);
      }
    }
   } catch(e) {}
  }

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
  try {
    // force=true limpa o cache
    if (req.query.force === 'true') {
      CACHE_TS = 0;
      CACHE_PEDIDOS = [];
    }
    res.json({pedidos:await getPedidos(token,req.query)});
  }
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

// ── Integração com o sistema de estoque ───────────────────────────────────────
// Autenticada por token fixo no header: Authorization: Bearer <ESTOQUE_TOKEN>
// Retorna as listas em produção com itens agrupados por modelo+cor.
app.get('/api/listas-estoque', async (req, res) => {
  try {
    // 1) Autenticação por token (Bearer)
    if (!ESTOQUE_TOKEN) {
      return res.status(503).json({ error: 'Integração de estoque não configurada (defina ESTOQUE_TOKEN no ambiente)' });
    }
    const auth = req.headers.authorization || '';
    const m = auth.match(/^Bearer\s+(.+)$/i);
    const tokenRecebido = m ? m[1].trim() : '';
    // Comparação em tempo constante para evitar ataques de temporização
    const a = Buffer.from(tokenRecebido);
    const b = Buffer.from(ESTOQUE_TOKEN);
    const tokenOk = a.length === b.length && crypto.timingSafeEqual(a, b);
    if (!tokenOk) return res.status(401).json({ error: 'Token inválido' });

    // 2) Carrega listas em produção
    const todas = await loadListas();
    const emProducao = todas.filter(l => (l.status || 'em_producao') === 'em_producao');

    // 3) Garante o cache de pedidos para cruzar itemId -> modelo/cor
    let pedidos = CACHE_PEDIDOS;
    if (!pedidos || !pedidos.length) {
      try {
        const token = await loadToken();
        if (token) { pedidos = await getPedidos(token); }
      } catch(e) { pedidos = CACHE_PEDIDOS || []; }
    }
    const porItemId = {};
    (pedidos || []).forEach(p => { if (p && p.itemId != null) porItemId[String(p.itemId)] = p; });

    // 4) Monta cada lista com itens agrupados por modeloBase + colecaoCor
    const listas = emProducao.map(l => {
      const grupos = {}; // chave "modelo||cor" -> quantidade
      const ids = Array.isArray(l.pedidoIds) ? l.pedidoIds : [];
      ids.forEach(itemId => {
        const ped = porItemId[String(itemId)];
        let modeloBase, colecaoCor;
        if (ped) {
          modeloBase = ped.modeloBase || ped.modelo || 'Sem modelo';
          colecaoCor = ped.colecaoCor || 'Sem cor';
        } else {
          // Sem pedido vinculado no cache: usa o que houver em modelos da lista
          modeloBase = 'Sem modelo';
          colecaoCor = 'Sem cor';
        }
        const chave = modeloBase + '||' + colecaoCor;
        grupos[chave] = (grupos[chave] || 0) + 1;
      });

      // Se não conseguiu cruzar nenhum item (cache vazio), tenta usar o campo "modelos" da lista
      let itens = Object.entries(grupos).map(([chave, quantidade]) => {
        const [modeloBase, colecaoCor] = chave.split('||');
        return { modeloBase, colecaoCor, quantidade };
      });
      if (!itens.length && Array.isArray(l.modelos) && l.modelos.length) {
        // l.modelos pode ser uma lista de strings "Modelo · Cor" ou objetos
        const g2 = {};
        l.modelos.forEach(mItem => {
          let modeloBase = 'Sem modelo', colecaoCor = 'Sem cor';
          if (typeof mItem === 'string') {
            const partes = mItem.split('·').map(s => s.trim());
            modeloBase = partes[0] || 'Sem modelo';
            colecaoCor = partes[1] || 'Sem cor';
          } else if (mItem && typeof mItem === 'object') {
            modeloBase = mItem.modeloBase || mItem.modelo || 'Sem modelo';
            colecaoCor = mItem.colecaoCor || mItem.cor || 'Sem cor';
          }
          const chave = modeloBase + '||' + colecaoCor;
          g2[chave] = (g2[chave] || 0) + 1;
        });
        itens = Object.entries(g2).map(([chave, quantidade]) => {
          const [modeloBase, colecaoCor] = chave.split('||');
          return { modeloBase, colecaoCor, quantidade };
        });
      }

      return {
        id: l.id,
        numero: l.numero,
        nome: l.nome || ('Lista #' + l.numero),
        dataEnvio: l.dataEnvio || null,
        dataProducao: l.dataProducao || null,
        totalPecas: l.totalPecas || ids.length,
        status: l.status || 'em_producao',
        itens,
      };
    });

    res.json({ listas });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/listas', exigirEdicao, async (req,res) => {
  const { nome, pedidoIds, dataEnvio, totalPecas, modelos } = req.body;
  const numero = await proximoNumeroLista();
  const lista = { id:crypto.randomBytes(8).toString('hex'), numero, nome:nome||`Lista #${numero}`,
    pedidoIds:pedidoIds||[], dataEnvio:dataEnvio||null, dataProducao:null,
    totalPecas:totalPecas||0, modelos:modelos||[], status:'em_producao', criadaEm:new Date().toISOString() };
  await salvarLista(lista);
  // Marca cada item da lista como "em produção" no banco (por item, não por pedido)
  await marcarItensProducao(pedidoIds||[], numero, 'em_producao');
  CACHE_TS = 0; // invalida cache para atualizar status e listaNumero
  res.json({lista});
});

app.put('/api/listas/:id', exigirEdicao, async (req,res) => {
  await atualizarLista(req.params.id, req.body);
  const listas = await loadListas();
  res.json({lista:listas.find(l=>l.id===req.params.id)});
});

app.delete('/api/listas/:id', exigirEdicao, async (req,res) => {
  // Antes de excluir, libera os itens dessa lista (remove status de produção)
  try {
    const listas = await loadListas();
    const lista = listas.find(l => l.id === req.params.id);
    if (lista && Array.isArray(lista.pedidoIds) && lista.pedidoIds.length) {
      for (const itemId of lista.pedidoIds) {
        await pool.query(`DELETE FROM itens_producao WHERE item_id=$1`, [String(itemId)]);
      }
    }
  } catch(e) { console.error('liberar itens:', e.message); }
  await deletarLista(req.params.id);
  CACHE_TS = 0;
  res.json({ok:true});
});

// ── Pronta Entrega (listas PE com peças, estoque vendável individualmente) ────

// Lista todas as listas PE com suas peças
app.get('/api/listas-pe', async (req,res) => {
  try {
    const lr = await pool.query(`SELECT * FROM listas_pe ORDER BY criada_em DESC`);
    const pr = await pool.query(`SELECT * FROM pecas_pe ORDER BY criada_em ASC`);
    const listas = lr.rows.map(l => ({
      ...l,
      pecas: pr.rows.filter(p => p.lista_id === l.id),
    }));
    res.json({ listas });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Catálogo de estoque: só peças disponíveis (não vendidas), com busca
app.get('/api/pronta-entrega/estoque', async (req,res) => {
  try {
    const busca = (req.query.busca||'').toLowerCase().trim();
    const r = await pool.query(`SELECT * FROM pecas_pe WHERE vendida=FALSE ORDER BY modelo ASC, colecao_cor ASC`);
    let pecas = r.rows;
    if (busca) {
      pecas = pecas.filter(p =>
        (p.modelo||'').toLowerCase().includes(busca) ||
        (p.colecao_cor||'').toLowerCase().includes(busca) ||
        (p.bordado||'').toLowerCase().includes(busca)
      );
    }
    res.json({ pecas });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Cria uma lista PE (numeração na mesma sequência das listas de produção)
app.post('/api/listas-pe', exigirEdicao, async (req,res) => {
  try {
    const { modelo, dataEnvio } = req.body;
    if (!modelo || !modelo.trim()) return res.status(400).json({error:'Modelo é obrigatório'});
    const numero = await proximoNumeroLista();
    const id = crypto.randomBytes(8).toString('hex');
    await pool.query(
      `INSERT INTO listas_pe(id, numero, modelo, data_envio) VALUES($1,$2,$3,$4)`,
      [id, numero, modelo.trim(), dataEnvio||null]
    );
    res.json({ ok:true, id, numero });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Atualiza dados da lista PE (ex: data de produção)
app.put('/api/listas-pe/:id', exigirEdicao, async (req,res) => {
  try {
    const { dataProducao, dataEnvio, modelo } = req.body;
    const campos = [];
    const vals = [req.params.id];
    let i = 2;
    if (dataProducao !== undefined) { campos.push(`data_producao=$${i++}`); vals.push(dataProducao||null); }
    if (dataEnvio !== undefined) { campos.push(`data_envio=$${i++}`); vals.push(dataEnvio||null); }
    if (modelo !== undefined) { campos.push(`modelo=$${i++}`); vals.push(modelo); }
    if (!campos.length) return res.json({ok:true});
    await pool.query(`UPDATE listas_pe SET ${campos.join(', ')} WHERE id=$1`, vals);
    res.json({ ok:true });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Exclui uma lista PE (e suas peças)
app.delete('/api/listas-pe/:id', exigirEdicao, async (req,res) => {
  try {
    await pool.query(`DELETE FROM pecas_pe WHERE lista_id=$1`, [req.params.id]);
    await pool.query(`DELETE FROM listas_pe WHERE id=$1`, [req.params.id]);
    res.json({ ok:true });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Adiciona uma peça a uma lista PE
app.post('/api/listas-pe/:id/pecas', exigirEdicao, async (req,res) => {
  try {
    const { modelo, colecaoCor, bordado, obs, numeroPedido, vendedora } = req.body;
    const lista = (await pool.query(`SELECT * FROM listas_pe WHERE id=$1`, [req.params.id])).rows[0];
    if (!lista) return res.status(404).json({error:'Lista não encontrada'});
    const id = crypto.randomBytes(8).toString('hex');
    // Usa o modelo da peça se informado, senão herda o modelo da lista
    await pool.query(
      `INSERT INTO pecas_pe(id, lista_id, modelo, colecao_cor, bordado, obs, numero_pedido, vendedora, vendida)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,FALSE)`,
      [id, req.params.id, (modelo||lista.modelo).trim(), colecaoCor||null, bordado||null, obs||null, numeroPedido||null, vendedora||null]
    );
    res.json({ ok:true, id });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Edita uma peça PE
app.put('/api/pecas-pe/:id', exigirEdicao, async (req,res) => {
  try {
    const { modelo, colecaoCor, bordado, obs, numeroPedido, vendedora } = req.body;
    await pool.query(
      `UPDATE pecas_pe SET modelo=$2, colecao_cor=$3, bordado=$4, obs=$5, numero_pedido=$6, vendedora=$7 WHERE id=$1`,
      [req.params.id, modelo, colecaoCor||null, bordado||null, obs||null, numeroPedido||null, vendedora||null]
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Marca peça PE como vendida (registra pedido e vendedora) — sai do estoque
app.post('/api/pecas-pe/:id/vender', exigirEdicao, async (req,res) => {
  try {
    const { numeroPedido, vendedora } = req.body;
    await pool.query(
      `UPDATE pecas_pe SET vendida=TRUE, numero_pedido=$2, vendedora=$3, vendida_em=NOW() WHERE id=$1`,
      [req.params.id, numeroPedido||null, vendedora||null]
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Reverte a venda de uma peça (volta ao estoque)
app.post('/api/pecas-pe/:id/devolver', exigirEdicao, async (req,res) => {
  try {
    await pool.query(
      `UPDATE pecas_pe SET vendida=FALSE, numero_pedido=NULL, vendedora=NULL, vendida_em=NULL WHERE id=$1`,
      [req.params.id]
    );
    res.json({ ok:true });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Exclui uma peça PE
app.delete('/api/pecas-pe/:id', exigirEdicao, async (req,res) => {
  try {
    await pool.query(`DELETE FROM pecas_pe WHERE id=$1`, [req.params.id]);
    res.json({ ok:true });
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.post('/api/pedido/:id/status', exigirEdicao, async (req,res) => {
  const token = await loadToken();
  if (!token) return res.status(401).json({error:'Não autorizado'});
  try {
    const {id} = req.params;
    const {novaTag} = req.body;

    // Busca tags atuais do pedido
    const getR = await fetch(`https://${SHOP}/admin/api/2024-01/orders/${id}.json?fields=id,tags`, {headers:shopHeaders(token)});
    let tagsAtuais = '';
    if (getR.ok) {
      const getData = await getR.json();
      tagsAtuais = getData.order?.tags || '';
    }

    // Remove tags de status antigas e adiciona a nova
    const statusTags = ['em_producao','em-producao','pronto','enviado','atrasado'];
    const outrasTagsArr = tagsAtuais.split(',').map(t=>t.trim()).filter(t => t && !statusTags.includes(t.toLowerCase()));
    outrasTagsArr.push(novaTag);
    const novasTags = [...new Set(outrasTagsArr)].join(', ');

    const r = await fetch(`https://${SHOP}/admin/api/2024-01/orders/${id}.json`,{
      method:'PUT', headers:shopHeaders(token),
      body:JSON.stringify({order:{id, tags:novasTags}})
    });
    CACHE_TS = 0;
    res.json(await r.json());
  } catch(e){res.status(500).json({error:e.message});}
});

// ── Rotas de edição de pedidos ────────────────────────────────────────────────
app.get('/api/pedido/:id/historico', async (req,res) => {
  try {
    const r = await pool.query(
      `SELECT * FROM historico_pedidos WHERE order_id=$1 ORDER BY alterado_em DESC LIMIT 20`,
      [req.params.id]
    );
    res.json({ historico: r.rows });
  } catch(e) { res.status(500).json({error:e.message}); }
});

// Histórico geral de alterações (todas as edições de pedidos, mais recentes primeiro)
app.get('/api/historico-geral', async (req,res) => {
  try {
    const limite = Math.min(parseInt(req.query.limite) || 200, 500);
    const r = await pool.query(
      `SELECT * FROM historico_pedidos ORDER BY alterado_em DESC LIMIT $1`,
      [limite]
    );
    const total = await pool.query(`SELECT COUNT(*)::int AS n FROM historico_pedidos`);

    // Cruza com dados atuais para enriquecer cada alteração
    const listas = await loadListas();
    const pedidosAtuais = CACHE_PEDIDOS || [];

    const historico = r.rows.map(h => {
      // Busca o ITEM exato (se o histórico tem item_id); senão, cai no primeiro item do pedido
      let pedidoAtual = null;
      if (h.item_id) {
        pedidoAtual = pedidosAtuais.find(p => String(p.itemId) === String(h.item_id));
      }
      if (!pedidoAtual) {
        pedidoAtual = pedidosAtuais.find(p => String(p.orderId) === String(h.order_id));
      }
      // Listas que contêm este item (ou qualquer item do pedido, se não houver item_id)
      let listasDoItem;
      if (h.item_id) {
        listasDoItem = listas
          .filter(l => Array.isArray(l.pedidoIds) && l.pedidoIds.some(id => String(id) === String(h.item_id)))
          .map(l => l.numero);
      } else {
        const itensDoPedido = pedidosAtuais.filter(p => String(p.orderId) === String(h.order_id)).map(p => String(p.itemId));
        listasDoItem = listas
          .filter(l => Array.isArray(l.pedidoIds) && l.pedidoIds.some(id => itensDoPedido.includes(String(id))))
          .map(l => l.numero);
      }
      return {
        ...h,
        modelo_atual: pedidoAtual ? pedidoAtual.modeloBase : null,
        cor_atual: pedidoAtual ? pedidoAtual.colecaoCor : null,
        em_listas: [...new Set(listasDoItem)],
      };
    });

    res.json({ historico, total: total.rows[0]?.n || 0 });
  } catch(e) { res.status(500).json({error:e.message}); }
});

app.post('/api/pedido/:id/editar', exigirEdicao, async (req,res) => {
  try {
    const { id } = req.params; // orderId
    const { alteradoPor, numero, itemId, campos, anteriores } = req.body;
    const ant = anteriores || {}; // valores que apareciam na tela antes da edição
    const numPedido = numero || null;
    const chaveItem = itemId || id; // se não vier itemId, usa o orderId (retaguarda)

    // Busca override atual do ITEM
    const atual = await pool.query(`SELECT * FROM itens_editados WHERE item_id=$1`, [chaveItem]);
    const dadosAtuais = atual.rows[0] || {};

    // Registra histórico para cada campo alterado
    const historicoItens = [];
    const updates = {};

    if (campos.modelo !== undefined) {
      historicoItens.push([id, numPedido, 'modelo', ant.modelo !== undefined ? ant.modelo : (dadosAtuais.modelo_override||null), campos.modelo, alteradoPor, chaveItem]);
      updates.modelo_override = campos.modelo;
    }
    if (campos.colecaoCor !== undefined) {
      historicoItens.push([id, numPedido, 'colecao_cor', ant.colecaoCor !== undefined ? ant.colecaoCor : (dadosAtuais.colecao_cor_override||null), campos.colecaoCor, alteradoPor, chaveItem]);
      updates.colecao_cor_override = campos.colecaoCor;
    }
    if (campos.bordado !== undefined) {
      historicoItens.push([id, numPedido, 'bordado', ant.bordado !== undefined ? ant.bordado : (dadosAtuais.bordado_override||null), campos.bordado, alteradoPor, chaveItem]);
      updates.bordado_override = campos.bordado;
    }
    if (campos.dataEnvio !== undefined) {
      historicoItens.push([id, numPedido, 'data_envio', ant.dataEnvio !== undefined ? ant.dataEnvio : (dadosAtuais.data_envio_override||null), campos.dataEnvio, alteradoPor, chaveItem]);
      updates.data_envio_override = campos.dataEnvio;
    }
    if (campos.vendedora !== undefined) {
      historicoItens.push([id, numPedido, 'vendedora', ant.vendedora !== undefined ? ant.vendedora : (dadosAtuais.vendedora_override||null), campos.vendedora, alteradoPor, chaveItem]);
      updates.vendedora_override = campos.vendedora;
    }
    if (campos.status !== undefined) {
      historicoItens.push([id, numPedido, 'status', ant.status !== undefined ? ant.status : (dadosAtuais.status_override||null), campos.status, alteradoPor, chaveItem]);
      updates.status_override = campos.status || null;
    }
    if (campos.obsInterna !== undefined) {
      historicoItens.push([id, numPedido, 'obs_interna', ant.obsInterna !== undefined ? ant.obsInterna : (dadosAtuais.obs_interna||null), campos.obsInterna, alteradoPor, chaveItem]);
      updates.obs_interna = campos.obsInterna || null;
    }

    // Salva overrides por ITEM
    if (Object.keys(updates).length > 0) {
      const cols = Object.keys(updates);
      // item_id ($1), order_id ($2), depois os campos
      const colNames = ['item_id','order_id',...cols].join(',');
      const placeholders = ['$1','$2',...cols.map((_,i)=>'$'+(i+3))].join(',');
      const setCols = cols.map((k,i)=>`${k}=$${i+3}`).join(',');
      const vals = [chaveItem, id, ...cols.map(k=>updates[k])];
      await pool.query(
        `INSERT INTO itens_editados(${colNames},atualizado_em)
         VALUES(${placeholders},NOW())
         ON CONFLICT(item_id) DO UPDATE SET ${setCols},atualizado_em=NOW()`,
        vals
      );
    }

    // Salva histórico
    for (const h of historicoItens) {
      await pool.query(
        `INSERT INTO historico_pedidos(order_id,order_numero,campo,valor_anterior,valor_novo,alterado_por,item_id) VALUES($1,$2,$3,$4,$5,$6,$7)`,
        h
      );
    }

    // Invalida cache
    CACHE_TS = 0;
    res.json({ ok: true });
  } catch(e) { console.error(e); res.status(500).json({error:e.message}); }
});

// Buscar overrides de um pedido
app.get('/api/pedido/:id/overrides', async (req,res) => {
  try {
    const r = await pool.query(`SELECT * FROM pedidos_editados WHERE order_id=$1`, [req.params.id]);
    res.json({ overrides: r.rows[0] || null });
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
initDB().then(()=>{
  app.listen(PORT,'0.0.0.0',()=>console.log(`VM Sistema porta ${PORT}`));
}).catch(e=>{
  console.error('Erro ao inicializar DB:', e.message);
  app.listen(PORT,'0.0.0.0',()=>console.log(`VM Sistema porta ${PORT} (sem DB)`));
});
