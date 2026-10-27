#!/usr/bin/env python3
"""微信实时监听 + 本地模型回复建议（原型，支持联系人切换）。
- 复用 wechat-exporter 的密钥缓存 / 分片定位 / 消息解析；SQLCipher 只读打开微信正在写的库，按 local_id 增量拉新消息
- 对方发来新消息后 debounce，把最近上下文 + 该联系人的关系分析摘录 + 过去「候选 vs 我实际发的」对照发给办公 Qwen，产出情绪判断 + 候选回复
- 页面 127.0.0.1:<port>：可搜索切换联系人；每次建议与我实际发出的回复配对落盘 ~/.wechat_live/<wxid>_feedback.jsonl
- 不自动发送任何消息
用法: ../.venv/bin/python live.py [--contact wxid|备注|昵称] [--interval 2] [--port 8765] [--test-fire]
每联系人的长期记忆放 ~/.wechat_live/memory/<wxid>.md（可选，取其 ## 五/六 章）
"""
import sys, os, json, time, argparse, threading, subprocess, collections, re, difflib, urllib.request, urllib.error
from datetime import datetime
from zoneinfo import ZoneInfo
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import quote, urlparse, parse_qs

EXPORTER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # 仓库根目录
sys.path.insert(0, EXPORTER)
from core import locator, key_extractor, db_decryptor, db_reader, message_parser, constants  # noqa: E402
from sqlcipher3 import dbapi2  # noqa: E402

STATE_DIR = os.path.expanduser('~/.wechat_live')
MEMORY_DIR = os.path.join(STATE_DIR, 'memory')
STYLE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'styles')
HERE = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------------------- 模型
def pick_endpoint():
    # 端点来自环境变量 LOCAL_LLM_ENDPOINTS 或仓库根目录的 llm_endpoints.json，见 core/llm_endpoints.py
    from core.llm_endpoints import pick_endpoint as _pick
    return _pick()

def chat(url, model, system, user, max_tokens=1500, timeout=180):
    body = dict(model=model, temperature=0.4, max_tokens=max_tokens,
                messages=[{'role': 'system', 'content': system}, {'role': 'user', 'content': user}],
                chat_template_kwargs={'enable_thinking': False})
    req = urllib.request.Request(url + '/v1/chat/completions', data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
    r = json.load(urllib.request.urlopen(req, timeout=timeout))
    content = r['choices'][0]['message'].get('content') or ''
    if not content.strip():
        raise RuntimeError('模型正文为空')
    return content

def parse_json(s):
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s.strip(), flags=re.M)
    return json.loads(s[s.find('{'):s.rfind('}') + 1])

SYSTEM = """你是「我」的私人聊天参谋。「我」正在和一位联系人用微信聊天，下面会给你：(1) 关于对方的长期记忆：如果其中有「回复守则」，那是硬约束，节奏、措辞、禁止事项都必须遵守，其余章节（人格画像、心理分析、关系分析）用来理解她此刻的状态；(2) 最近的聊天上下文，按时间排列，带北京时间，「我」= 本人，另一名字 = 对方；(3) 对方刚发来、我还没回的消息。
可能还会给你 (4) 过去几次「模型候选 vs 我实际发出」的对照——把它当作口吻校准：我实际发的才是标准，候选只是参考，向我的真实风格靠。
可能还会给你 (5) 一份「参谋风格」：它决定策略取向、判断方法和句式骨架。但优先级低于回复守则——风格与守则里的雷区、禁止事项、节奏规则冲突时，一律按守则；风格里的句式骨架要换成「我」的口吻，不照抄。
请冷静、具体地判断，并给出可直接发出的回复草稿。要求：
- 回复要像「我」平时在上下文里的口吻（长短、用词、表情习惯），别端着。长度对齐「我」最近几条消息的典型长度，宁短勿长。
- 三条候选分别偏向：接话/把话题顺下去；轻松幽默；克制或转移。「接话」指把对话推进，不是推进关系。
- 禁止替「我」发明上下文里没有的安排、承诺或动作（见面、去找对方、送东西、请客等）；对方只是暗示，就只回应暗示本身，不替我接下这个局。
- 亲密度和主动程度以「我」最近自己发的消息为上限，不要比我平时更热络、更黏、更油；别用油腻的套话（「拯救你的无聊」「陪你解解闷」这类）。
- 群聊里「对方」是多个人，判断时注意是谁在对我说话。
- 不要编造上下文里没有的事实。
- 候选必须是「我」会随手打出来的那一句：不要复述或总结对方刚说的信息（她说贵，别回「确实贵」再解释一遍为什么贵），不要给建议、出主意、下结论，不要像客服/顾问/长辈。接不住的话题宁可一句短的反应或反问。
- 三条候选要真的不同（不同角度或不同动作），不是同一句换措辞。
只输出一个 JSON 对象：
- urgency 按守则的「回复节奏」判定：马上回 / 过会儿回（并在 advice 里说大概等多久）/ 先别回。判定为「先别回」时 replies 给空数组，advice 说明原因。
- 每条候选先在心里对照守则的禁词和禁止事项过一遍，违反的不要输出。
只输出一个 JSON 对象：
{"mood":"对方此刻的情绪/状态 ≤20字","intent":"对方这几条想要什么 ≤40字","urgency":"马上回|过会儿回|先别回","advice":"给我的一句话建议 ≤60字","replies":[{"style":"接话|轻松|克制","text":"..."}]}"""

# ----------------------------------------------------------------------------- 微信库实时读
class Account:
    """账号级：布局、密钥、联系人表、分片索引。只在启动时读一次（联系人表走解密副本）。"""
    def __init__(self):
        self.layout = locator.select_account(None)
        self.keys = key_extractor.load_cached_keys()
        dec_dir = os.path.join(os.path.expanduser(constants.DECRYPTED_DIR), self.layout.account_id)
        self.decrypted = {src: os.path.join(dec_dir, os.path.basename(src)) for src in self.layout.message_dbs}
        self.decrypted[self.layout.contact_db] = os.path.join(dec_dir, 'contact.db')
        if not os.path.exists(self.decrypted[self.layout.contact_db]):
            print('[account] 解密快照不存在（导出器重设密钥会清掉它），重建中…', flush=True)
            db_decryptor.decrypt_layout(self.layout, self.keys)
        self.contacts = db_reader.read_contacts(self.decrypted, self.layout)
        self.owner = db_reader.resolve_owner_wxid(self.layout, self.decrypted)
        oc = self.contacts.get(self.owner)
        self.my_names = {n for n in ((oc.nickname, oc.remark, oc.alias) if oc else ()) if n}
        dec_msg = [p for p in self.decrypted.values() if os.path.basename(p).startswith('message_') and os.path.basename(p) not in ('message_fts.db', 'message_resource.db')]
        self.index = db_reader.build_table_index(dec_msg)
        self._patch_connect()
        print(f'[account] {self.layout.account_id}  联系人 {len(self.contacts)}  分片 {len(self.layout.message_dbs)}', flush=True)

    def _patch_connect(self):
        live, layout, keys = set(self.layout.message_dbs) | ({self.layout.session_db} if self.layout.session_db else set()), self.layout, self.keys
        orig = db_reader._connect_ro
        def connect_ro(path):
            if path in live:
                salt = db_decryptor.read_salt(path).hex()
                key = db_decryptor.resolve_key(path, salt, keys)
                conn = dbapi2.connect('file:' + quote(path) + '?mode=ro', uri=True)
                cur = conn.cursor()
                cur.execute('PRAGMA key = "%s"' % db_decryptor._key_literal(key, path))
                db_decryptor._apply_pragmas(cur, layout.version)
                cur.execute('PRAGMA query_only=1')
                conn.text_factory = lambda b: b.decode('utf-8', 'replace') if isinstance(b, bytes) else b
                return conn
            return orig(path)
        db_reader._connect_ro = connect_ro

    _sess_cache = ({}, 0.0)

    def session_times(self):
        """username -> 最近一条消息时间(unix 秒)，实时读微信 session.db，30 秒缓存。"""
        data, at = self._sess_cache
        if time.time() - at < 30 or not self.layout.session_db: return data
        try:
            conn = db_reader._connect_ro(self.layout.session_db)
            try:
                rows = conn.execute('SELECT username, COALESCE(sort_timestamp, last_timestamp, 0) FROM SessionTable').fetchall()
            finally:
                conn.close()
            data = {u: int(t or 0) for u, t in rows}
            self._sess_cache = (data, time.time())
        except Exception as e:
            print(f'[session] 读会话列表失败 {type(e).__name__}: {e}', flush=True)
        return data

    def search(self, q, limit=30):
        q = (q or '').strip().lower()
        times = self.session_times()
        rows = []
        for c in self.contacts.values():
            if c.is_official: continue
            if q and q not in c.search_blob(): continue
            has_table = c.chat_table(self.layout.version).lower() in self.index
            last = times.get(c.username, 0)
            if not q and not last: continue          # 空搜索只列最近会话
            rows.append(dict(username=c.username, name=c.display_name, nickname=c.nickname, remark=c.remark, group=c.is_group, has_chat=has_table or last > 0, last_ts=last))
        rows.sort(key=lambda r: (-r['last_ts'], not r['has_chat'], r['name']))
        return rows[:limit]

    def find(self, key):
        hits = [c for c in self.contacts.values() if key in (c.username, c.remark, c.nickname, c.alias)]
        exact = [c for c in hits if c.username == key]
        return exact[0] if exact else (hits[0] if len(hits) == 1 else None), hits

class LiveChat:
    """联系人级：消息表、分片、增量读。"""
    def __init__(self, acct, contact):
        self.acct, self.contact = acct, contact
        hit = db_reader.find_chat_table(acct.index, contact, acct.layout.version)
        if not hit:
            raise LookupError(f'没找到 {contact.display_name} 的消息表（可能从未聊过，或解密快照太旧）')
        dec_paths, self.table = hit
        self.live_dbs = [[s for s in acct.layout.message_dbs if os.path.basename(s) == os.path.basename(d)][0] for d in dec_paths]

    def fetch(self, start_ts):
        msgs = db_reader.read_messages(self.live_dbs, self.table, self.acct.layout, self.contact, self.acct.owner, start_ts=start_ts)
        for m in msgs:
            message_parser.parse(m, is_group=self.contact.is_group)
        return msgs

# ----------------------------------------------------------------------------- 建议 vs 实际回复 配对
class Feedback:
    """一次建议产生后，直到下一条对方消息到来（或我回复后 CLOSE_AFTER 秒无动静），期间我发的所有消息视为对这次建议的实际回应。"""
    CLOSE_AFTER = 90

    def __init__(self, path):
        self.path = path; self.open = None; self.mine = []; self.first_reply_at = None

    def on_suggestion(self, sug):
        self.flush('superseded'); self.open = dict(sug); self.mine = []; self.first_reply_at = None

    def on_my_message(self, m):
        if self.open is None: return
        self.mine.append(m)
        if self.first_reply_at is None: self.first_reply_at = time.time()

    def on_incoming(self):
        self.flush('incoming' if self.mine else 'no_reply_before_next_incoming')

    def tick(self):
        if self.open and self.first_reply_at and time.time() - self.first_reply_at >= self.CLOSE_AFTER:
            self.flush('timeout')

    def flush(self, reason):
        if self.open is None: return
        actual = [m.display_text or f'[{m.kind}]' for m in self.mine]
        joined = ''.join(t for t in actual if not t.startswith('['))
        best, best_ratio = None, 0.0
        for r in self.open.get('replies') or []:
            ratio = difflib.SequenceMatcher(None, joined, r.get('text', '')).ratio() if joined else 0.0
            if ratio > best_ratio: best, best_ratio = r.get('style'), ratio
        verdict = 'adopted' if best_ratio >= 0.8 else 'adapted' if best_ratio >= 0.45 else 'own' if actual else 'none'
        rec = dict(time=time.strftime('%Y-%m-%d %H:%M:%S'), closed_by=reason, for_ids=self.open.get('for_ids'),
                   suggestion={k: self.open.get(k) for k in ('mood', 'intent', 'urgency', 'advice', 'replies')},
                   actual=actual, actual_ids=[m.local_id for m in self.mine],
                   reply_delay_sec=round(self.mine[0].timestamp - max(self.open.get('for_ts') or [0]), 0) if self.mine and self.open.get('for_ts') else None,
                   closest_style=best, similarity=round(best_ratio, 2), verdict=verdict)
        with open(self.path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
        print(f"[feedback] {verdict} sim={rec['similarity']} actual={len(actual)}条 closed_by={reason}", flush=True)
        self.open = None; self.mine = []; self.first_reply_at = None

    def stats(self):
        if not os.path.exists(self.path): return dict(total=0)
        rows = [json.loads(l) for l in open(self.path, encoding='utf-8') if l.strip()]
        c = collections.Counter(r['verdict'] for r in rows)
        return dict(total=len(rows), **c)

    def recent_examples(self, n=8):
        if not os.path.exists(self.path): return ''
        rows = [json.loads(l) for l in open(self.path, encoding='utf-8') if l.strip()]
        rows = [r for r in rows if r['actual']][-n:]
        out = []
        for r in rows:
            cands = ' / '.join(x.get('text', '') for x in r['suggestion'].get('replies') or [])
            out.append(f"- 当时候选: {cands}\n  我实际发的: {' | '.join(r['actual'])}  （{r['verdict']}）")
        return '\n'.join(out)

# ----------------------------------------------------------------------------- 参谋风格
def list_styles():
    out = [dict(key='default', name='默认（守则）')]
    for f in sorted(os.listdir(STYLE_DIR)) if os.path.isdir(STYLE_DIR) else []:
        if f.endswith('.md') and not f.endswith('.source.md'):
            first = open(os.path.join(STYLE_DIR, f), encoding='utf-8').readline().strip()
            out.append(dict(key=f[:-3], name=first.replace('# 风格：', '') or f[:-3]))
    return out

def load_style(key):
    if not key or key == 'default': return ''
    p = os.path.join(STYLE_DIR, key + '.md')
    return open(p, encoding='utf-8').read() if os.path.exists(p) else ''

# ----------------------------------------------------------------------------- 会话（当前联系人）
def load_memory(username):
    p = os.path.join(MEMORY_DIR, f'{username}.md')
    if not os.path.exists(p): return ''
    txt = open(p, encoding='utf-8').read()
    parts = ['## ' + x for x in re.split(r'^## ', txt, flags=re.M)[1:]]
    rules = [x for x in parts if x.startswith('## 九')]
    rest = [x for x in parts if x.startswith(('## 五', '## 六', '## 七', '## 八'))]
    return ('\n'.join(rules + rest))[:10000] or txt[-10000:]

BJ = ZoneInfo('Asia/Shanghai')   # 本机时钟可能不是北京时间，消息时间一律按北京时间给模型和页面

class Session:
    def __init__(self, acct, contact, ctx_n, group_all=False):
        self.group_all = group_all
        self.chat = LiveChat(acct, contact)
        self.contact = contact; self.me, self.other = '我', contact.display_name
        self.memory = load_memory(contact.username)
        self.fb = Feedback(os.path.join(STATE_DIR, f'{contact.username}_feedback.jsonl'))
        self.log_path = os.path.join(STATE_DIR, f'{contact.username}.jsonl')
        self.ctx = collections.deque(maxlen=ctx_n)
        recent = self.chat.fetch(int(time.time()) - 3 * 86400)
        if len(recent) < 10:
            recent = self.chat.fetch(int(time.time()) - 30 * 86400)
        for m in recent[-ctx_n:]:
            self.ctx.append(m)
        self.last_id = max((m.local_id for m in self.ctx), default=0)
        self.last_ts = max((m.timestamp for m in self.ctx), default=int(time.time()) - 60)
        self.pending, self.pending_since = [], None
        print(f'[live] 切到 {self.other}  表 {self.chat.table}  上下文 {len(self.ctx)} 条  记忆 {len(self.memory)} 字  最新 local_id={self.last_id}', flush=True)

    def fmt(self, m):
        who = self.me if m.is_sender else (m.sender_username or self.other) if self.contact.is_group else self.other
        if self.contact.is_group and not m.is_sender and m.sender_username:
            c = self.chat.acct.contacts.get(m.sender_username); who = c.display_name if c else m.sender_username
        return f"{datetime.fromtimestamp(m.timestamp, BJ):%m-%d %H:%M} {who}: {m.display_text or f'[{m.kind}]'}"

    def poll(self):
        new = [m for m in self.chat.fetch(self.last_ts) if m.local_id > self.last_id]
        for m in new:
            self.ctx.append(m); self.last_id = max(self.last_id, m.local_id); self.last_ts = max(self.last_ts, m.timestamp)
            with open(self.log_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(dict(id=m.local_id, ts=m.timestamp, me=m.is_sender, sender=m.sender_username, kind=m.kind, text=m.display_text), ensure_ascii=False) + '\n')
            if m.is_sender:
                self.pending, self.pending_since = [], None; self.fb.on_my_message(m)
            elif self.contact.is_group and not self.group_all and not self._mentions_me(m):
                pass                                            # 群里没 @ 我的发言只进上下文，不触发建议
            else:
                self.fb.on_incoming(); self.pending.append(m); self.pending_since = time.time()
        self.fb.tick()
        return new

    def _mentions_me(self, m):
        t = m.display_text or ''
        if any(('@' + n) in t for n in self.chat.acct.my_names) or '@所有人' in t or '@全体成员' in t: return True
        qf = (m.extra or {}).get('quoted_from') or ''
        return m.kind == 'quote' and qf in self.chat.acct.my_names     # 引用我的消息来回复

def notify(title, text):
    try:
        subprocess.run(['osascript', '-e', f'display notification {json.dumps(text[:120])} with title {json.dumps(title)}'], timeout=5, capture_output=True)
    except Exception:
        pass

# ----------------------------------------------------------------------------- 主程序
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--contact', help='备注 / 昵称 / wxid；省略则用上次的')
    ap.add_argument('--interval', type=float, default=2.0)
    ap.add_argument('--debounce', type=float, default=5.0)
    ap.add_argument('--context', type=int, default=40)
    ap.add_argument('--port', type=int, default=8765)
    ap.add_argument('--test-fire', action='store_true', help='切换到联系人后把最后一条对方消息当作新消息跑一次')
    ap.add_argument('--group-all', action='store_true', help='群聊里任何人发言都触发建议（默认只有 @我 才触发）')
    ap.add_argument('--style', default=None, help='参谋风格：styles/ 下的文件名（不含 .md），默认沿用上次')
    a = ap.parse_args()

    os.makedirs(MEMORY_DIR, exist_ok=True)
    url, model = pick_endpoint(); print(f'[model] {url} ({model})', flush=True)
    acct = Account()
    last_file = os.path.join(STATE_DIR, 'last_contact')
    style_file = os.path.join(STATE_DIR, 'last_style')
    style = {'key': a.style or (open(style_file).read().strip() if os.path.exists(style_file) else 'default')}
    if style['key'] not in {x['key'] for x in list_styles()}: style['key'] = 'default'
    want = a.contact or (open(last_file).read().strip() if os.path.exists(last_file) else None)

    lock = threading.Lock()
    state = dict(contact=None, contact_username=None, updated=None, suggestion=None, pending=[], recent=[], status='未选择联系人', error=None, feedback=None, model=model, style=style['key'], styles=list_styles())
    state_path = os.path.join(STATE_DIR, 'state.json')
    def save():
        with lock:
            json.dump(state, open(state_path, 'w'), ensure_ascii=False, indent=1)
    switch_req = {'username': None, 'test_fire': a.test_fire}
    analyze_req = {'n': 0}
    refresh_req = {'on': False}
    if want:
        c, hits = acct.find(want)
        if c: switch_req['username'] = c.username
        else: state['error'] = f'联系人「{want}」匹配到 {len(hits)} 个，请在页面里选'
    save()

    class H(SimpleHTTPRequestHandler):
        def __init__(self, *x, **k): super().__init__(*x, directory=HERE, **k)
        def _json(self, obj, code=200):
            data = json.dumps(obj, ensure_ascii=False).encode()
            self.send_response(code); self.send_header('Content-Type', 'application/json; charset=utf-8'); self.send_header('Cache-Control', 'no-store'); self.end_headers(); self.wfile.write(data)
        def do_GET(self):
            u = urlparse(self.path)
            if u.path == '/state.json':
                self.send_response(200); self.send_header('Content-Type', 'application/json; charset=utf-8'); self.send_header('Cache-Control', 'no-store'); self.end_headers(); self.wfile.write(open(state_path, 'rb').read()); return
            if u.path == '/contacts':
                return self._json(acct.search(parse_qs(u.query).get('q', [''])[0]))
            if u.path == '/': self.path = '/index.html'
            return super().do_GET()
        def do_POST(self):
            u = urlparse(self.path)
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length') or 0)) or b'{}')
            if u.path == '/select' and body.get('username') in acct.contacts:
                switch_req['username'] = body['username']; switch_req['test_fire'] = bool(body.get('test_fire'))
                return self._json(dict(ok=True))
            if u.path == '/refresh':
                refresh_req['on'] = True; return self._json(dict(ok=True))
            if u.path == '/analyze':
                analyze_req['n'] = max(1, min(int(body.get('n') or 1), 10)); return self._json(dict(ok=True))
            if u.path == '/style' and body.get('key') in {x['key'] for x in list_styles()}:
                style['key'] = body['key']; open(style_file, 'w').write(style['key'])
                state['style'] = style['key']; state['styles'] = list_styles(); save()
                print(f"[style] 切换风格 → {style['key']}", flush=True)
                return self._json(dict(ok=True))
            return self._json(dict(ok=False, error='unknown'), 400)
        def log_message(self, *x): pass
    srv = ThreadingHTTPServer(('127.0.0.1', a.port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f'[ui] http://127.0.0.1:{a.port}', flush=True)

    sess = None
    while True:
        try:
            if switch_req['username']:
                uname, fire = switch_req['username'], switch_req['test_fire']; switch_req['username'] = None
                if sess: sess.fb.flush('switched')
                try:
                    sess = Session(acct, acct.contacts[uname], a.context, group_all=a.group_all)
                    open(last_file, 'w').write(uname)
                    state.update(contact=sess.other, contact_username=uname, suggestion=None, updated=None, pending=[], recent=[sess.fmt(m) for m in sess.ctx], status='监听中', error=None, feedback=sess.fb.stats())
                    if fire:
                        incoming = [m for m in sess.ctx if not m.is_sender]
                        if incoming:
                            sess.pending, sess.pending_since = [incoming[-1]], time.time() - a.debounce - 1
                            print('[test] 用最后一条对方消息触发一次', flush=True)
                except Exception as e:
                    if not os.path.exists(acct.decrypted[acct.layout.contact_db]):
                        print(f'[live] 切换失败 {type(e).__name__}: {e}，解密快照已丢失，重建后重试', flush=True)
                        state['status'] = '解密快照丢失，重建中…'; save()
                        acct = Account(); switch_req['username'] = uname; continue
                    state['error'] = f'切换失败 {type(e).__name__}: {e}'; print('[live] ' + state['error'], flush=True)
                save()
            if sess is None:
                time.sleep(a.interval); continue

            if analyze_req['n']:
                n = analyze_req['n']; analyze_req['n'] = 0
                incoming = [m for m in sess.ctx if not m.is_sender][-n:]
                if incoming:
                    sess.pending, sess.pending_since = incoming, time.time() - a.debounce - 1
                    print(f'[manual] 手动触发，取最近 {len(incoming)} 条对方消息', flush=True)
                else:
                    state['status'] = '上下文里没有对方的消息，无法分析'; save()
            new = sess.poll()
            if new:
                mine = sum(1 for m in new if m.is_sender)
                state['status'] = '我已回复，继续监听' if mine and not sess.pending else f'对方新消息 {len(sess.pending)} 条，等待 {a.debounce:.0f}s 看是否还有'
                state['recent'] = [sess.fmt(m) for m in sess.ctx]; state['pending'] = [sess.fmt(m) for m in sess.pending]; state['feedback'] = sess.fb.stats(); save()
                print(f"[live] 新消息 {len(new)} 条（对方 {len(new) - mine}）", flush=True)
            if refresh_req['on']:
                refresh_req['on'] = False
                state['recent'] = [sess.fmt(m) for m in sess.ctx]      # 不碰 pending：高亮保留到我回复为止
                state['status'] = f'已手动同步，上下文 {len(sess.ctx)} 条' + (f'，新 {len(new)} 条' if new else '，无新消息'); save()
                print(f'[manual] 手动同步，新 {len(new)} 条', flush=True)

            if sess.pending and time.time() - sess.pending_since >= a.debounce:
                state['status'] = '本地模型分析中…'; save()
                examples = sess.fb.recent_examples()
                user = (f"## 关系分析摘录\n{sess.memory or '（无）'}\n\n## 最近上下文\n" + '\n'.join(sess.fmt(m) for m in sess.ctx)
                        + "\n\n## 对方刚发来、我还没回的\n" + '\n'.join(sess.fmt(m) for m in sess.pending)
                        + (f"\n\n## 过去的候选 vs 我实际发的（口吻校准）\n{examples}" if examples else '')
                        + (f"\n\n## 参谋风格（优先级低于守则）\n{load_style(style['key'])}" if load_style(style['key']) else ''))
                t0 = time.time()
                try:
                    try:
                        sug = parse_json(chat(url, model, SYSTEM, user))
                    except (urllib.error.URLError, TimeoutError, OSError) as e:     # 当前端点掉线：按配置顺序重新探活，换一口再试一次
                        print(f'[model] {url} 失败 {type(e).__name__}: {e}，重新探活端点', flush=True)
                        url, model = pick_endpoint(); state['model'] = model
                        print(f'[model] 切到 {url} ({model})', flush=True)
                        sug = parse_json(chat(url, model, SYSTEM, user))
                    sug['elapsed'] = round(time.time() - t0, 1); sug['for_ids'] = [m.local_id for m in sess.pending]; sug['for_ts'] = [m.timestamp for m in sess.pending]; sug['style'] = style['key']
                    sess.fb.on_suggestion(sug)
                    state['suggestion'] = sug; state['updated'] = datetime.now(BJ).strftime('%H:%M:%S'); state['status'] = '建议已更新，继续监听'; state['error'] = None
                    first = (sug.get('replies') or [{}])[0].get('text', '')
                    notify(f'{sess.other} · {sug.get("urgency", "")}', (sug.get('advice', '') + ' ｜ ' + first) if first else sug.get('advice', ''))
                    print(f"[model] {sug['elapsed']}s urgency={sug.get('urgency')} replies={len(sug.get('replies') or [])}", flush=True)
                except Exception as e:
                    state['error'] = f'{type(e).__name__}: {e}'; state['status'] = '模型调用失败，继续监听'
                    print(f'[model] 失败 {state["error"]}', flush=True)
                sess.pending, sess.pending_since = [], None; save()
        except Exception as e:
            state['error'] = f'{type(e).__name__}: {e}'; save(); print(f'[live] 轮询异常 {state["error"]}', flush=True)
        time.sleep(a.interval)

if __name__ == '__main__':
    main()
