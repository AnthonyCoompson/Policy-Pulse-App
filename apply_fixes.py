#!/usr/bin/env python3
"""
PolicyPulse fix patcher.
Run from the repo root (the folder holding main.py and app.html):
    python apply_fixes.py
It edits main.py and app.html in place, keeps .bak copies, and aborts
without writing if any anchor is not found exactly once.
"""
import re, shutil, sys

def load(p):
    with open(p, encoding="utf-8") as f:
        return f.read()

def save(p, s):
    shutil.copyfile(p, p + ".bak")
    with open(p, "w", encoding="utf-8", newline="") as f:
        f.write(s)

def rep(s, old, new, label):
    n = s.count(old)
    if n != 1:
        sys.exit(f"ABORT [{label}]: expected 1 match, found {n}")
    return s.replace(old, new)

def rep_span(s, start, end, new, label):
    i = s.find(start)
    j = s.find(end, i + 1) if i >= 0 else -1
    if i < 0 or j < 0 or s.count(start) != 1:
        sys.exit(f"ABORT [{label}]: span anchors not found uniquely")
    return s[:i] + new + s[j:]

# ───────────────────────── main.py ─────────────────────────
m = load("main.py")

m = rep(m, "from datetime import datetime\n\nfrom database import (",
           "from datetime import datetime, timedelta\n\nfrom database import (", "datetime import")

m = rep_span(m, '_API_KEY = os.environ.get("PP_API_KEY", "")', "def require_auth(request: Request):",
r'''_API_KEY = os.environ.get("PP_API_KEY", "").strip()

def verify_api_key(request: Request) -> bool:
    if not _API_KEY:
        return True
    auth = request.headers.get("Authorization", "")
    return auth.startswith("Bearer ") and auth[7:].strip() == _API_KEY

''', "auth")

m = rep(m, "# ── AI CHAT PROXY", r'''@app.get("/auth/check")
def auth_check(_=Depends(require_auth)):
    """Lets the UI verify the API key, not just reachability."""
    return {"ok": True, "auth_required": bool(_API_KEY)}


# ── AI CHAT PROXY''', "auth check")

m = rep(m, '@app.get("/articles/{article_id}")\ndef get_article(',
           '@app.get("/articles/{article_id:int}")\ndef get_article(', "get route")
m = rep(m, '@app.delete("/articles/{article_id}")\ndef delete_article(',
           '@app.delete("/articles/{article_id:int}")\ndef delete_article(', "delete route")

m = rep_span(m, '@app.delete("/articles/prune")', "# ── ARTICLE RE-ANALYSIS",
r'''_PRUNE_WHERE = (
    "staged = 0 AND ("
    " (pub_date IS NOT NULL AND pub_date != '' AND pub_date < ?)"
    " OR ((pub_date IS NULL OR pub_date = '') AND substr(processed_date,1,10) < ?))"
)


@app.delete("/articles/prune")
def prune_old_articles(
    days: int = Query(180, ge=30, le=3650),
    dry_run: bool = Query(False),
    _=Depends(require_auth),
):
    from database import get_conn
    conn = get_conn()
    cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    n = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {_PRUNE_WHERE}", (cutoff, cutoff)).fetchone()[0]
    if not dry_run:
        conn.execute(f"DELETE FROM articles WHERE {_PRUNE_WHERE}", (cutoff, cutoff))
        conn.commit()
    conn.close()
    return {"ok": True, "dry_run": dry_run, "deleted": 0 if dry_run else n,
            "would_delete": n, "cutoff_date": cutoff,
            "note": "Staged articles are never pruned."}


@app.get("/articles/prune/preview")
def prune_preview(days: int = Query(180, ge=30, le=3650), _=Depends(require_auth)):
    from database import get_conn
    conn = get_conn()
    cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    n = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {_PRUNE_WHERE}", (cutoff, cutoff)).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    conn.close()
    return {"would_delete": n, "total": total, "cutoff_date": cutoff, "days": days}


''', "prune")

# extra fix 1: notion_test lost its route decorator
m = rep(m, "async def notion_test(body: dict):",
           '@app.post("/notion/test")\nasync def notion_test(body: dict):', "notion decorator")

# extra fix 2: backfill used helpers that do not exist; make it self-contained
m = rep_span(m, "def _run_backfill_bg(source: dict, pages: int):", '@app.get("/sources/{source_id}/check")',
r'''def _backfill_page_url(url: str, page: int, style: str) -> str:
    from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
    if page <= 1 or style == "none":
        return url
    p = urlparse(url)
    q = dict(parse_qsl(p.query))
    if style == "path":
        return urlunparse(p._replace(path=p.path.rstrip("/") + f"/page/{page}/"))
    if style == "offset":
        q["offset"] = str((page - 1) * 20)
    else:  # "auto" and "query"
        q["page"] = str(page)
    return urlunparse(p._replace(query=urlencode(q)))


def _run_backfill_bg(source: dict, pages: int):
    import logging as _log
    from database import get_exclusion_keywords, update_source_scraped
    from scraper import scrape_generic, _base_url, _process_and_save
    log = _log.getLogger(__name__)
    name = source["name"]
    style = source.get("pagination_style") or "auto"
    try:
        exclusions = get_exclusion_keywords()
        seen, total_added = set(), 0
        for page in range(1, pages + 1):
            page_url = _backfill_page_url(source["url"], page, style)
            raw = scrape_generic(page_url, name, base_url=_base_url(source["url"]))
            fresh = [a for a in raw if a["url"] not in seen]
            if not fresh:
                log.info(f"[backfill] {name}: page {page} had no new links, stopping")
                break
            seen.update(a["url"] for a in fresh)
            added, _ = _process_and_save(fresh, source, exclusions=exclusions)
            total_added += added
        update_source_scraped(name, total_added)
        log.info(f"[backfill] {name}: {total_added} new articles")
    except Exception as e:
        log.error(f"[backfill] {name}: {e}", exc_info=True)


''', "backfill")

# extra fix 3: remove the second (dead) check_source_reachability route
m = rep_span(m, "# ── SOURCE REACHABILITY", "# ── ARTICLE PRUNING", "", "dup check route")

# ───────────────────────── app.html ─────────────────────────
h = load("app.html")

h = rep(h, "function normaliseArticle(a) {", r'''/* apiCall: like apiFetch but keeps status + message so the UI can explain failures */
async function apiCall(path, opts){
  var res = {ok:false, status:0, data:null, detail:'', kind:''};
  if(!API_BASE){ res.kind='no-backend'; res.detail='No backend URL set.'; return res; }
  var isWrite = opts && opts.method && opts.method !== 'GET';
  var ctrl = new AbortController();
  var timer = setTimeout(function(){ ctrl.abort(); }, isWrite ? 60000 : 20000);
  try{
    var headers = {'Content-Type':'application/json'};
    if(_PP_API_KEY) headers['Authorization'] = 'Bearer ' + _PP_API_KEY;
    var r = await fetch(API_BASE + path, Object.assign({headers:headers, signal:ctrl.signal}, opts||{}));
    clearTimeout(timer);
    res.status = r.status;
    try{ res.data = await r.json(); }catch(e){}
    if(r.ok){ res.ok = true; return res; }
    var d = res.data && res.data.detail;
    res.detail = (typeof d === 'string' ? d : d ? JSON.stringify(d) : '') || ('HTTP ' + r.status);
    res.kind = r.status===401 ? 'auth' : r.status>=500 ? 'server' : 'http';
  }catch(e){
    clearTimeout(timer);
    res.kind = e.name==='AbortError' ? 'timeout' : 'network';
    res.detail = e.name==='AbortError' ? 'Request timed out (Render may be waking up, retry in 30s).' : 'Network error: ' + e.message;
  }
  return res;
}
function apiFailMessage(res){
  if(res.kind==='auth') return _PP_API_KEY
    ? 'Backend rejected your saved API key (401). Re-paste the exact PP_API_KEY from Render in Settings > API Key.'
    : 'No API key saved in this browser. Paste your Render PP_API_KEY in Settings > API Key.';
  return res.detail || 'Unknown error';
}
async function checkAuth(){
  var r = await apiCall('/auth/check');
  if(r.ok) return 'ok';
  if(r.kind==='auth') return _PP_API_KEY ? 'rejected' : 'missing';
  if(r.status===404) return 'unsupported';
  return 'error';
}

function normaliseArticle(a) {''', "apiCall")

h = rep_span(h, "async function scrapeNow(){", "async function clearDashboard(){",
r'''async function scrapeNow(){
  if(typeof gtag!=='undefined') gtag('event','scrape_triggered',{event_category:'Scraper'});
  var btn=document.getElementById('scrape-btn'), progress=document.getElementById('scrape-progress');
  function reset(){ if(progress) progress.style.display='none'; if(btn){btn.disabled=false;btn.innerHTML='&#x1F50D; Scrape Now';} }
  if(!API_BASE){
    showToast('&#128268;','No Backend Connected','Enter your Render URL in Settings > Backend Connection.',7000);
    showTab('settings'); return;
  }
  if(btn){btn.disabled=true;btn.innerHTML='<span class="spin">&#8635;</span> Scraping&hellip;';}
  if(progress) progress.style.display='block';
  var before = await apiCall('/stats');
  var base = before.ok ? before.data : {total:0,last_scraped:null};
  var res = await apiCall('/scrape',{method:'POST'});
  if(!res.ok){ reset(); showToast('&#9888;','Scrape Error',apiFailMessage(res),9000); return; }
  showToast('&#10003;','Scrape Started','Fetching and AI-processing articles. Feed refreshes automatically.',5000);
  var polls=0;
  var poller=setInterval(async function(){
    polls++;
    var s=await apiCall('/stats');
    var done = s.ok && s.data.last_scraped && s.data.last_scraped!==base.last_scraped;
    if(!done && polls<36) return;
    clearInterval(poller);
    _articleCache=[]; _cacheTime=0;
    await apiLoadArticles({limit:100,force:true}); await apiLoadStats();
    reset(); renderFeed(); renderDashboard(); updateBadges();
    var added = s.ok ? Math.max(0, s.data.total - base.total) : 0;
    showToast('&#128240;', done?'Scrape Complete':'Still Running',
      done ? (added+' new article'+(added!==1?'s':'')+' added.') : 'Taking longer than usual. Check the Scrape Log tab.', 4500);
  },10000);
}
''', "scrapeNow")

h = rep(h, "  var d = await apiFetch(endpoint, { method: 'POST', body: JSON.stringify({ limit: 300 }) });\n",
           "  var fr = await apiCall(endpoint, { method: 'POST', body: JSON.stringify({ limit: 300 }) });\n  var d = fr.ok ? fr.data : null;\n", "runFixDates call")
h = rep(h, "    if(statEl) statEl.textContent = '⚠ Could not start — check your backend.';\n    showToast('&#x26A0;','Error','Date fix failed to start.',3000);",
           "    var fmsg = apiFailMessage(fr);\n    if(statEl) statEl.textContent = '⚠ ' + fmsg;\n    showToast('&#x26A0;','Date Fix Failed',fmsg,9000);", "runFixDates fail")

h = rep(h, "  var d = await apiFetch('/articles/fix-dates',{method:'POST',body:JSON.stringify({ids:[id]})});\n  if(d&&d.ok){",
           "  var sr = await apiCall('/articles/fix-dates',{method:'POST',body:JSON.stringify({ids:[id]})});\n  var d = sr.ok ? sr.data : null;\n  if(d&&d.ok){", "fixSingle call")
h = rep(h, "    showToast('&#x26A0;','Error','Could not start date fix.',3000);",
           "    showToast('&#x26A0;','Date Fix Failed',apiFailMessage(sr),9000);", "fixSingle fail")

h = rep_span(h, "async function prunePreview(){", "/* === DARK/LIGHT MODE === */",
r'''async function prunePreview(){
  var days=parseInt(document.getElementById('prune-days-inp').value)||180;
  var el=document.getElementById('prune-result');
  var r=await apiCall('/articles/prune/preview?days='+days);
  el.style.display='block';
  if(!r.ok){ el.style.background='rgba(244,63,94,0.08)'; el.style.border='1px solid rgba(244,63,94,0.25)'; el.style.color='#fb7185'; el.textContent='⚠ '+apiFailMessage(r); return; }
  var d=r.data, bad=d.would_delete>0;
  el.style.background=bad?'rgba(244,63,94,0.08)':'rgba(16,185,129,0.08)';
  el.style.border=bad?'1px solid rgba(244,63,94,0.25)':'1px solid rgba(16,185,129,0.25)';
  el.style.color=bad?'#fb7185':'var(--green)';
  el.textContent = bad ? '⚠ '+d.would_delete+' of '+d.total+' articles older than '+d.cutoff_date+' would be deleted.' : '✓ Nothing older than '+days+' days.';
}
async function pruneArticles(){
  var days=parseInt(document.getElementById('prune-days-inp').value)||180;
  var p=await apiCall('/articles/prune/preview?days='+days);
  if(!p.ok){ showToast('&#x26A0;','Prune Failed',apiFailMessage(p),9000); return; }
  if(p.data.would_delete===0){ showToast('&#x2713;','Nothing to Prune','No articles older than '+days+' days.',3000); return; }
  if(!confirm('Permanently delete '+p.data.would_delete+' articles older than '+p.data.cutoff_date+'?\n\nStaged articles are kept. This cannot be undone.')) return;
  var r=await apiCall('/articles/prune?days='+days,{method:'DELETE'});
  if(!r.ok){ showToast('&#x26A0;','Prune Failed',apiFailMessage(r),9000); return; }
  _articleCache=[]; _cacheTime=0;
  await apiLoadArticles({limit:100,force:true});
  renderFeed(); renderDashboard(); updateBadges(); prunePreview();
  showToast('&#x1F5D1;','Pruned',r.data.deleted+' articles deleted.',4000);
}

''', "prune js")

h = rep(h, """    if(r.ok){
      if(stat) stat.innerHTML = '<span style="color:var(--green)">&#x2713; Connected — backend is healthy</span>';
      showToast('&#x2705;','Backend OK','Render backend is reachable.',3000);
    } else {""", """    if(r.ok){
      var a = await checkAuth();
      var msgs = {
        ok:'<span style="color:var(--green)">&#x2713; Reachable and API key accepted</span>',
        missing:'<span style="color:#fbbf24">&#x26A0; Reachable, but no API key saved. Scrape, prune and date fix will fail.</span>',
        rejected:'<span style="color:#fb7185">&#x2715; Reachable, but the backend rejected your API key.</span>',
        unsupported:'<span style="color:var(--muted)">&#x2713; Reachable (redeploy backend to enable key check)</span>',
        error:'<span style="color:#fbbf24">&#x26A0; Reachable, key check failed.</span>'
      };
      if(stat) stat.innerHTML = msgs[a];
      showToast(a==='ok'?'&#x2705;':'&#x26A0;','Backend Check', a==='ok'?'Backend and API key OK.':'See status under the URL field.',4000);
    } else {""", "testBackend")

h = rep(h, "  showToast('&#x1F511;','API Key Saved','Backend authentication key stored.',2500);\n",
           "  showToast('&#x1F511;','API Key Saved','Backend authentication key stored.',2500);\n  testBackendConnection();\n", "saveApiKey")
h = rep(h, "if(beInp) beInp.value = localStorage.getItem('pp_api_base') || '';",
           "if(beInp) beInp.value = localStorage.getItem('pp_api_base') || API_BASE || '';", "settings url")

save("main.py", m)
save("app.html", h)
print("Patched main.py and app.html (.bak copies written).")
