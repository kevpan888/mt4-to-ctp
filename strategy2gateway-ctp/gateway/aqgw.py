"""astockquant HTTP/JSON gateway over the standalone protocol client.

Wraps aqclient in a small stdlib HTTP server so an
AI Agent can drive quotes / account / positions / trading.

  GET  /health                    liveness + session stats
  GET  /account                   balance / equity / name / group
  GET  /positions                 open positions
  GET  /orders                    live pending orders (cmd 2..5)
  GET  /history   ?limit=N        closed records (most recent last)
  GET  /news                     news/mail inbox (0x98 pushes)
  GET  /symbols                   symbol index -> name
  GET  /tick      ?symbol=ag      last live tick (bid/ask/age)
  GET  /bars      ?symbol=000001&period=D1&from=<ts>&limit=N&refresh=1
                                  historical OHLCV bars (oldest first;
                                  every pull is also upserted into the
                                  local SQLite history db)
  GET  /klines    ?symbol=ag&period=M15&from=&to=&limit=N
                                  bars from the LOCAL history db only -
                                  serves data older than the server's
                                  1024-bar-per-request cap once enough
                                  has accumulated
  GET  /history-export ?symbol=ag&period=M15&from=&to=
                                  the same local history as a CSV file
                                  download (epoch time + local datetime,
                                  oldest first)
  GET  /stream    ?symbols=ag,000001&periods=M1,D1&ticks=1&bars=1
                                  SSE push: event: hello|snapshot|tick|bar
                                  (token via ?token= for EventSource)
  GET  /chart     ?symbol=ag&period=D1   single-file candlestick chart
                                  page (browser UI over /bars + /stream)
  GET  /site/*                    local static-site preview (public, no
                                  token; the operator serves the real
                                  site and the signup API from sitetools/)
  POST /reconnect                 force session rebuild
  POST /order                     {"action":"open",   "symbol":"ag",
                                    "cmd":"buy"|"sell","lots":1.0}
                                  {"action":"close",   "ticket":N}
                                  {"action":"close",   "all":true}
                                  {"action":"modify",  "ticket":N,
                                    "sl":14900.0, "tp":15100.0}
                                  {"action":"place",   "symbol":"ag",
                                    "cmd":"buy_limit"|"sell_limit"|
                                         "buy_stop"|"sell_stop"|2..5,
                                    "lots":1.0,"price":14900.0,
                                    "sl":0,"tp":0,"expiration":0}
                                  {"action":"cancel",  "ticket":N}
                                  {"action":"modify_pending","ticket":N,
                                    "price":14950.0,"sl":0,"tp":0}
  POST /backtest                  run backtrader over the local history
                                    db; body e.g. {"symbol":"ag",
                                    "period":"M15","strategy":"sma_cross",
                                    "fast":20,"slow":120,"lots":1,
                                    "cash":100000,
                                    "from":"YYYY-MM-DD","to":"YYYY-MM-DD",
                                    "comm":10.0,"margin":27000,"mult":15,
                                    "equity":true}
                                    (needs the backtrader package)

Replies are {"ok":true,"data":...} or {"ok":false,"error":"..."}.
Optional shared token: set AQGW_TOKEN and pass X-Auth-Token.
Credentials come from AQ_LOGIN / AQ_PASS env (never logged).
"""
import argparse
import html
import json
import os
import queue
import re
import sqlite3
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from aqclient import (AQClient, TradeConn, TT_OPEN, TT_CLOSE,
                      TT_CLOSE_MKT, TT_MODIFY, TT_PEND, TT_DELETE,
                      OP_BUY_LIMIT, OP_SELL_LIMIT, OP_BUY_STOP,
                      OP_SELL_STOP)

PEND_CMDS = {"buy_limit": OP_BUY_LIMIT, "sell_limit": OP_SELL_LIMIT,
             "buy_stop": OP_BUY_STOP, "sell_stop": OP_SELL_STOP}

MAX_HISTORY = 500

# Standard chart period codes
PERIODS = {"M1": 1, "M5": 5, "M15": 15, "M30": 30, "H1": 60,
           "H4": 240, "D1": 1440, "W1": 10080, "MN1": 43200}


# ---------------- local bar history ----------------
# The server caps every 0x9c history answer at 1024 bars per (symbol,
# period) and ignores start_time paging, so deep history can only be
# accumulated locally.  Every pull that flows through the gateway is
# upserted here; /klines then serves the union of everything seen so
# far, even after gateway restarts.

class HistoryDB:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        try:
            self.db.execute("PRAGMA busy_timeout=5000")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=NORMAL")
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS bars ("
                "symbol TEXT NOT NULL, period INTEGER NOT NULL, "
                "time INTEGER NOT NULL, open REAL, high REAL, low REAL, "
                "close REAL, volume INTEGER, "
                "PRIMARY KEY(symbol, period, time))")
            self.db.commit()
        except Exception:
            self.db.close()          # release the handle so the corrupt
            raise                    # file can be moved aside by the caller

    def put(self, symbol, period, bars):
        rows = [(symbol, period, int(b["time"]), float(b["open"]),
                 float(b["high"]), float(b["low"]), float(b["close"]),
                 int(b["volume"])) for b in bars]
        if not rows:
            return
        with self.lock:
            self.db.executemany(
                "INSERT OR REPLACE INTO bars VALUES (?,?,?,?,?,?,?,?)",
                rows)
            self.db.commit()

    def get(self, symbol, period, from_ts=0, to_ts=0, limit=0):
        sql = ("SELECT time, open, high, low, close, volume FROM bars "
               "WHERE symbol=? AND period=?")
        args = [symbol, period]
        if from_ts:
            sql += " AND time>=?"
            args.append(from_ts)
        if to_ts:
            sql += " AND time<=?"
            args.append(to_ts)
        if limit:
            sql += " ORDER BY time DESC LIMIT ?"
            args.append(limit)
        else:
            sql += " ORDER BY time"
        with self.lock:
            rows = self.db.execute(sql, args).fetchall()
        if limit:
            rows.reverse()              # keep oldest-first like /bars
        return [{"time": r[0], "open": r[1], "high": r[2], "low": r[3],
                 "close": r[4], "volume": r[5]} for r in rows]

    def count(self):
        with self.lock:
            return self.db.execute(
                "SELECT COUNT(*) FROM bars").fetchone()[0]

    def rows(self, symbol, period, from_ts=0, to_ts=0):
        """Raw tuples for CSV export; the lock is held only for the
        query itself, not while the download streams out."""
        sql = ("SELECT time, open, high, low, close, volume FROM bars "
               "WHERE symbol=? AND period=?")
        args = [symbol, period]
        if from_ts:
            sql += " AND time>=?"
            args.append(from_ts)
        if to_ts:
            sql += " AND time<=?"
            args.append(to_ts)
        sql += " ORDER BY time"
        with self.lock:
            return self.db.execute(sql, args).fetchall()


def _open_history_db(path, log):
    """Open the bar history db; on a corrupt file move it aside and
    start fresh, so an end user never has to repair sqlite by hand."""
    try:
        db = HistoryDB(path)
        log("history db: %s (%d rows)" % (path, db.count()))
        return db
    except sqlite3.DatabaseError as e:
        bad = "%s.corrupt-%s" % (path, time.strftime("%Y%m%d-%H%M%S"))
        try:
            os.replace(path, bad)
        except OSError as e2:
            log("history db unusable (%r), move failed (%r) - "
                "/klines disabled" % (e, e2))
            return None
        log("history db corrupt (%r) - moved to %s, starting fresh"
            % (e, bad))
        try:
            db = HistoryDB(path)
            log("history db: %s (0 rows)" % path)
            return db
        except Exception as e2:
            log("history db reopen failed (%r) - /klines disabled" % e2)
            return None
    except Exception as e:
        log("history db unavailable (%r) - /klines disabled" % e)
        return None


# ---------------- static SVG chart ----------------
# Bruno's Visualize tab renders HTML in a sandboxed iframe that blocks
# scripts, so the JS chart page can't run there.  This draws a plain
# candle chart server-side as SVG markup - no <script>, always renders.

_SVG_W, _SVG_H = 920, 520
_SVG_ML, _SVG_MR, _SVG_MT, _SVG_MB = 10, 66, 26, 34


def _svg_chart(symbol, period_name, bars):
    n = len(bars)
    if not n:
        return ("<!doctype html><meta charset='utf-8'>"
                "<body style='background:#0e1116;color:#8b95a3;"
                "font:14px sans-serif'>no data</body>")
    lo_v = min(b["low"] for b in bars)
    hi_v = max(b["high"] for b in bars)
    pad = (hi_v - lo_v) * 0.04 or 1.0
    lo_v, hi_v = lo_v - pad, hi_v + pad
    pw = _SVG_W - _SVG_ML - _SVG_MR
    ph = _SVG_H - _SVG_MT - _SVG_MB
    slot = pw / n
    bw = max(slot * 0.62, 1.0)

    def X(i):
        return _SVG_ML + slot * (i + 0.5)

    def Y(v):
        return _SVG_MT + ph * (hi_v - v) / (hi_v - lo_v)

    parts = [
        "<!doctype html><meta charset='utf-8'>"
        "<div style='font:13px/1.5 Segoe UI,sans-serif;color:#8b95a3;"
        "background:#0e1116;padding:6px 10px'>"
        "<b style='color:#d7dde5'>%s %s</b>  "
        "<span style='color:#566070'>(snapshot; live: open URL in "
        "browser)</span></div>"
        % (html.escape(symbol), period_name),
        "<svg viewBox='0 0 %d %d' style='width:100%%;height:auto;"
        "background:#0e1116'>" % (_SVG_W, _SVG_H)]

    for k in range(6):
        v = lo_v + (hi_v - lo_v) * k / 5.0
        y = Y(v)
        parts.append(
            "<line x1='%d' y1='%.1f' x2='%d' y2='%.1f' stroke='#232a33'/>"
            % (_SVG_ML, y, _SVG_W - _SVG_MR, y))
        parts.append(
            "<text x='%d' y='%.1f' fill='#8b95a3' font-size='11'>%.2f</text>"
            % (_SVG_W - _SVG_MR + 6, y + 4, v))
    for k in range(5):
        i = min(int(n * k / 4.0), n - 1)
        parts.append(
            "<text x='%.1f' y='%d' fill='#8b95a3' font-size='11' "
            "text-anchor='middle'>%s</text>"
            % (X(i), _SVG_H - _SVG_MB + 18,
               time.strftime("%m-%d %H:%M",
                             time.localtime(bars[i]["time"]))))
    for i, b in enumerate(bars):
        col = "#e0524d" if b["close"] >= b["open"] else "#2ebd85"
        x = X(i)
        parts.append(
            "<line x1='%.1f' y1='%.1f' x2='%.1f' y2='%.1f' stroke='%s'/>"
            % (x, Y(b["high"]), x, Y(b["low"]), col))
        parts.append(
            "<rect x='%.2f' y='%.1f' width='%.2f' height='%.2f' fill='%s'/>"
            % (x - bw / 2, Y(max(b["open"], b["close"])), bw,
               max(abs(Y(b["open"]) - Y(b["close"])), 0.8), col))
    parts.append("</svg>")
    return "".join(parts)

# Single-file chart page served by GET /chart.  Reads token/symbol/period
# from its own URL query, pulls history from /bars and keeps the forming
# candle fresh via /stream (SSE).  Candle colors follow the CN convention:
# red = up, green = down.
_CHART_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>astockquant chart</title>
<style>
  :root{--bg:#0e1116;--panel:#151b23;--line:#232a33;--tx:#d7dde5;
        --dim:#8b95a3;--up:#e0524d;--dn:#2ebd85;--acc:#4c8dff}
  *{box-sizing:border-box;margin:0;padding:0}
  html,body{height:100%}
  body{background:var(--bg);color:var(--tx);
       font:14px/1.4 "Segoe UI","Microsoft YaHei",sans-serif;
       display:flex;flex-direction:column}
  header{display:flex;align-items:center;gap:10px;padding:8px 12px;
         background:var(--panel);border-bottom:1px solid var(--line);
         flex-wrap:wrap}
  .logo{font-weight:700;letter-spacing:.5px;color:var(--acc)}
  #sym{width:96px;background:#0b0f14;border:1px solid var(--line);
       color:var(--tx);padding:4px 8px;border-radius:4px;font:inherit}
  #sym:focus{outline:none;border-color:var(--acc)}
  .btn{background:#0b0f14;border:1px solid var(--line);color:var(--dim);
       padding:3px 9px;border-radius:4px;cursor:pointer;font-size:12px}
  .btn:hover{color:var(--tx);border-color:#39424e}
  .btn.on{color:#fff;border-color:var(--acc);background:#16283f}
  #stat{display:flex;align-items:center;gap:6px;color:var(--dim);
        font-size:12px}
  .dot{width:8px;height:8px;border-radius:50%;background:#5a6472}
  .dot.ok{background:var(--dn)} .dot.warn{background:#e0a24d}
  .dot.err{background:var(--up)}
  #price{margin-left:auto;font-variant-numeric:tabular-nums;font-size:15px;
         white-space:nowrap}
  #price .sep{color:var(--dim);margin:0 6px}
  #chart{flex:1;min-height:0;position:relative}
  .legend{position:absolute;left:8px;z-index:3;pointer-events:none;
          font-size:12px;line-height:1.5;color:var(--tx);
          text-shadow:0 1px 2px rgba(0,0,0,.9)}
  #maLeg{top:8px}
  #bollLeg{top:30px}
  #fallback{margin:40px auto;max-width:560px;background:var(--panel);
            border:1px solid var(--line);border-radius:8px;padding:20px;
            display:none}
  #fallback h2{font-size:15px;margin-bottom:8px}
  #fallback p{color:var(--dim);font-size:13px}
</style>
</head>
<body>
<header>
  <span class="logo">astockquant</span>
  <input id="sym" spellcheck="false" title="品种代码，回车打开">
  <span id="periods"></span>
  <span id="inds"></span>
  <span id="stat"><span class="dot" id="dot"></span><span id="sttxt">加载中…</span></span>
  <span id="price"><span class="b">–</span><span class="sep">/</span><span class="a">–</span></span>
</header>
<div id="chart"><div class="legend" id="maLeg"></div><div class="legend" id="bollLeg"></div></div>
<div id="fallback">
  <h2>图表库加载失败</h2>
  <p>页面需要从 unpkg.com 加载 lightweight-charts 图表库。请检查本机网络后刷新重试。</p>
</div>
<script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"
        onerror="document.getElementById('fallback').style.display='block'"></script>
<script>
(function(){
  if(!window.LightweightCharts) return;
  var q=new URLSearchParams(location.search);
  var token=q.get('token')||'';
  var symbol=q.get('symbol')||'ag';
  var period=(q.get('period')||'D1').toUpperCase();
  var PMAP={M1:1,M5:5,M15:15,M30:30,H1:60,H4:240,D1:1440,W1:10080,MN1:43200};
  document.title=symbol+' '+period+' \u00b7 astockquant';

  // ---- controls ----
  function go(){
    var n=new URLSearchParams();
    if(token) n.set('token',token);
    n.set('symbol',symIn.value.trim()||symbol);
    n.set('period',period);
    location.search=n.toString();
  }
  var symIn=document.getElementById('sym');
  symIn.value=symbol;
  symIn.addEventListener('keydown',function(e){if(e.key==='Enter')go();});
  var pbox=document.getElementById('periods');
  Object.keys(PMAP).forEach(function(p){
    var b=document.createElement('button');
    b.className='btn'+(p===period?' on':'');
    b.textContent=p;
    b.onclick=function(){period=p;go();};
    pbox.appendChild(b);
  });

  // ---- status / price readout ----
  var dot=document.getElementById('dot'),sttxt=document.getElementById('sttxt');
  function status(cls,txt){dot.className='dot '+cls;sttxt.textContent=txt;}
  var lastBid=null,bEl=null,aEl=null;
  function showTick(bid,ask){
    if(!bEl){bEl=document.querySelector('#price .b');
             aEl=document.querySelector('#price .a');}
    if(bid!=null&&lastBid!=null)
      bEl.style.color=bid>=lastBid?'var(--up)':'var(--dn)';
    bEl.textContent=bid==null?'\u2013':bid;
    aEl.textContent=ask==null?'\u2013':ask;
    if(bid!=null) lastBid=bid;
  }

  // ---- chart ----
  var box=document.getElementById('chart');
  var chart=LightweightCharts.createChart(box,{
    layout:{background:{type:'solid',color:'#0e1116'},textColor:'#8b95a3'},
    grid:{vertLines:{color:'#161c24'},horzLines:{color:'#161c24'}},
    rightPriceScale:{borderColor:'#232a33'},
    timeScale:{borderColor:'#232a33',timeVisible:true,secondsVisible:false,
               rightOffset:6},
    crosshair:{mode:LightweightCharts.CrosshairMode.Normal}
  });
  var candles=chart.addCandlestickSeries({
    upColor:'#e0524d',downColor:'#2ebd85',borderVisible:false,
    wickUpColor:'#e0524d',wickDownColor:'#2ebd85'
  });
  chart.priceScale('right').applyOptions(
    {scaleMargins:{top:0.08,bottom:0.08}});
  var maSeries={},MACOLS={20:'#c678dd',120:'#2ebd85'};
  [20,120].forEach(function(n){
    maSeries[n]=chart.addLineSeries({color:MACOLS[n],lineWidth:1,
      priceLineVisible:false,lastValueVisible:false});
  });
  var bollUp=chart.addLineSeries({color:'#4c8dff',lineWidth:1,
    priceLineVisible:false,lastValueVisible:false});
  var bollMid=chart.addLineSeries({color:'#f0b90b',lineWidth:1,
    priceLineVisible:false,lastValueVisible:false});
  var bollLo=chart.addLineSeries({color:'#4c8dff',lineWidth:1,
    priceLineVisible:false,lastValueVisible:false});
  var sarS=chart.addLineSeries({color:'#e8e8e8',lineWidth:1,
    lineVisible:false,pointMarkersVisible:true,pointMarkersRadius:1,
    priceLineVisible:false,lastValueVisible:false});
  // 左上角指标读数（跟随十字光标）
  var maLeg=document.getElementById('maLeg'),
      bollLeg=document.getElementById('bollLeg'),lastR=null;
  function f2(v){return v==null||isNaN(v)?'\u2013':(+v).toFixed(2);}
  function legVal(p,sr,arr){
    if(p&&p.time&&p.seriesData){
      var d=p.seriesData.get(sr);
      return d&&d.value!=null?d.value:null;
    }
    return arr&&arr.length?arr[arr.length-1]:null;
  }
  function drawLeg(p){
    if(!lastR){maLeg.textContent='';bollLeg.textContent='';return;}
    maLeg.innerHTML=
      '<span style="color:'+MACOLS[20]+'">MA20 '+
      f2(legVal(p,maSeries[20],lastR.ma20))+'</span> '+
      '<span style="color:'+MACOLS[120]+'">MA120 '+
      f2(legVal(p,maSeries[120],lastR.ma120))+'</span>';
    var parts=[];
    if(IND.boll)
      parts.push('<span style="color:#4c8dff">BOLL(26,2) '+
        f2(legVal(p,bollUp,lastR.bu))+'</span> '+
        '<span style="color:#f0b90b">'+f2(legVal(p,bollMid,lastR.bm))+
        '</span> '+
        '<span style="color:#4c8dff">'+f2(legVal(p,bollLo,lastR.bl))+
        '</span>');
    if(IND.sar)
      parts.push('<span style="color:#e8e8e8">SAR '+
        f2(legVal(p,sarS,lastR.sar))+'</span>');
    bollLeg.innerHTML=parts.length?parts.join(' '):'';
  }
  chart.subscribeCrosshairMove(drawLeg);
  function sizeChart(){
    chart.applyOptions({width:box.clientWidth,height:box.clientHeight});
  }
  sizeChart();
  window.addEventListener('resize',sizeChart);
  // ---- 指标计算（页面内公式） ----
  var closes=[],barsT=[],highs=[],lows=[],opens=[];
  function sma(a,n){
    var o=new Array(a.length).fill(null),s=0;
    for(var i=0;i<a.length;i++){s+=a[i];if(i>=n)s-=a[i-n];
      if(i>=n-1)o[i]=s/n;}
    return o;
  }
  function boll(a,n,k){
    var mid=sma(a,n),up=[],lo=[];
    for(var i=0;i<a.length;i++){
      if(mid[i]==null){up[i]=null;lo[i]=null;continue;}
      var s=0;
      for(var j=i-n+1;j<=i;j++){var d=a[j]-mid[i];s+=d*d;}
      var sd=Math.sqrt(s/n);
      up[i]=mid[i]+k*sd;lo[i]=mid[i]-k*sd;
    }
    return {mid:mid,up:up,lo:lo};
  }
  function psar(cl,hi,lo,step,max){
    var o=new Array(cl.length).fill(null);
    if(cl.length<2)return o;
    var up=cl[1]>=cl[0],sar=up?lo[0]:hi[0],
        ep=up?hi[0]:lo[0],af=step;
    o[1]=sar;
    for(var i=2;i<cl.length;i++){
      sar=sar+af*(ep-sar);
      if(up)sar=Math.min(sar,lo[i-1],lo[i-2]);
      else sar=Math.max(sar,hi[i-1],hi[i-2]);
      if(up&&lo[i]<sar){up=false;sar=ep;ep=lo[i];af=step;}
      else if(!up&&hi[i]>sar){up=true;sar=ep;ep=hi[i];af=step;}
      else if(up&&hi[i]>ep){ep=hi[i];af=Math.min(af+step,max);}
      else if(!up&&lo[i]<ep){ep=lo[i];af=Math.min(af+step,max);}
      o[i]=sar;
    }
    return o;
  }
  function computeAll(){
    var b=boll(closes,26,2);
    return {ma20:sma(closes,20),ma120:sma(closes,120),
            bm:b.mid,bu:b.up,bl:b.lo,
            sar:psar(closes,highs,lows,0.02,0.2)};
  }
  // Heiken Ashi: haC=(o+h+l+c)/4; haO_i=(haO+haC)_{i-1}/2; wick=range
  function haAll(){
    var o=[],i,n=closes.length,po=0,pc=0;
    for(i=0;i<n;i++){
      var hc=(opens[i]+highs[i]+lows[i]+closes[i])/4;
      var ho=i?(po+pc)/2:(opens[i]+closes[i])/2;
      po=ho;pc=hc;
      o.push({time:barsT[i],open:ho,close:hc,
              high:Math.max(highs[i],ho,hc),
              low:Math.min(lows[i],ho,hc)});
    }
    return o;
  }
  function rawAll(){
    var o=[],i;
    for(i=0;i<closes.length;i++)
      o.push({time:barsT[i],open:opens[i],high:highs[i],
              low:lows[i],close:closes[i]});
    return o;
  }
  function refeed(){
    candles.setData(closes.length?(IND.ha?haAll():rawAll()):[]);
  }
  var IND={ma:true,boll:true,sar:true,ha:false};
  try{var sv=localStorage.getItem('aqchart.ind');
      if(sv){sv=JSON.parse(sv);
        IND.ma=!!sv.ma;
        IND.boll=sv.boll==null?true:!!sv.boll;
        IND.sar=sv.sar==null?true:!!sv.sar;
        IND.ha=!!sv.ha;}}catch(e){}
  // visibility is implemented by (re)filling series data instead of
  // applyOptions({visible}) - toggling via setData is reliable across
  // lightweight-charts builds
  function fill(sr,a){
    if(!closes.length){sr.setData([]);return;}
    var o=[];
    for(var i=0;i<a.length;i++)
      if(a[i]!=null)o.push({time:barsT[i],value:a[i]});
    sr.setData(o);
  }
  function applyInd(){
    var r=closes.length?computeAll():null;
    lastR=r;
    if(IND.ma&&r){fill(maSeries[20],r.ma20);fill(maSeries[120],r.ma120);}
    else{[20,120].forEach(function(n){maSeries[n].setData([]);});}
    if(IND.boll&&r){fill(bollUp,r.bu);fill(bollMid,r.bm);
      fill(bollLo,r.bl);}
    else{bollUp.setData([]);bollMid.setData([]);bollLo.setData([]);}
    if(IND.sar&&r)fill(sarS,r.sar);else sarS.setData([]);
    drawLeg(null);
  }
  var indBox=document.getElementById('inds');
  [['ma','MA'],['boll','BOLL'],['sar','SAR'],['ha','HA']].forEach(function(kv){
    var b=document.createElement('button');
    b.className='btn';b.textContent=kv[1];b.dataset.k=kv[0];
    b.onclick=function(){toggleInd(kv[0]);};
    indBox.appendChild(b);
  });
  function toggleInd(k){
    IND[k]=!IND[k];
    try{localStorage.setItem('aqchart.ind',JSON.stringify(IND));}catch(e){}
    refreshIndBtns();applyInd();
    if(k==='ha')refeed();
  }
  function refreshIndBtns(){
    indBox.querySelectorAll('button').forEach(function(b){
      b.className='btn'+(IND[b.dataset.k]?' on':'');});
  }
  refreshIndBtns();applyInd();
  function indTail(){
    var r=computeAll(),i=closes.length-1;
    if(i<0)return;
    lastR=r;
    var t=barsT[i];
    function u(sr,v,cf){
      if(v==null)return;
      var p={time:t,value:v};if(cf)p.color=cf;sr.update(p);
    }
    u(maSeries[20],r.ma20[i]);u(maSeries[120],r.ma120[i]);
    u(bollUp,r.bu[i]);u(bollMid,r.bm[i]);u(bollLo,r.bl[i]);
    u(sarS,r.sar[i]);
    drawLeg(null);
  }
  function upd(b){
    if(barsT.length&&b.time===barsT[barsT.length-1]){
      closes[closes.length-1]=b.close;
      highs[highs.length-1]=b.high;
      lows[lows.length-1]=b.low;
      opens[opens.length-1]=b.open;
    }else{
      barsT.push(b.time);closes.push(b.close);
      highs.push(b.high);lows.push(b.low);opens.push(b.open);
    }
    if(IND.ha){
      var ha=haAll();
      if(ha.length)candles.update(ha[ha.length-1]);
    }else{
      candles.update({time:b.time,open:b.open,high:b.high,
                      low:b.low,close:b.close});
    }
    if(b.close!=null&&b.time)indTail();
  }

  // ---- history ----
  function qs(o){
    var a=[];for(var k in o)a.push(k+'='+encodeURIComponent(o[k]));
    return a.join('&');
  }
  fetch('/bars?'+qs({symbol:symbol,period:period,limit:500,token:token}))
   .then(function(r){return r.json();})
   .then(function(j){
      if(!j.ok){status('err',j.error||'加载失败');return;}
      var bars=j.data.bars||[];
      if(!bars.length){status('err','无数据（检查品种代码）');openStream();return;}
      closes=bars.map(function(b){return b.close;});
      barsT=bars.map(function(b){return b.time;});
      highs=bars.map(function(b){return b.high;});
      lows=bars.map(function(b){return b.low;});
      opens=bars.map(function(b){return b.open;});
      refeed();
      applyInd();
      chart.timeScale().fitContent();
      var vr=chart.timeScale().getVisibleLogicalRange();
      if(vr)chart.timeScale().setVisibleLogicalRange(
          {from:vr.from,to:vr.to+6});
      status('warn','连接实时流…');
      openStream();
   })
   .catch(function(e){status('err','/bars 请求失败');});

  // ---- realtime (SSE) ----
  function openStream(){
    var es=new EventSource('/stream?'+qs({symbols:symbol,periods:period,
                                          ticks:1,bars:1,token:token}));
    es.onopen=function(){status('ok','实时');};
    es.onerror=function(){status('warn','重连中…');};
    es.addEventListener('snapshot',function(e){
      (JSON.parse(e.data)||[]).forEach(function(s){
        if(s.symbol===symbol&&s.period===PMAP[period]&&s.bar) upd(s.bar);
      });
    });
    es.addEventListener('bar',function(e){
      var m=JSON.parse(e.data);
      if(m.symbol===symbol&&m.period===PMAP[period]) upd(m.bar);
    });
    es.addEventListener('tick',function(e){
      var t=JSON.parse(e.data);
      showTick(t.bid,t.ask);
    });
  }
})();
</script>
</body>
</html>
"""


class Session:
    """Keeps the quote connection alive; lazy trade connection."""

    def __init__(self, host, port, login, password, log,
                 symbols=("ag",), hdb=None):
        self.host, self.port = host, port
        self.login, self.password = login, password
        self.log = log
        self.symbols = tuple(symbols)
        self.hdb = hdb
        self.lock = threading.RLock()
        self.cli = None
        self.tc = None
        self.t0 = time.time()
        self.stop = False
        self.last_err = ""
        self.quote_reconnects = 0
        self.trade_reconnects = 0
        self.trade_ok_total = 0
        self.trade_fail_total = 0
        self._bar_req_lock = threading.Lock()
        self._bar_req_meta = {}
        # (name, period) keys whose full history has been pulled at
        # least once on the CURRENT quote session; the live 0x9c
        # subscription also pushes the forming bar into cli.bars, so
        # "cache non-empty" alone must not count as history present
        self._full_pulled = set()

    def _save_bars(self, name, period, bars):
        """Persist a pulled bar batch; never let a db hiccup break
        the quote path."""
        if self.hdb is None or not bars:
            return
        try:
            self.hdb.put(name, period, bars)
        except Exception as e:
            self.log("history db write failed: %r" % e)

    # ------------- quote session -------------
    def start(self):
        self._connect_quote()

    def _connect_quote(self):
        cli = AQClient(self.host, self.port, self.login, self.password)
        cli.connect()
        # full init token sequence (gold order); zero cache hashes
        # force the server to push full tables every connect
        now = int(time.time())
        cli.send_enc(0x10)
        cli.expect += [("ack10", 0x10)]
        cli.send_enc(0x08, b"\x00" * 16)
        cli.expect += [("ack_frame", 0x08)]
        cli.send_enc(0x0A, b"\x00" * 16)
        cli.expect += [("ack_frame", 0x0A)]
        cli.send_enc(0x21)
        cli.expect += [("ack21", 0x21)]
        cli.send_enc(0x22, struct.pack("<II", 0, now))
        cli.expect += [("ack_frame", 0x22)]
        cli.send_enc(0x1E, struct.pack("<I", 0))
        cli.expect += [("ack_frame", 0x1E)]
        cli.send_enc(0x96, struct.pack("<H", 1024)
                     + b"".join(struct.pack("<H", i)
                                for i in range(1024)))
        # per-symbol quote subscription -> 0x97 ticks
        body = b"".join(
            s.encode("latin-1")[:12].ljust(12, b"\x00")
            + struct.pack("<IIH", 1, now, 0) for s in self.symbols)
        cli.send_enc(0x9C, struct.pack("<H", len(self.symbols)) + body)
        with self.lock:
            self.cli = cli
            self._full_pulled.clear()
            self.quote_reconnects += 1
        # start workers only after self.cli is set: _pump_loop's first
        # check is "self.cli is not cli -> return" (startup race)
        threading.Thread(target=cli.ping_loop, daemon=True).start()
        threading.Thread(target=self._pump_loop, args=(cli,),
                         daemon=True).start()
        self.log("quote session up (login=%d)" % self.login)

    def _pump_loop(self, cli):
        """Drain the quote socket; rebuild the session on any drop."""
        while not self.stop:
            if self.cli is not cli:
                return
            try:
                cli.sock.settimeout(0.25)
                with self.lock:
                    cli._pump()
            except (ConnectionError, OSError) as e:
                if self.stop or self.cli is not cli:
                    return
                self.last_err = repr(e)
                self.log("quote link down: %r - reconnecting" % e)
                try:
                    cli.sock.close()
                except OSError:
                    pass
                with self.lock:
                    if self.cli is cli:
                        self.cli = None
                        self.tc = None
                while not self.stop and self.cli is None:
                    time.sleep(2.0)
                    try:
                        self._connect_quote()
                    except Exception as e2:
                        self.last_err = repr(e2)
                        self.log("reconnect failed: %r" % e2)

    # ------------- trade connection -------------
    def _wait_quote(self, timeout):
        """等行情会话重建；行情断线时由 _pump_loop 负责重连。"""
        t_end = time.time() + timeout
        while time.time() < t_end:
            with self.lock:
                if self.cli is not None:
                    return True
            time.sleep(0.2)
        with self.lock:
            return self.cli is not None

    def _trade(self):
        """Return a live TradeConn, reconnecting once if needed."""
        if self.tc is not None:
            return self.tc
        if self.cli is None:
            # 行情会话不在时无法建交易连接；抛可重试错误而不是 AttributeError
            raise ConnectionError("quote session down")
        tc = TradeConn(self.cli)
        tc.connect()
        self.tc = tc
        return tc

    def _drop_trade(self):
        if self.tc is not None:
            try:
                self.tc.close()
            except OSError:
                pass
            self.tc = None

    def trade_request(self, body, tag):
        """One 0xbe request on the trade conn, with automatic reconnect.

        分两段处理，避免重复下单：
          1) 建连阶段（可安全重试）：先探测旧连接是否已被上游关闭，
             关闭则丢弃并重建，最多 3 次，期间等行情会话恢复。
          2) 发送阶段（不重发）：请求一旦发出，若连接中途断开，
             本笔的实际状态未知，只丢弃连接并报错，由调用方查询核对。
        """
        attempts = 3
        last = None
        tc = None
        for attempt in range(1, attempts + 1):
            try:
                with self.lock:
                    if self.tc is not None and not self.tc.is_alive():
                        self.log("trade conn closed by upstream, rebuilding")
                        self._drop_trade()
                        self.trade_reconnects += 1
                    tc = self._trade()
                break
            except (ConnectionError, OSError, RuntimeError) as e:
                last = e
                self.last_err = repr(e)
                self.trade_reconnects += 1
                self.log("trade connect error (%r) attempt %d/%d"
                         % (e, attempt, attempts))
                with self.lock:
                    self._drop_trade()
                if attempt == attempts:
                    self.trade_fail_total += 1
                    raise ConnectionError(
                        "trade connect failed after %d attempts: %r"
                        % (attempts, last))
                # 行情会话可能同时断线，先等它就绪再重建交易连接
                self._wait_quote(min(2.0 * attempt, 6.0))
                time.sleep(0.4 * attempt)

        try:
            with self.lock:
                recs, ev = tc.request(body, tag=tag)
        except (ConnectionError, OSError) as e:
            # 请求已发送，重发会造成重复委托；标记状态未知后交给调用方核对
            self.last_err = repr(e)
            self.trade_fail_total += 1
            self.log("trade conn lost after send (%r) - order state unknown" % e)
            with self.lock:
                self._drop_trade()
                self.trade_reconnects += 1
            raise ConnectionError(
                "order state unknown (conn lost after send), "
                "check /orders before retry: %r" % e)

        for r in recs:
            with self.lock:
                cli = self.cli
                if cli is None:
                    break
                cli._upsert_trade(r)
        self.trade_ok_total += 1
        return recs, ev

    # ------------- operations -------------
    def wait_ready(self, timeout=10.0):
        t_end = time.time() + timeout
        while time.time() < t_end:
            with self.lock:
                if self.cli is not None and self.cli.account:
                    return True
            time.sleep(0.2)
        return False

    def op_open(self, symbol, cmd, lots, comment=""):
        body = self.cli.trade_body(TT_OPEN, cmd, symbol, lots,
                                   price=0.0, comment=comment)
        recs, ev = self.trade_request(body, "OPEN %s %s %.2f"
                                      % (symbol, "buy" if cmd == 0
                                         else "sell", lots))
        return self._result(recs, ev)

    def op_close(self, ticket):
        with self.lock:
            rec = next((p for p in self.cli.positions
                        if p["ticket"] == ticket), None)
            lots = rec["lots"] if rec else 0.0
            cmd = rec["cmd"] if rec else 0
            sym = rec["symbol"] if rec else "ag"
        if lots <= 0:
            raise ValueError("no open position #%d" % ticket)
        body = self.cli.trade_body(TT_CLOSE_MKT, cmd, sym, lots,
                                   price=0.0, order=ticket)
        recs, ev = self.trade_request(body, "CLOSE #%d mkt" % ticket)
        if ev and ev[-1] == ("error", 0x03):
            with self.lock:
                px = self.cli._close_price(rec) if rec else 0.0
            body = self.cli.trade_body(TT_CLOSE, cmd, sym, lots,
                                       price=px, order=ticket)
            recs, ev = self.trade_request(body, "CLOSE #%d inst"
                                          % ticket)
        return self._result(recs, ev)

    def op_close_all(self):
        # NOTE: wire 0x4a ("close all of symbol", FUN_0044dd30) executes
        # per server log but its reply layout is unverified (requests
        # time out client-side) and an empty symbol body is rejected
        # 0x03.  Iterating 0x46 closes is fully parsed and safe.
        out = []
        with self.lock:
            targets = [(p["ticket"], p["cmd"], p["symbol"], p["lots"])
                       for p in self.cli.positions]
        for ticket, cmd, sym, lots in targets:
            body = self.cli.trade_body(TT_CLOSE_MKT, cmd, sym, lots,
                                       price=0.0, order=ticket)
            try:
                recs, ev = self.trade_request(
                    body, "CLOSE #%d" % ticket)
            except (ConnectionError, OSError) as e:
                out.append({"ticket": ticket, "ok": False,
                            "error": repr(e)})
                continue
            r = self._result(recs, ev)
            r["ticket"] = ticket
            out.append(r)
        ok = all(r["ok"] for r in out) if out else True
        return {"ok": ok, "error": None if ok else "partial failure",
                "results": out}

    def op_modify(self, ticket, sl, tp):
        with self.lock:
            rec = next((p for p in self.cli.positions
                        if p["ticket"] == ticket), None)
        if rec is None:
            raise ValueError("no open position #%d" % ticket)
        body = self.cli.trade_body(TT_MODIFY, rec["cmd"], rec["symbol"],
                                   rec["lots"], price=0.0,
                                   sl=sl, tp=tp, order=ticket)
        recs, ev = self.trade_request(body, "MODIFY #%d" % ticket)
        return self._result(recs, ev)

    # ------------- M8 pending orders -------------
    def op_place(self, symbol, cmd, lots, price, sl=0.0, tp=0.0,
                 expiration=0, comment=""):
        body = self.cli.trade_body(TT_PEND, cmd, symbol, lots,
                                   price=price, sl=sl, tp=tp,
                                   comment=comment, expiration=expiration)
        recs, ev = self.trade_request(
            body, "PEND %s cmd=%d %.2f@%.3f" % (symbol, cmd, lots, price))
        return self._result(recs, ev)

    def op_cancel(self, ticket):
        with self.lock:
            rec = next((o for o in self.cli.orders
                        if o["ticket"] == ticket), None)
        if rec is None:
            raise ValueError("no pending order #%d" % ticket)
        body = self.cli.trade_body(TT_DELETE, rec["cmd"], rec["symbol"],
                                   rec["lots"], price=0.0,
                                   order=ticket)
        recs, ev = self.trade_request(body, "CANCEL #%d" % ticket)
        r = self._result(recs, ev)
        # the server sends no record frame on the trade conn for delete
        # (the 0x9b notify on the quote conn is authoritative); converge the
        # local view optimistically, truth re-syncs on reconnect.
        if r["ok"] and not recs:
            with self.lock:
                self.cli.orders = [o for o in self.cli.orders
                                   if o["ticket"] != ticket]
        return r

    def op_modify_pending(self, ticket, price, sl=0.0, tp=0.0,
                          expiration=0):
        with self.lock:
            rec = next((o for o in self.cli.orders
                        if o["ticket"] == ticket), None)
        if rec is None:
            raise ValueError("no pending order #%d" % ticket)
        body = self.cli.trade_body(TT_MODIFY, rec["cmd"], rec["symbol"],
                                   rec["lots"], price=price, sl=sl,
                                   tp=tp, order=ticket,
                                   expiration=expiration)
        recs, ev = self.trade_request(
            body, "MODPEND #%d @%.3f" % (ticket, price))
        return self._result(recs, ev)

    def _result(self, recs, ev):
        last = ev[-1] if ev else ("timeout", 0)
        ok = last[0] == "final" and last[1] == "ok"
        err = None if ok else "%s:%s" % (last[0], last[1])
        return {"ok": ok, "error": err, "records": recs, "events": ev}

    # ------------- chart bars -------------
    def op_bars(self, symbol, period, from_ts=0, limit=0, refresh=False):
        # NOTE: the wait must happen OUTSIDE self.lock - the pump thread
        # holds that RLock while draining the socket and is the one that
        # signals the request via _ingest_bars; waiting locked = deadlock.
        with self.lock:
            cli = self.cli
            if cli is None:
                raise ConnectionError("disconnected")
            pulled = (symbol, period) in self._full_pulled
        if refresh or not pulled:
            bars = cli.request_bars(symbol, period)
            self._save_bars(symbol, period, bars)
            with self.lock:
                self._full_pulled.add((symbol, period))
        else:
            bars = cli.get_bars(symbol, period)
        if from_ts:
            bars = [b for b in bars if b["time"] >= from_ts]
        if limit:
            bars = bars[-limit:]
        return bars

    # ------------- SSE streaming -------------
    def stream_client(self):
        with self.lock:
            return self.cli

    def refresh_bars(self, cli, name, period, min_age=2.0):
        """Re-pull a chart key, coalescing concurrent callers and
        skipping when the last request is younger than min_age.  The
        server includes the currently-forming bar in its history answer
        but does not stream per-tick bar updates, so SSE feeds rely on
        this short-interval poll driven by incoming ticks."""
        with self._bar_req_lock:
            meta = self._bar_req_meta.setdefault((name, period), {})
            age = time.time() - meta.get("t", 0.0)
            if age >= min_age or not cli.bars.get((name, period)):
                meta["t"] = time.time()
                do_req = True
            else:
                do_req = False
        if do_req:
            bars = cli.request_bars(name, period)
            self._save_bars(name, period, bars)
            return bars
        return cli.get_bars(name, period)


def _js(handler, code, obj):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    # the /chart page is served from this origin but may also be opened
    # from file:// or other ports; simple GETs need no CORS preflight
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    gw = None          # set by main()
    token = None
    server_version = "astockquant/1.0"
    sys_version = ""

    def log_message(self, fmt, *args):
        pass

    def _auth(self, q=None):
        if not self.token:
            return True
        # EventSource cannot set headers, so allow ?token= for /stream.
        return (self.headers.get("X-Auth-Token") == self.token
                or (q or {}).get("token") == self.token)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        return json.loads(self.rfile.read(n).decode("utf-8"))

    # ---------------- chart page ----------------
    def _handle_chart(self):
        body = _CHART_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_chartsvg(self, q):
        s = self.gw
        with s.lock:
            connected = s.cli is not None
        if not connected:
            return _js(self, 503, {"ok": False, "error": "disconnected"})
        name = q.get("symbol", "ag")
        pspec = q.get("period", "D1").upper()
        period = PERIODS.get(pspec)
        if period is None:
            if pspec.isdigit():
                period = int(pspec)
            else:
                return _js(self, 400, {"ok": False, "error":
                    "bad period; use one of %s or minutes"
                    % ",".join(PERIODS)})
        try:
            limit = int(q.get("limit", "500") or "500")
        except ValueError:
            return _js(self, 400, {"ok": False, "error": "bad limit"})
        try:
            bars = s.op_bars(name, period, 0, limit, False)
        except Exception as e:
            return _js(self, 502, {"ok": False, "error": repr(e)})
        body = _svg_chart(name, pspec, bars).encode("utf-8")
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------------- GET ----------------
    def _handle_bars(self, q):
        s = self.gw
        with s.lock:
            connected = s.cli is not None
        if not connected:
            return _js(self, 503, {"ok": False, "error": "disconnected"})
        name = q.get("symbol", "ag")
        pspec = q.get("period", "D1").upper()
        period = PERIODS.get(pspec)
        if period is None:
            if pspec.isdigit():
                period = int(pspec)
            else:
                return _js(self, 400, {"ok": False, "error":
                    "bad period; use one of %s or minutes"
                    % ",".join(PERIODS)})
        try:
            from_ts = int(q.get("from", "0") or "0")
            limit = int(q.get("limit", "0") or "0")
        except ValueError:
            return _js(self, 400, {"ok": False, "error": "bad from/limit"})
        refresh = q.get("refresh", "") in ("1", "true", "yes")
        bars = s.op_bars(name, period, from_ts, limit, refresh)
        return _js(self, 200, {"ok": True, "data": {
            "symbol": name, "period": period,
            "count": len(bars), "bars": bars}})

    def _handle_site(self, path):
        """Serve static site files from the configured site dir."""
        import mimetypes
        root = getattr(self.gw, "site_dir", None) or os.path.join(
            os.path.dirname(__file__), "aqnt-site")
        rel = path[len("/site"):]
        if rel in ("", "/"):
            rel = "/index.html"
        rel = rel.lstrip("/")
        # prevent path traversal
        real = os.path.normpath(os.path.join(root, rel))
        if not real.startswith(root):
            return _js(self, 403, {"ok": False, "error": "forbidden"})
        if not os.path.isfile(real):
            return _js(self, 404, {"ok": False, "error": "not found"})
        body = open(real, "rb").read()
        ctype, _ = mimetypes.guess_type(real)
        if ctype is None:
            ctype = "application/octet-stream"
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", ctype + ("; charset=utf-8"
             if ctype.startswith("text/") or "javascript" in ctype
             else ""))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_klines(self, q):
        s = self.gw
        if s.hdb is None:
            return _js(self, 503, {"ok": False,
                                   "error": "history db unavailable"})
        name = q.get("symbol", "")
        if not name:
            return _js(self, 400, {"ok": False, "error": "symbol required"})
        pspec = q.get("period", "M15")
        period = PERIODS.get(pspec.upper())
        if period is None:
            if pspec.isdigit():
                period = int(pspec)
            else:
                return _js(self, 400, {"ok": False, "error":
                    "bad period; use one of %s or minutes"
                    % ",".join(PERIODS)})
        try:
            from_ts = int(q.get("from", "0") or "0")
            to_ts = int(q.get("to", "0") or "0")
            limit = int(q.get("limit", "0") or "0")
        except ValueError:
            return _js(self, 400, {"ok": False,
                                   "error": "bad from/to/limit"})
        bars = s.hdb.get(name, period, from_ts, to_ts, limit)
        return _js(self, 200, {"ok": True, "data": {
            "symbol": name, "period": period, "source": "local",
            "count": len(bars), "bars": bars}})

    def _handle_history_export(self, q):
        s = self.gw
        if s.hdb is None:
            return _js(self, 503, {"ok": False,
                                   "error": "history db unavailable"})
        name = q.get("symbol", "")
        if not name:
            return _js(self, 400, {"ok": False, "error": "symbol required"})
        pspec = q.get("period", "M15")
        period = PERIODS.get(pspec.upper())
        if period is None:
            if pspec.isdigit():
                period = int(pspec)
            else:
                return _js(self, 400, {"ok": False, "error":
                    "bad period; use one of %s or minutes"
                    % ",".join(PERIODS)})
        try:
            from_ts = int(q.get("from", "0") or "0")
            to_ts = int(q.get("to", "0") or "0")
        except ValueError:
            return _js(self, 400, {"ok": False, "error": "bad from/to"})
        rows = s.hdb.rows(name, period, from_ts, to_ts)
        pname = pspec.upper() if pspec.upper() in PERIODS else str(period)
        fname = "%s_%s.csv" % (re.sub(r"[^A-Za-z0-9._-]", "_", name), pname)
        out = ["\ufefftime,datetime,open,high,low,close,volume"]
        for r in rows:
            out.append("%d,%s,%s,%s,%s,%s,%d" % (
                r[0],
                time.strftime("%Y-%m-%d %H:%M", time.localtime(r[0])),
                r[1], r[2], r[3], r[4], r[5]))
        body = ("\r\n".join(out) + "\r\n").encode("utf-8")
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Disposition",
                         "attachment; filename=\"%s\"" % fname)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------------- SSE ----------------
    def _parse_periods(self, spec):
        out = []
        for p in spec.upper().split(","):
            p = p.strip()
            if not p:
                continue
            per = PERIODS.get(p)
            if per is None:
                if p.isdigit():
                    per = int(p)
                else:
                    return None, p
            out.append(per)
        return out, None

    def _handle_stream(self, q):
        s = self.gw
        want_ticks = q.get("ticks", "1") not in ("0", "false", "no")
        want_bars = q.get("bars", "1") not in ("0", "false", "no")
        symbols = [x.strip() for x in
                   q.get("symbols", ",".join(s.symbols)).split(",")
                   if x.strip()]
        periods, bad = self._parse_periods(q.get("periods", "M1"))
        if bad is not None:
            return _js(self, 400, {"ok": False, "error":
                "bad period %r; use %s" % (bad, ",".join(PERIODS))})
        symset = set(symbols)
        barkeys = [(n, p) for n in symbols for p in periods]

        cli = s.stream_client()
        if cli is None:
            return _js(self, 503, {"ok": False, "error": "disconnected"})

        evq = queue.Queue(maxsize=4000)
        dirty = set()
        dirty_lock = threading.Lock()

        def push(kind, obj):
            try:
                evq.put_nowait((kind, obj))
            except queue.Full:
                pass                       # drop backpressure, never block pump

        ids = []
        if want_ticks:
            def on_t(t):
                if not symset or t["symbol"] in symset:
                    with dirty_lock:
                        dirty.add(t["symbol"])
                    push("tick", t)
            ids.append(cli.on_tick(on_t))
        if want_bars:
            ids.append(cli.on_bar(
                lambda n, p, b, new: push("bar", {
                    "symbol": n, "period": p, "new": bool(new), "bar": b})
                if (n, p) in set(barkeys) else None))

        # Seed each chart key once (flags=0) so the cache and the
        # server-side subscription exist; the bulk answer is suppressed
        # from listeners while this waiter is active.
        snap = []
        emitted = {}
        if want_bars:
            for n, p in barkeys:
                try:
                    if not cli.bars.get((n, p)):
                        cli.request_bars(n, p)
                    cached = cli.get_bars(n, p)
                    if cached:
                        s._save_bars(n, p, cached)
                        last = cached[-1]
                        snap.append({"symbol": n, "period": p, "bar": last})
                        emitted[(n, p)] = last["time"]
                except Exception as e:
                    s.log("stream subscribe %s/%d failed: %r" % (n, p, e))

        # Server pushes no per-tick bar frames: when ticks move a symbol,
        # re-pull that symbol's charts and emit changed forming bars.
        stop = threading.Event()

        def bar_refresher():
            empty_tried = {}
            while not stop.wait(3.0):
                with dirty_lock:
                    syms = set(dirty)
                    dirty.clear()
                for n, p in barkeys:
                    if n not in syms:
                        continue
                    if not cli.bars.get((n, p)):
                        if time.time() - empty_tried.get((n, p), 0) < 30:
                            continue
                        empty_tried[(n, p)] = time.time()
                    try:
                        bars = s.refresh_bars(cli, n, p)
                    except Exception:
                        continue
                    if not bars:
                        continue
                    last = bars[-1]
                    prev_t = emitted.get((n, p))
                    if prev_t != last["time"]:
                        emitted[(n, p)] = last["time"]
                        push("bar", {"symbol": n, "period": p,
                                     "new": prev_t is None
                                     or last["time"] > prev_t,
                                     "bar": last})

        if want_bars:
            threading.Thread(target=bar_refresher, daemon=True).start()

        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        # HTTP/1.0 (BaseHTTPRequestHandler default): body is delimited by
        # connection close, which every EventSource/urllib client handles.
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        def sse(kind, obj):
            self.wfile.write(("event: %s\ndata: %s\n\n"
                              % (kind, json.dumps(obj, ensure_ascii=False)))
                             .encode("utf-8"))
            self.wfile.flush()

        try:
            sse("hello", {"time": int(time.time()), "ticks": want_ticks,
                          "bars": want_bars, "symbols": symbols,
                          "periods": periods})
            sse("snapshot", snap)      # always sent; may be an empty list
            while True:
                try:
                    kind, obj = evq.get(timeout=15.0)
                    sse(kind, obj)
                except queue.Empty:
                    if s.stream_client() is not cli:
                        sse("error", {"reason": "session reconnected; "
                                                "reopen the stream"})
                        return
                    self.wfile.write(b": hb\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError,
                OSError):
            pass                       # client gone
        finally:
            stop.set()
            for qid in ids:
                cli.remove_listener(qid)

    def _handle_backtest(self):
        """POST /backtest - run the local backtrader engine over the
        history db; blocks a few seconds per run, no session lock held
        (sqlite WAL allows the concurrent read)."""
        req = self._body()
        try:
            import backtest
        except ImportError as e:
            return _js(self, 503, {"ok": False,
                                   "error": "backtrader not installed "
                                            "(%r)" % e})

        def day(v):
            return int(time.mktime(time.strptime(str(v), "%Y-%m-%d"))) \
                if v else 0

        try:
            data = backtest.run_backtest(
                symbol=str(req.get("symbol", "ag")),
                period=req.get("period", "M15"),
                strategy=str(req.get("strategy", "sma_cross")),
                fast=int(req.get("fast") or 20),
                slow=int(req.get("slow") or 120),
                lots=int(req.get("lots") or 1),
                cash=float(req.get("cash") or 100000.0),
                t0=day(req.get("from", "")),
                t1=day(req.get("to", "")),
                comm=req.get("comm"), margin=req.get("margin"),
                mult=req.get("mult"),
                include_equity=bool(req.get("equity")))
        except ValueError as e:
            return _js(self, 400, {"ok": False, "error": str(e)})
        except Exception as e:
            return _js(self, 500, {"ok": False, "error": repr(e)})
        return _js(self, 200, {"ok": True, "data": data})

    def do_GET(self):
        s = self.gw
        path, _, qs = self.path.partition("?")
        q = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
        try:
            skip_auth = path.startswith("/site")
            if not skip_auth and not self._auth(q):
                return _js(self, 401, {"ok": False, "error": "unauthorized"})
            # /chart, /bars and /stream must NOT hold s.lock: /chart is a
            # static page, and the other two wait while the pump thread
            # needs that same lock to receive/emit frames.
            if path == "/chart":
                return self._handle_chart()
            if path == "/chartsvg":
                return self._handle_chartsvg(q)
            if path.startswith("/site"):
                return self._handle_site(path)
            if path == "/stream":
                return self._handle_stream(q)
            if path == "/bars":
                return self._handle_bars(q)
            if path == "/klines":
                return self._handle_klines(q)
            if path == "/history-export":
                return self._handle_history_export(q)
            with s.lock:
                cli = s.cli
                if path == "/health":
                    return _js(self, 200, {"ok": True, "data": {
                        "connected": cli is not None,
                        "login": s.login,
                        "uptime_s": round(time.time() - s.t0, 1),
                        "last_error": s.last_err,
                        "quote_reconnects": s.quote_reconnects,
                        "trade_reconnects": s.trade_reconnects,
                        "trade_ok_total": s.trade_ok_total,
                        "trade_fail_total": s.trade_fail_total,
                        "positions": len(cli.positions) if cli else 0,
                        "orders": len(cli.orders) if cli else 0,
                        "ticks": cli.n_ticks if cli else 0,
                        "history_rows": s.hdb.count() if s.hdb else 0,
                        "history_db_bytes":
                            os.path.getsize(s.hdb.path)
                            if s.hdb and os.path.exists(s.hdb.path)
                            else 0}})
                if cli is None:
                    return _js(self, 503, {"ok": False,
                                           "error": "disconnected"})
                if path == "/account":
                    acc = cli.get_account()
                    return _js(self, 200, {"ok": True, "data": acc})
                if path == "/positions":
                    return _js(self, 200, {"ok": True,
                                           "data": cli.get_positions()})
                if path == "/orders":
                    return _js(self, 200, {"ok": True,
                                           "data": cli.get_orders()})
                if path == "/history":
                    lim = int(q.get("limit", MAX_HISTORY))
                    return _js(self, 200,
                               {"ok": True,
                                "data": cli.get_history()[-lim:]})
                if path == "/news":
                    return _js(self, 200, {"ok": True,
                                           "data": cli.get_news()})
                if path == "/symbols":
                    return _js(self, 200, {"ok": True,
                                           "data": cli.sym_names})
                if path == "/tick":
                    name = q.get("symbol", "ag")
                    idx = next((i for i, n in cli.sym_names.items()
                                if n == name), None)
                    t = cli.last_tick.get(idx) if idx is not None else None
                    if t is None:
                        return _js(self, 404, {"ok": False,
                                               "error": "no tick for %r"
                                               % name})
                    return _js(self, 200, {"ok": True, "data": {
                        "symbol": name,
                        "time": t[0],
                        "bid": t[1], "ask": t[2],
                        "age_s": round(time.time() - t[0], 1)}})
            return _js(self, 404, {"ok": False, "error": "no route"})
        except Exception as e:
            return _js(self, 500, {"ok": False, "error": repr(e)})

    # ---------------- POST ----------------
    def do_POST(self):
        s = self.gw
        try:
            if self.path.startswith("/api/"):
                return _js(self, 404, {"ok": False, "error": "no route"})
            if not self._auth():
                return _js(self, 401, {"ok": False, "error": "unauthorized"})
            if self.path == "/reconnect":
                with s.lock:
                    s._drop_trade()
                    old = s.cli
                    s.cli = None
                if old is not None:
                    try:
                        old.sock.close()
                    except OSError:
                        pass
                # the old pump thread exits on "self.cli is not cli",
                # so rebuild the session right here
                try:
                    s._connect_quote()
                except Exception as e:
                    s.last_err = repr(e)
                    return _js(self, 502,
                               {"ok": False, "error": repr(e)})
                s.wait_ready(10.0)
                return _js(self, 200, {"ok": s.cli is not None})
            if self.path == "/debug/inject":
                # M7: local test hook to inject a news record without
                # needing the wire; gated by the same auth as /order
                req = json.loads(self.rfile.read(
                    int(self.headers.get("Content-Length", 0))))
                cli = s.cli
                if cli is None:
                    return _js(self, 502, {"ok": False,
                                           "error": "not connected"})
                rec = {"id": len(cli.news) + 1,
                       "time": int(time.time()),
                       "category": str(req.get("category", "test"))[:31],
                       "subject": str(req.get("subject", ""))[:255],
                       "language": int(req.get("language", 0)),
                       "keywords": str(req.get("keywords", ""))[:63],
                       "extra": str(req.get("extra", ""))[:255],
                       "body": str(req.get("body", ""))[:4096]}
                cli.news.append(rec)
                return _js(self, 200, {"ok": True, "id": rec["id"]})
            if self.path == "/backtest":
                return self._handle_backtest()
            if self.path != "/order":
                return _js(self, 404, {"ok": False, "error": "no route"})
            if s.cli is None:
                return _js(self, 503, {"ok": False,
                                       "error": "disconnected"})
            req = self._body()
            act = req.get("action", "")
            if act == "open":
                cmd = req.get("cmd", "buy")
                if isinstance(cmd, str):
                    cmd = {"buy": 0, "sell": 1}.get(cmd.lower(), -1)
                if cmd not in (0, 1):
                    return _js(self, 400, {"ok": False,
                                           "error": "cmd must be buy/sell"})
                lots = float(req.get("lots", 0))
                if lots <= 0:
                    return _js(self, 400, {"ok": False,
                                           "error": "lots must be > 0"})
                r = s.op_open(req.get("symbol", "ag"), cmd, lots,
                              str(req.get("comment", ""))[:31])
                return _js(self, 200 if r["ok"] else 502, r)
            if act == "close":
                if req.get("all"):
                    return _js(self, 200, s.op_close_all())
                t = int(req.get("ticket") or 0)
                if t <= 0:
                    return _js(self, 400, {"ok": False,
                                           "error": "ticket required"})
                r = s.op_close(t)
                return _js(self, 200 if r["ok"] else 502, r)
            if act == "modify":
                t = int(req.get("ticket") or 0)
                if t <= 0:
                    return _js(self, 400, {"ok": False,
                                           "error": "ticket required"})
                sl = float(req.get("sl", 0) or 0)
                tp = float(req.get("tp", 0) or 0)
                r = s.op_modify(t, sl, tp)
                return _js(self, 200 if r["ok"] else 502, r)
            if act == "place":
                cmd = req.get("cmd", "")
                if isinstance(cmd, str):
                    cmd = PEND_CMDS.get(cmd.lower(), -1)
                if cmd not in (2, 3, 4, 5):
                    return _js(self, 400, {"ok": False,
                        "error": "cmd must be buy_limit/sell_limit/"
                                 "buy_stop/sell_stop (2..5)"})
                lots = float(req.get("lots", 0))
                if lots <= 0:
                    return _js(self, 400, {"ok": False,
                                           "error": "lots must be > 0"})
                price = float(req.get("price", 0) or 0)
                if price <= 0:
                    return _js(self, 400, {"ok": False,
                        "error": "price required > 0 for pending"})
                sl = float(req.get("sl", 0) or 0)
                tp = float(req.get("tp", 0) or 0)
                expiration = int(req.get("expiration", 0) or 0)
                r = s.op_place(req.get("symbol", "ag"), cmd, lots, price,
                               sl, tp, expiration,
                               str(req.get("comment", ""))[:31])
                return _js(self, 200 if r["ok"] else 502, r)
            if act == "cancel":
                t = int(req.get("ticket") or 0)
                if t <= 0:
                    return _js(self, 400, {"ok": False,
                                           "error": "ticket required"})
                try:
                    r = s.op_cancel(t)
                    if not r.get("ok") and "timeout" in str(r.get("error", "")):
                        # 0x48 撤单服务端只回进度帧、不回最终记录；超时后若挂单表里
                        # 已查不到该 ticket，说明撤单实际已生效，按成功处理
                        with s.lock:
                            gone = not any(o["ticket"] == t
                                           for o in s.cli.orders)
                        if gone:
                            r = {"ok": True, "records": [],
                                 "events": [["final", "cancel"]]}
                except ValueError as e:
                    # 撤单目标已成交/已撤销时返回业务错误，而不是让连接断开
                    return _js(self, 502, {"ok": False, "error": str(e)})
                return _js(self, 200 if r["ok"] else 502, r)
            if act == "modify_pending":
                t = int(req.get("ticket") or 0)
                if t <= 0:
                    return _js(self, 400, {"ok": False,
                                           "error": "ticket required"})
                price = float(req.get("price", 0) or 0)
                if price <= 0:
                    return _js(self, 400, {"ok": False,
                        "error": "price required > 0 for pending"})
                sl = float(req.get("sl", 0) or 0)
                tp = float(req.get("tp", 0) or 0)
                expiration = int(req.get("expiration", 0) or 0)
                r = s.op_modify_pending(t, price, sl, tp, expiration)
                return _js(self, 200 if r["ok"] else 502, r)
            return _js(self, 400, {"ok": False,
                                   "error": "unknown action %r" % act})
        except ValueError as e:
            return _js(self, 400, {"ok": False, "error": str(e)})
        except ConnectionError as e:
            # 上游交易连接重试后仍失败：返回 502 让调用方明确知道可重试
            return _js(self, 502, {"ok": False,
                                   "error": "upstream_unavailable: %s" % e})
        except Exception as e:
            return _js(self, 500, {"ok": False, "error": repr(e)})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--srv-host", default="117.72.80.248")
    ap.add_argument("--srv-port", type=int, default=443)
    ap.add_argument("--port", type=int, default=8222)
    ap.add_argument("--login", type=int,
                    default=int(os.environ.get("AQ_LOGIN", "0")))
    ap.add_argument("--password", default=os.environ.get("AQ_PASS", ""))
    ap.add_argument("--symbols", default="ag",
                    help="comma-separated quote subscriptions")
    ap.add_argument("--history-db",
                    default=os.path.join(os.path.dirname(
                        os.path.abspath(__file__)), "history.db"),
                    help="SQLite bar-history file (default: history.db "
                         "next to this script)")
    ap.add_argument("--site-dir",
                    default=os.path.join(os.path.dirname(
                        os.path.abspath(__file__)), "aqnt-site"),
                    help="static website root served under /site")
    args = ap.parse_args()
    if not args.login or not args.password:
        ap.error("credentials required: AQ_LOGIN/AQ_PASS env or CLI")

    def log(msg):
        print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)

    hdb = _open_history_db(args.history_db, log)
    s = Session(args.srv_host, args.srv_port, args.login, args.password,
                log, symbols=[x.strip() for x in
                              args.symbols.split(",") if x.strip()],
                hdb=hdb)
    s.site_dir = args.site_dir
    Handler.gw = s
    Handler.token = os.environ.get("AQGW_TOKEN")
    s.start()
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    srv.daemon_threads = True              # long-lived SSE connections
    log("astockquant gateway on http://%s:%d (server %s:%d, login=%d)"
        % (args.host, args.port, args.srv_host, args.srv_port,
           args.login))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
