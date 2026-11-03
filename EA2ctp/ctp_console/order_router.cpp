#include "order_router.h"
#include "app_util.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <sstream>

namespace {

std::string OneLine(const std::string &s) {
    std::string t = s;
    for (size_t i = 0; i < t.size(); ++i) {
        if (t[i] == '\r' || t[i] == '\n') t[i] = ' ';
    }
    return TrimCopy(t);
}

std::string CsvSafe(const std::string &s) {
    std::string t = OneLine(s);
    std::replace(t.begin(), t.end(), ',', ';');
    return t;
}

std::string ActionText(bool buy) { return buy ? "BUY" : "SELL"; }
std::string OffsetText(bool open) { return open ? "OPEN" : "CLOSE"; }

} // namespace

void OrderRouter::Configure(const std::string &mode, int ctpFirstTimeoutMs,
                            const std::string &ledgerFile) {
    m_mode = LowerCopy(TrimCopy(mode));
    if (m_mode != "ctp_first" && m_mode != "ctp_only") m_mode = "parallel";
    m_timeoutMs = ctpFirstTimeoutMs > 0 ? ctpFirstTimeoutMs : 3000;
    m_ledgerFile = ledgerFile.empty() ? "orders.csv" : ledgerFile;

    {
        // 检查现有文件的 header 是否带 groupKey——不带就自动升级
        std::ifstream test(m_ledgerFile.c_str());
        if (test.good()) {
            std::string firstLine;
            std::getline(test, firstLine);
            test.close();
            if (firstLine.find("groupKey") == std::string::npos) {
                // 旧 header：读全部、插空 groupKey 列、重写
                std::ifstream fin(m_ledgerFile.c_str());
                std::vector<std::string> rows;
                std::string line;
                while (std::getline(fin, line)) rows.push_back(line);
                fin.close();
                std::ofstream fout(m_ledgerFile.c_str(), std::ios::trunc);
                fout << "time,groupKey,source,key,symbol,action,offset,type,"
                        "price,volume,filled,status,loginNr,groupName,comment\n";
                for (size_t i = 1; i < rows.size(); ++i) {
                    // 在 time,source 之间插一个空 groupKey
                    size_t pos = rows[i].find(',');
                    if (pos != std::string::npos) {
                        fout << rows[i].substr(0, pos) << ",,"
                             << rows[i].substr(pos + 1) << "\n";
                    } else {
                        fout << rows[i] << "\n";
                    }
                }
                fout.close();
            }
        } else {
            test.close();
        }
        std::lock_guard<std::mutex> lk(m_fileMutex);
        m_file.open(m_ledgerFile.c_str(), std::ios::app);
        m_fileReady = m_file.is_open();
        if (m_fileReady) {
            // 确保 header 已正确写入（新文件或刚升级过）
            std::ifstream verify(m_ledgerFile.c_str());
            std::string hdr; std::getline(verify, hdr); verify.close();
            if (hdr.find("groupKey") == std::string::npos) {
                m_file << "time,groupKey,source,key,symbol,action,offset,type,"
                          "price,volume,filled,status,loginNr,groupName,comment\n";
                m_file.flush();
            }
        }
    }
}

void OrderRouter::SetCtp(CtpTrader *ctp) { m_ctp = ctp; }
void OrderRouter::SetGw(GwClient *gw) { m_gw = gw; }
void OrderRouter::SetNotify(std::function<void(const std::string &)> fn) { m_notify = fn; }

void OrderRouter::Start() {
    m_stop = false;
    m_worker = new std::thread(&OrderRouter::WorkerLoop, this);
    m_gwAsyncThread = new std::thread(&OrderRouter::GwAsyncLoop, this);
}

void OrderRouter::Stop() {
    m_stop = true;
    {
        std::lock_guard<std::mutex> lk(m_qMutex);
        m_qCv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lk(m_gwAsyncMutex);
        m_gwAsyncCv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lk(m_gwWaitMutex);
        m_gwWaitCv.notify_all();
    }
    if (m_worker) {
        m_worker->join();
        delete m_worker;
        m_worker = nullptr;
    }
    if (m_gwAsyncThread) {
        m_gwAsyncThread->join();
        delete m_gwAsyncThread;
        m_gwAsyncThread = nullptr;
    }
    // 清空队列中尚未开始的 GwAsyncPending（shared_ptr 自动释放）
    {
        std::lock_guard<std::mutex> lk(m_gwAsyncMutex);
        m_gwAsyncQueue.clear();
    }
    std::lock_guard<std::mutex> lk(m_fileMutex);
    if (m_fileReady) {
        m_file.flush();
        m_file.close();
        m_fileReady = false;
    }
}

GwOrderResult OrderRouter::PlaceGw(const OrderCommand &cmd, int lots, double price) {
    GwOrderResult r;
    if (!m_gw || !m_gw->IsConfigured()) {
        r.error = "gateway_not_configured";
        return r;
    }
    std::string gwSymbol = LowerCopy(ContractPrefix(cmd.symbol));
    // 平仓只是反向交易：买入平空 -> 网关 sell，卖出平多 -> 网关 buy
    bool gwBuy = cmd.open ? cmd.buy : !cmd.buy;
    if (cmd.type == 'L') {
        r = m_gw->LimitOrder(gwSymbol, gwBuy, lots, price, cmd.comment);
    } else {
        r = m_gw->MarketOrder(gwSymbol, gwBuy, lots, cmd.comment);
    }
    if (!r.ok) {
        LogMessage("Gateway order failed: " + r.error);
    } else {
        LogMessage("Gateway order ok: " + gwSymbol + " " +
                   (gwBuy ? "BUY" : "SELL") + " lots=" + std::to_string(lots) +
                   " ticket=" + std::to_string(r.ticket));
    }
    return r;
}

OrderReply OrderRouter::Submit(const OrderCommand &cmd) {
    OrderReply reply;
    std::string symbol = TrimCopy(cmd.symbol);

    if (cmd.type == 'S') {
        reply.ok = false;
        reply.text = "ERR unsupported orderType S";
        LogMessage("Reject order: stop-loss orderType S not supported");
        return reply;
    }
    if (cmd.type == 'D') {
        return HandleCancel(cmd);
    }
    if (cmd.type != 'M' && cmd.type != 'L') {
        reply.ok = false;
        reply.text = "ERR unsupported orderType " + std::string(1, cmd.type);
        return reply;
    }
    if (symbol.empty()) {
        reply.ok = false;
        reply.text = "ERR symbol required";
        return reply;
    }
    if (cmd.lots <= 0) {
        reply.ok = false;
        reply.text = "ERR invalid lotnr";
        return reply;
    }

    std::string gk = NextGroupKey();
    double ctpPrice = cmd.price;
    if (cmd.type == 'M') {
        ctpPrice = cmd.buy ? cmd.price + 20.0 * cmd.bidAskDif
                           : cmd.price - 20.0 * cmd.bidAskDif;
    }

    bool ctpOk = false;
    std::string ref = "-";
    std::string ctpErr;
    if (m_ctp && m_ctp->IsReady()) {
        ref = m_ctp->NextOrderRef();
        if (m_ctp->InsertOrder(ref, symbol, cmd.buy, cmd.open, cmd.type == 'M',
                               ctpPrice, cmd.lots, cmd.comment, ctpErr)) {
            ctpOk = true;
        }
    } else {
        ctpErr = "ctp_not_ready";
    }

    // 纯 CTP 模式：只报 CTP，完全不下发网关
    if (m_mode == "ctp_only") {
        if (!ctpOk) {
            reply.ok = false;
            reply.text = "ERR ctp=" + OneLine(ctpErr.empty() ? "ctp_not_ready" : ctpErr);
            return reply;
        }
        Persist(gk, "CTP", ref, symbol, ActionText(cmd.buy), OffsetText(cmd.open),
                std::string(1, cmd.type), ctpPrice, cmd.lots, 0, "NEW",
                cmd.loginNr, cmd.groupName, cmd.comment);
        reply.ok = true;
        reply.text = "OK ref=" + ref + " group=" + gk;
        LogMessage("Order routed (ctp_only) group=" + gk + ": " + reply.text);
        return reply;
    }

    // 先 CTP 后网关：仅市价单
    if (m_mode == "ctp_first" && cmd.type == 'M') {
        if (!ctpOk) {
            reply.ok = false;
            reply.text = "ERR ctp=" + OneLine(ctpErr.empty() ? "ctp_not_ready" : ctpErr);
            return reply;
        }
        {
            std::lock_guard<std::mutex> lk(m_pendingMutex);
            PendingFirst pf;
            pf.groupKey = gk;
            pf.symbol = symbol;
            pf.buy = cmd.buy;
            pf.comment = cmd.comment;
            pf.deadlineMs = NowMs() + m_timeoutMs;
            m_pending[ref] = pf;
        }
        Persist(gk, "CTP", ref, symbol, ActionText(cmd.buy), OffsetText(cmd.open),
                std::string(1, cmd.type), ctpPrice, cmd.lots, 0, "NEW",
                cmd.loginNr, cmd.groupName, cmd.comment);
        reply.ok = true;
        reply.text = "OK ref=" + ref + " gw=PENDING group=" + gk;
        LogMessage("Order routed (ctp_first) group=" + gk + ": " + reply.text);
        return reply;
    }

    // 并行双发（限价单在 ctp_first 模式下也走这里）
    GwOrderResult gwRes;
    gwRes.ok = false;
    gwRes.error = "gateway_not_configured";
    if (m_gw && m_gw->IsConfigured()) {
        // 网关腿异步执行：投递队列后最多等 3s，超时先回 PENDING，
        // worker 线程拿到真实 ticket 后补落盘并推送 GW_TICKET
        auto ap = std::make_shared<GwAsyncPending>();
        ap->groupKey = gk;
        ap->cmd = cmd;
        ap->lots = cmd.lots;
        ap->price = cmd.price;
        {
            std::lock_guard<std::mutex> lk(m_gwAsyncMutex);
            m_gwAsyncQueue.push_back(ap);
        }
        m_gwAsyncCv.notify_one();

        // 有界等待：3s，避免网关上游卡顿拖慢客户端应答
        {
            std::unique_lock<std::mutex> lk(m_gwWaitMutex);
            m_gwWaitCv.wait_for(lk, std::chrono::milliseconds(3000),
                                [&ap] { return ap->ready.load(); });
        }
        if (ap->ready.load()) {
            gwRes = ap->result;
        } else {
            // 超时：gwRes 保持 ok=false，落盘/广播完全交给 GwAsyncLoop
            LogMessage("Order group=" + gk + " gw leg pending (3s timeout)");
        }
    }

    if (ctpOk) {
        Persist(gk, "CTP", ref, symbol, ActionText(cmd.buy), OffsetText(cmd.open),
                std::string(1, cmd.type), ctpPrice, cmd.lots, 0, "NEW",
                cmd.loginNr, cmd.groupName, cmd.comment);
    }
    // NEW GW 行由 GwAsyncLoop 在拿到真实结果后落盘，这里不再重复

    std::string gwField;
    if (gwRes.ok) {
        gwField = std::to_string(gwRes.ticket);
    } else if (m_gw && m_gw->IsConfigured()) {
        // 网关可用但 3s 内没出结果 → PENDING（worker 稍后会推送真实 ticket）
        gwField = "PENDING";
    } else {
        gwField = "-";
    }

    if (ctpOk || gwRes.ok || gwField == "PENDING") {
        reply.ok = true;
        reply.text = "OK ref=" + (ctpOk ? ref : std::string("-")) +
                     " gw=" + gwField +
                     " group=" + gk;
    } else {
        reply.ok = false;
        reply.text = "ERR ctp=" + OneLine(ctpErr.empty() ? "-" : ctpErr) +
                     " gw=" + OneLine(gwRes.error.empty() ? "-" : gwRes.error);
    }
    LogMessage("Order routed group=" + gk + ": " + reply.text + " " + symbol + " " +
               ActionText(cmd.buy) + " " + OffsetText(cmd.open) + " " +
               std::string(1, cmd.type) + " lots=" + std::to_string(cmd.lots));
    return reply;
}

OrderReply OrderRouter::HandleCancel(const OrderCommand &cmd) {
    OrderReply reply;
    std::string symbol = TrimCopy(cmd.symbol);
    bool all = symbol.empty() || symbol == "*";
    std::string gk = NextGroupKey();

    int ctpSent = 0;
    std::string ctpErr;
    bool ctpReady = m_ctp && m_ctp->IsReady();
    if (ctpReady) {
        ctpSent = m_ctp->CancelAll(all ? "*" : symbol, ctpErr);
    } else {
        ctpErr = "ctp_not_ready";
    }

    int gwDone = 0;
    int gwAttempted = 0;
    std::string gwErr;
    bool gwReady = m_gw && m_gw->IsConfigured();
    if (gwReady) {
        gwDone = m_gw->CancelAll(all ? "" : LowerCopy(ContractPrefix(symbol)),
                                 gwErr, gwAttempted);
    } else {
        gwErr = "gateway_not_configured";
    }

    Persist(gk, "CAN", all ? "*" : symbol, all ? "*" : symbol, "CANCEL", "-", "D",
            0.0, 0, 0, "SENT", cmd.loginNr, cmd.groupName, cmd.comment);

    if (!ctpReady && !gwReady) {
        reply.ok = false;
        reply.text = "ERR no trading channel";
        return reply;
    }
    // 两侧都失败时报错；只要有一侧可用并成功发起撤单即视为成功
    bool ctpGood = ctpReady && ctpErr.empty();
    bool gwGood = gwReady && gwErr.empty();
    if (!ctpGood && !gwGood) {
        reply.ok = false;
        reply.text = "ERR ctp=" + OneLine(ctpErr.empty() ? "-" : ctpErr) +
                     " gw=" + OneLine(gwErr.empty() ? "-" : gwErr);
        LogMessage("Cancel failed group=" + gk + ": " + reply.text);
        return reply;
    }
    reply.ok = true;
    reply.text = "OK cancel ctp=" + std::to_string(ctpSent) +
                 " gw=" + std::to_string(gwDone) + " group=" + gk;
    LogMessage("Cancel routed group=" + gk + ": " + reply.text +
               (symbol.empty() ? " all" : " " + symbol));
    return reply;
}

void OrderRouter::OnCtpTrade(const CtpTradeEvent &ev) {
    {
        std::lock_guard<std::mutex> lk(m_qMutex);
        m_queue.push_back(ev);
    }
    m_qCv.notify_one();
}

void OrderRouter::OnCtpOrder(const CtpOrderEvent &ev) {
    if (ev.status == THOST_FTDC_OST_Canceled || ev.status == '4') {
        LogMessage("CTP order " + ev.ref + " terminal status=" +
                   std::string(1, ev.status) + " filled=" +
                   std::to_string(ev.filled) + "/" + std::to_string(ev.volume));
    }
}

void OrderRouter::HandleFill(const CtpTradeEvent &ev) {
    bool follow = false;
    int delta = 0;
    std::string symbol;
    bool buy = true;
    std::string comment;
    std::string gk;

    {
        std::lock_guard<std::mutex> lk(m_pendingMutex);
        auto it = m_pending.find(ev.ref);
        if (it != m_pending.end()) {
            it->second.ctpFilled += ev.volume;
            delta = it->second.ctpFilled - it->second.gwSent;
            gk = it->second.groupKey;
            if (delta > 0) {
                it->second.gwSent += delta;
                follow = true;
                symbol = it->second.symbol;
                buy = it->second.buy;
                comment = it->second.comment;
            }
        }
    }

    if (follow && m_gw && m_gw->IsConfigured()) {
        OrderCommand sub;
        sub.symbol = symbol;
        sub.buy = buy;
        sub.open = true;
        sub.type = 'M';
        sub.comment = comment;
        GwOrderResult g = PlaceGw(sub, delta, 0.0);
        if (g.ok) {
            LogMessage("Gateway follow-up group=" + gk + " ref=" + ev.ref +
                       " ticket=" + std::to_string(g.ticket) +
                       " delta=" + std::to_string(delta));
            // 补发腿也要落盘，沿用同一 groupKey 建立关联
            Persist(gk, "GW", std::to_string(g.ticket), symbol,
                    ActionText(buy), OffsetText(true), "M", 0.0, delta, 0,
                    "FOLLOW", "", "", comment);
        } else {
            LogMessage("Gateway follow-up failed group=" + gk + " ref=" +
                       ev.ref + ": " + g.error);
        }
    }

    Notify("TRADE " + ev.ref + "," + FormatDouble(ev.price, 4) + "," +
           std::to_string(ev.volume) + ",CTP");
    // CTP 成交落盘：带 groupKey（若 ctp_first 模式下仍有 pending 记录可取）
    std::string fillGk = gk;
    if (fillGk.empty()) {
        // 并行模式下成交事件本身没有 groupKey，留空；orders.csv 仍可通过 ref 关联
        // （也可以在这里反向查 m_active，但成本太高，留空即可）
    }
    Persist(fillGk, "CTP", ev.ref, ev.instrument, ActionText(ev.buy),
            OffsetText(ev.open), "T", ev.price, ev.volume, ev.volume, "FILL",
            "", "", "");
}

void OrderRouter::CheckDeadlines() {
    long long now = NowMs();
    std::lock_guard<std::mutex> lk(m_pendingMutex);
    for (std::map<std::string, PendingFirst>::iterator it = m_pending.begin();
         it != m_pending.end();) {
        if (now >= it->second.deadlineMs) {
            if (it->second.ctpFilled == 0) {
                LogMessage("ctp_first timeout without fill, ref=" + it->first);
            }
            it = m_pending.erase(it);
        } else {
            ++it;
        }
    }
}

void OrderRouter::WorkerLoop() {
    while (!m_stop.load()) {
        std::deque<CtpTradeEvent> local;
        {
            std::unique_lock<std::mutex> lk(m_qMutex);
            m_qCv.wait_for(lk, std::chrono::milliseconds(100));
            local.swap(m_queue);
        }
        while (!local.empty()) {
            HandleFill(local.front());
            local.pop_front();
        }
        CheckDeadlines();
    }
}

void OrderRouter::GwAsyncLoop() {
    while (!m_stop.load()) {
        std::shared_ptr<GwAsyncPending> ap;
        {
            std::unique_lock<std::mutex> lk(m_gwAsyncMutex);
            m_gwAsyncCv.wait_for(lk, std::chrono::milliseconds(100),
                                 [this] { return m_stop.load() || !m_gwAsyncQueue.empty(); });
            if (m_stop.load() && m_gwAsyncQueue.empty()) break;
            if (!m_gwAsyncQueue.empty()) {
                ap = m_gwAsyncQueue.front();
                m_gwAsyncQueue.pop_front();
            }
        }
        if (!ap) continue;

        // 执行网关请求（最长 30s，GwClient 内部超时）
        GwOrderResult r = PlaceGw(ap->cmd, ap->lots, ap->price);

        // 落盘 + 推送
        std::string symbol = ap->cmd.symbol;
        std::string gk = ap->groupKey;
        std::string action = ActionText(ap->cmd.buy);
        std::string offset = OffsetText(ap->cmd.open);
        std::string typeStr = std::string(1, ap->cmd.type);

        if (r.ok) {
            Persist(gk, "GW", std::to_string(r.ticket), symbol, action, offset,
                    typeStr, ap->price, ap->lots, 0, "NEW",
                    ap->cmd.loginNr, ap->cmd.groupName, ap->cmd.comment);
            Notify("GW_TICKET " + gk + "," + std::to_string(r.ticket));
        } else {
            Persist(gk, "GW", "-", symbol, action, offset, typeStr, ap->price,
                    ap->lots, 0, "FAIL", ap->cmd.loginNr, ap->cmd.groupName,
                    OneLine(r.error));
            Notify("GW_FAIL " + gk + "," + OneLine(r.error));
        }

        // 通知可能在等 3s 的 Submit 线程：结果已就绪
        ap->result = r;
        ap->ready.store(true);
        {
            std::lock_guard<std::mutex> lk(m_gwWaitMutex);
            m_gwWaitCv.notify_all();
        }
        // shared_ptr 自动释放，Submit 侧仍持有 ap，等读完才会销毁
    }
}

std::string OrderRouter::NextGroupKey() {
    int n = ++m_groupSeq;
    char buf[16];
    std::snprintf(buf, sizeof(buf), "G%010d", n);
    return std::string(buf);
}

void OrderRouter::Persist(const std::string &groupKey, const std::string &source,
                          const std::string &key, const std::string &symbol,
                          const std::string &action, const std::string &offset,
                          const std::string &type, double price, int volume,
                          int filled, const std::string &status,
                          const std::string &login, const std::string &group,
                          const std::string &comment) {
    std::lock_guard<std::mutex> lk(m_fileMutex);
    if (!m_fileReady) return;
    m_file << NowStamp() << ","
           << CsvSafe(groupKey) << ","
           << CsvSafe(source) << ","
           << CsvSafe(key) << ","
           << CsvSafe(symbol) << ","
           << CsvSafe(action) << ","
           << CsvSafe(offset) << ","
           << CsvSafe(type) << ","
           << FormatDouble(price, 4) << ","
           << volume << ","
           << filled << ","
           << CsvSafe(status) << ","
           << CsvSafe(login) << ","
           << CsvSafe(group) << ","
           << CsvSafe(comment) << "\n";
    m_file.flush();
}

void OrderRouter::Notify(const std::string &line) {
    if (m_notify) m_notify(line);
}
