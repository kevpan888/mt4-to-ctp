#include "gw_client.h"
#include "app_util.h"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>
#include <vector>

namespace {

bool ParseUrl(const std::string &base, std::string &host, int &port) {
    std::string b = TrimCopy(base);
    if (b.empty()) return false;
    size_t p = b.find("://");
    if (p != std::string::npos) b = b.substr(p + 3);
    size_t slash = b.find('/');
    if (slash != std::string::npos) b = b.substr(0, slash);
    size_t colon = b.rfind(':');
    if (colon != std::string::npos) {
        host = b.substr(0, colon);
        port = std::atoi(b.substr(colon + 1).c_str());
    } else {
        host = b;
        port = 80;
    }
    return !host.empty() && port > 0 && port < 65536;
}

SOCKET ConnectHost(const std::string &host, int port, int timeoutMs, int recvMs = 4000) {
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) return INVALID_SOCKET;

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);

    bool resolved = false;
    struct in_addr ia;
    if (inet_pton(AF_INET, host.c_str(), &ia) == 1) {
        addr.sin_addr = ia;
        resolved = true;
    } else {
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        struct addrinfo *res = nullptr;
        if (getaddrinfo(host.c_str(), nullptr, &hints, &res) == 0 && res) {
            addr.sin_addr = ((sockaddr_in *)res->ai_addr)->sin_addr;
            freeaddrinfo(res);
            resolved = true;
        }
    }
    if (!resolved) {
        closesocket(s);
        return INVALID_SOCKET;
    }

    u_long nb = 1;
    ioctlsocket(s, FIONBIO, &nb);
    int rc = connect(s, (sockaddr *)&addr, sizeof(addr));
    if (rc == SOCKET_ERROR) {
        int err = WSAGetLastError();
        if (err != WSAEWOULDBLOCK && err != WSAEINPROGRESS) {
            closesocket(s);
            return INVALID_SOCKET;
        }
        fd_set wfds;
        FD_ZERO(&wfds);
        FD_SET(s, &wfds);
        timeval tv;
        tv.tv_sec = timeoutMs / 1000;
        tv.tv_usec = (timeoutMs % 1000) * 1000;
        int sel = select((int)s + 1, nullptr, &wfds, nullptr, &tv);
        if (sel <= 0) {
            closesocket(s);
            return INVALID_SOCKET;
        }
        int soErr = 0;
        int len = sizeof(soErr);
        getsockopt(s, SOL_SOCKET, SO_ERROR, (char *)&soErr, &len);
        if (soErr != 0) {
            closesocket(s);
            return INVALID_SOCKET;
        }
    }
    nb = 0;
    ioctlsocket(s, FIONBIO, &nb);
    DWORD rcv = (DWORD)recvMs;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (const char *)&rcv, sizeof(rcv));
    return s;
}

bool SendAll(SOCKET s, const std::string &data) {
    int total = 0;
    int len = (int)data.size();
    while (total < len) {
        int n = send(s, data.c_str() + total, len - total, 0);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}

bool SplitHttp(const std::string &resp, int &status, std::string &body) {
    size_t hdrEnd = resp.find("\r\n\r\n");
    if (hdrEnd == std::string::npos) return false;
    body = resp.substr(hdrEnd + 4);
    size_t sp1 = resp.find(' ');
    if (sp1 == std::string::npos) return false;
    status = std::atoi(resp.substr(sp1 + 1, 3).c_str());
    return true;
}

bool HttpRequest(const std::string &host, int port, const std::string &request,
                 int &status, std::string &body, std::string &err,
                 int recvMs = 4000) {
    status = 0;
    body.clear();
    err.clear();
    SOCKET s = ConnectHost(host, port, 3000, recvMs);
    if (s == INVALID_SOCKET) {
        err = "connect_failed";
        return false;
    }
    if (!SendAll(s, request)) {
        closesocket(s);
        err = "send_failed";
        return false;
    }
    std::string resp;
    char buf[8192];
    while (true) {
        int n = recv(s, buf, sizeof(buf), 0);
        if (n <= 0) break;
        resp.append(buf, n);
        if (resp.size() > 4 * 1024 * 1024) break;
    }
    closesocket(s);
    if (!SplitHttp(resp, status, body)) {
        err = "bad_response";
        return false;
    }
    return true;
}

// ---- 极简 JSON 工具（只处理网关返回的扁平对象/数组） ----

size_t FindValuePos(const std::string &s, const std::string &key) {
    std::string token = "\"" + key + "\"";
    size_t p = s.find(token);
    while (p != std::string::npos) {
        size_t q = p + token.size();
        while (q < s.size() && std::isspace((unsigned char)s[q])) ++q;
        if (q < s.size() && s[q] == ':') return q + 1;
        p = s.find(token, p + 1);
    }
    return std::string::npos;
}

bool JsonGetString(const std::string &s, const std::string &key, std::string &out) {
    size_t p = FindValuePos(s, key);
    if (p == std::string::npos) return false;
    while (p < s.size() && std::isspace((unsigned char)s[p])) ++p;
    if (p >= s.size() || s[p] != '"') return false;
    ++p;
    out.clear();
    while (p < s.size()) {
        char c = s[p++];
        if (c == '\\' && p < s.size()) {
            char e = s[p++];
            switch (e) {
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                case '"': out.push_back('"'); break;
                case '\\': out.push_back('\\'); break;
                default: out.push_back(e); break;
            }
        } else if (c == '"') {
            return true;
        } else {
            out.push_back(c);
        }
    }
    return false;
}

bool JsonGetNumber(const std::string &s, const std::string &key, double &out) {
    size_t p = FindValuePos(s, key);
    if (p == std::string::npos) return false;
    while (p < s.size() && std::isspace((unsigned char)s[p])) ++p;
    char *end = nullptr;
    double v = std::strtod(s.c_str() + p, &end);
    if (end == s.c_str() + p) return false;
    out = v;
    return true;
}

bool JsonGetBool(const std::string &s, const std::string &key, bool &out) {
    size_t p = FindValuePos(s, key);
    if (p == std::string::npos) return false;
    while (p < s.size() && std::isspace((unsigned char)s[p])) ++p;
    if (s.compare(p, 4, "true") == 0) { out = true; return true; }
    if (s.compare(p, 5, "false") == 0) { out = false; return true; }
    return false;
}

// 从 key 对应的数组中拆出每个顶层对象
bool JsonExtractArray(const std::string &s, const std::string &key,
                      std::vector<std::string> &objs) {
    objs.clear();
    size_t p = FindValuePos(s, key);
    if (p == std::string::npos) return false;
    while (p < s.size() && std::isspace((unsigned char)s[p])) ++p;
    if (p >= s.size() || s[p] != '[') return false;
    ++p;
    int depth = 0;
    bool inStr = false;
    bool esc = false;
    size_t objStart = std::string::npos;
    for (; p < s.size(); ++p) {
        char c = s[p];
        if (inStr) {
            if (esc) esc = false;
            else if (c == '\\') esc = true;
            else if (c == '"') inStr = false;
            continue;
        }
        if (c == '"') { inStr = true; continue; }
        if (c == '{') {
            if (depth == 0) objStart = p;
            ++depth;
        } else if (c == '}') {
            if (depth > 0) --depth;
            if (depth == 0 && objStart != std::string::npos) {
                objs.push_back(s.substr(objStart, p - objStart + 1));
                objStart = std::string::npos;
            }
        } else if (c == ']' && depth == 0) {
            return true;
        }
    }
    return true;
}

std::string JsonEscape(const std::string &in) {
    std::string out;
    for (size_t i = 0; i < in.size(); ++i) {
        char c = in[i];
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if ((unsigned char)c < 0x20) out += ' ';
                else out.push_back(c);
        }
    }
    return out;
}

std::string JoinSymbols(const std::vector<std::string> &symbols) {
    std::string out;
    for (size_t i = 0; i < symbols.size(); ++i) {
        if (i) out += ",";
        out += LowerCopy(symbols[i]);
    }
    return out;
}

} // namespace

GwClient::~GwClient() {
    StopAll();
}

void GwClient::Configure(const std::string &base, const std::string &token) {
    m_base = TrimCopy(base);
    m_token = TrimCopy(token);
    ParseUrl(m_base, m_host, m_port);
}

bool GwClient::IsConfigured() const {
    return !m_host.empty() && m_port > 0;
}

long long GwClient::LastTickMs() const { return m_lastTickMs.load(); }

void GwClient::SetTickCallback(std::function<void(const std::string &, double, double)> cb) {
    m_tickCb = cb;
}

void GwClient::SetPositionCallback(std::function<void(const GwPosition &)> cb) {
    m_posCb = cb;
}

std::string GwClient::BuildRequest(const std::string &method, const std::string &path,
                                   const std::string &body, bool keepAlive) {
    std::string req;
    req += method + " " + path + " HTTP/1.1\r\n";
    req += "Host: " + m_host + ":" + std::to_string(m_port) + "\r\n";
    if (!m_token.empty()) req += "X-Auth-Token: " + m_token + "\r\n";
    req += "Accept: application/json\r\n";
    if (method == "POST") {
        req += "Content-Type: application/json\r\n";
        req += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    }
    req += std::string("Connection: ") + (keepAlive ? "keep-alive" : "close") + "\r\n\r\n";
    if (method == "POST") req += body;
    return req;
}

GwOrderResult GwClient::DoOrder(const std::string &body) {
    GwOrderResult r;
    if (!IsConfigured()) {
        r.error = "gateway_not_configured";
        return r;
    }
    int status = 0;
    std::string resp, err;
    // 网关的挂单/撤单请求最长约 4~25 秒（内部持锁串行），读超时需放宽
    if (!HttpRequest(m_host, m_port, BuildRequest("POST", "/order", body, false),
                     status, resp, err, 30000)) {
        r.error = err;
        return r;
    }
    bool ok = false;
    JsonGetBool(resp, "ok", ok);
    r.ok = ok && status == 200;
    if (!r.ok) {
        std::string msg;
        if (JsonGetString(resp, "error", msg) && !msg.empty()) r.error = msg;
        else r.error = "http_" + std::to_string(status);
        return r;
    }
    std::vector<std::string> recs;
    if (JsonExtractArray(resp, "records", recs) && !recs.empty()) {
        double ticket = 0;
        if (JsonGetNumber(recs[0], "ticket", ticket)) r.ticket = (long)ticket;
    }
    return r;
}

GwOrderResult GwClient::MarketOrder(const std::string &symbol, bool buy, int lots,
                                    const std::string &comment) {
    std::string body = "{\"action\":\"open\",\"symbol\":\"" + JsonEscape(LowerCopy(symbol)) +
                       "\",\"cmd\":\"" + (buy ? "buy" : "sell") +
                       "\",\"lots\":" + std::to_string(lots) +
                       ",\"comment\":\"" + JsonEscape(comment.substr(0, 31)) + "\"}";
    return DoOrder(body);
}

GwOrderResult GwClient::LimitOrder(const std::string &symbol, bool buy, int lots,
                                   double price, const std::string &comment) {
    std::string body = "{\"action\":\"place\",\"symbol\":\"" + JsonEscape(LowerCopy(symbol)) +
                       "\",\"cmd\":\"" + (buy ? "buy_limit" : "sell_limit") +
                       "\",\"lots\":" + std::to_string(lots) +
                       ",\"price\":" + FormatDouble(price, 4) +
                       ",\"sl\":0,\"tp\":0,\"expiration\":0" +
                       ",\"comment\":\"" + JsonEscape(comment.substr(0, 31)) + "\"}";
    return DoOrder(body);
}

std::vector<GwOrderInfo> GwClient::QueryOrders(bool *ok) {
    std::vector<GwOrderInfo> out;
    if (ok) *ok = false;
    if (!IsConfigured()) return out;
    int status = 0;
    std::string resp, err;
    if (!HttpRequest(m_host, m_port, BuildRequest("GET", "/orders", "", false),
                     status, resp, err, 8000)) {
        return out;
    }
    if (status != 200) return out;
    if (ok) *ok = true;
    std::vector<std::string> objs;
    JsonExtractArray(resp, "data", objs);
    for (size_t i = 0; i < objs.size(); ++i) {
        GwOrderInfo info;
        double ticket = 0;
        std::string sym;
        if (JsonGetNumber(objs[i], "ticket", ticket)) info.ticket = (long)ticket;
        JsonGetString(objs[i], "symbol", sym);
        info.symbol = sym;
        if (info.ticket != 0) out.push_back(info);
    }
    return out;
}

std::vector<GwPosition> GwClient::QueryPositions(bool *ok) {
    std::vector<GwPosition> out;
    if (ok) *ok = false;
    if (!IsConfigured()) return out;
    int status = 0;
    std::string resp, err;
    if (!HttpRequest(m_host, m_port, BuildRequest("GET", "/positions", "", false),
                     status, resp, err, 8000)) {
        return out;
    }
    if (status != 200) return out;
    if (ok) *ok = true;
    std::vector<std::string> objs;
    JsonExtractArray(resp, "data", objs);
    for (size_t i = 0; i < objs.size(); ++i) {
        GwPosition p;
        double v = 0;
        if (JsonGetNumber(objs[i], "ticket", v)) p.ticket = (long)v;
        JsonGetString(objs[i], "symbol", p.symbol);
        if (JsonGetNumber(objs[i], "cmd", v)) p.cmd = (int)v;
        if (JsonGetNumber(objs[i], "lots", v)) p.lots = v;
        if (JsonGetNumber(objs[i], "open_price", v)) p.openPrice = v;
        if (p.ticket != 0) out.push_back(p);
    }
    return out;
}

int GwClient::CancelAll(const std::string &symbolFilter, std::string &err, int &attempted) {
    err.clear();
    attempted = 0;
    if (!IsConfigured()) {
        err = "gateway_not_configured";
        return 0;
    }
    bool ok = false;
    std::vector<GwOrderInfo> orders = QueryOrders(&ok);
    if (!ok) {
        err = "query_orders_failed";
        return 0;
    }
    std::string filter = LowerCopy(TrimCopy(symbolFilter));
    int done = 0;
    for (size_t i = 0; i < orders.size(); ++i) {
        if (!filter.empty() && filter != "*" &&
            LowerCopy(orders[i].symbol) != filter) {
            continue;
        }
        ++attempted;
        std::string body = "{\"action\":\"cancel\",\"ticket\":" +
                           std::to_string(orders[i].ticket) + "}";
        GwOrderResult r = DoOrder(body);
        if (r.ok) ++done;
        else if (err.empty()) err = r.error;
    }
    // 撤单目标可能在查询与撤单之间已成交/消失，部分失败不影响整体结果
    if (done > 0) err.clear();
    return done;
}

void GwClient::StartStream(const std::vector<std::string> &symbols) {
    if (!IsConfigured() || m_streamRunning.load()) return;
    m_symbols = symbols;
    m_streamStop = false;
    m_streamRunning = true;
    m_streamThread = new std::thread(&GwClient::StreamLoop, this);
    LogMessage("Gateway SSE stream starting for symbols: " + JoinSymbols(symbols));
}

void GwClient::StopStream() {
    m_streamStop = true;
    {
        std::lock_guard<std::mutex> lk(m_sockMutex);
        if (m_streamSock != 0) {
            closesocket((SOCKET)m_streamSock);
            m_streamSock = 0;
        }
    }
    if (m_streamThread) {
        m_streamThread->join();
        delete m_streamThread;
        m_streamThread = nullptr;
    }
    m_streamRunning = false;
}

void GwClient::StartPositionPoll() {
    if (!IsConfigured() || m_pollRunning.load()) return;
    m_pollStop = false;
    m_pollRunning = true;
    m_pollThread = new std::thread(&GwClient::PollLoop, this);
    LogMessage("Gateway position polling started");
}

void GwClient::StopPositionPoll() {
    m_pollStop = true;
    if (m_pollThread) {
        m_pollThread->join();
        delete m_pollThread;
        m_pollThread = nullptr;
    }
    m_pollRunning = false;
}

void GwClient::StopAll() {
    StopStream();
    StopPositionPoll();
}

void GwClient::StreamLoop() {
    while (!m_streamStop.load()) {
        if (m_symbols.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        std::string path = "/stream?symbols=" + JoinSymbols(m_symbols) +
                           "&ticks=1&bars=0";
        SOCKET s = ConnectHost(m_host, m_port, 3000);
        if (s == INVALID_SOCKET) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
            continue;
        }
        {
            std::lock_guard<std::mutex> lk(m_sockMutex);
            m_streamSock = (unsigned long long)s;
        }

        std::string req = "GET " + path + " HTTP/1.1\r\n" +
                          "Host: " + m_host + ":" + std::to_string(m_port) + "\r\n" +
                          (m_token.empty() ? std::string()
                                           : "X-Auth-Token: " + m_token + "\r\n") +
                          "Accept: text/event-stream\r\n" +
                          "Connection: keep-alive\r\n\r\n";
        if (!SendAll(s, req)) {
            std::lock_guard<std::mutex> lk(m_sockMutex);
            m_streamSock = 0;
            closesocket(s);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
            continue;
        }

        std::string buf;
        int status = 0;
        bool headerDone = false;
        while (!m_streamStop.load() && !headerDone) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(s, &rfds);
            timeval tv;
            tv.tv_sec = 1;
            tv.tv_usec = 0;
            int sel = select((int)s + 1, &rfds, nullptr, nullptr, &tv);
            if (sel < 0) break;
            if (sel == 0) continue;
            char tmp[4096];
            int n = recv(s, tmp, sizeof(tmp), 0);
            if (n <= 0) break;
            buf.append(tmp, n);
            size_t he = buf.find("\r\n\r\n");
            if (he != std::string::npos) {
                size_t sp1 = buf.find(' ');
                if (sp1 != std::string::npos)
                    status = std::atoi(buf.substr(sp1 + 1, 3).c_str());
                buf = buf.substr(he + 4);
                headerDone = true;
            }
        }
        if (!headerDone || status != 200) {
            LogMessage("Gateway SSE rejected, status=" + std::to_string(status));
            std::lock_guard<std::mutex> lk(m_sockMutex);
            m_streamSock = 0;
            closesocket(s);
            std::this_thread::sleep_for(std::chrono::milliseconds(5000));
            continue;
        }
        LogMessage("Gateway SSE connected");

        std::string eventName;
        std::string data;
        std::string lineBuf = buf;

        auto processLine = [&](const std::string &rawLine, std::string &ev, std::string &dat) {
            std::string line = rawLine;
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) {
                if (ev == "tick" && !dat.empty()) {
                    std::string sym;
                    double bid = 0, ask = 0;
                    if (JsonGetString(dat, "symbol", sym) &&
                        JsonGetNumber(dat, "bid", bid) &&
                        JsonGetNumber(dat, "ask", ask)) {
                        m_lastTickMs = NowMs();
                        if (m_tickCb) m_tickCb(sym, bid, ask);
                    }
                }
                ev.clear();
                dat.clear();
                return;
            }
            if (line.compare(0, 6, "event:") == 0) {
                ev = TrimCopy(line.substr(6));
            } else if (line.compare(0, 5, "data:") == 0) {
                if (!dat.empty()) dat += "\n";
                dat += TrimCopy(line.substr(5));
            }
        };

        while (!m_streamStop.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(s, &rfds);
            timeval tv;
            tv.tv_sec = 1;
            tv.tv_usec = 0;
            int sel = select((int)s + 1, &rfds, nullptr, nullptr, &tv);
            if (sel < 0) break;
            if (sel == 0) continue;
            char tmp[4096];
            int n = recv(s, tmp, sizeof(tmp), 0);
            if (n <= 0) break;
            lineBuf.append(tmp, n);
            size_t pos;
            while ((pos = lineBuf.find('\n')) != std::string::npos) {
                std::string line = lineBuf.substr(0, pos);
                lineBuf.erase(0, pos + 1);
                processLine(line, eventName, data);
            }
            if (lineBuf.size() > 65536) lineBuf.clear();
        }

        {
            std::lock_guard<std::mutex> lk(m_sockMutex);
            m_streamSock = 0;
        }
        closesocket(s);
        if (!m_streamStop.load()) {
            LogMessage("Gateway SSE disconnected, retrying in 3s");
            std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        }
    }
    m_streamRunning = false;
}

void GwClient::PollLoop() {
    while (!m_pollStop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        if (m_pollStop.load()) break;
        bool ok = false;
        std::vector<GwPosition> pos = QueryPositions(&ok);
        if (!ok) continue;

        std::set<long> cur;
        for (size_t i = 0; i < pos.size(); ++i) cur.insert(pos[i].ticket);

        if (!m_seedDone) {
            m_seenTickets = cur;
            m_seedDone = true;
            continue;
        }
        for (size_t i = 0; i < pos.size(); ++i) {
            if (m_seenTickets.find(pos[i].ticket) == m_seenTickets.end()) {
                m_seenTickets.insert(pos[i].ticket);
                if (m_posCb) m_posCb(pos[i]);
            }
        }
        for (std::set<long>::iterator it = m_seenTickets.begin();
             it != m_seenTickets.end();) {
            if (cur.find(*it) == cur.end()) it = m_seenTickets.erase(it);
            else ++it;
        }
    }
    m_pollRunning = false;
}
