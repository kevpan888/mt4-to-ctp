// Simple CTP Console Program - Bid/Ask Price Display
// Based on config.ini configuration

#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <cctype>
#include <iomanip>
#include <ctime>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <direct.h>
#pragma comment(lib,"ws2_32.lib")
#else
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <signal.h>
#endif

#include "ThostFtdcMdApi.h"
#include "app_util.h"
#include "ctp_trader.h"
#include "gw_client.h"
#include "order_router.h"
#include "order_server.h"

// Helper functions (moved early so they're available before use)
static inline std::string trim_copy(const std::string &s) {
    std::string t = s;
    while (!t.empty() && std::isspace((unsigned char)t.back())) t.pop_back();
    while (!t.empty() && std::isspace((unsigned char)t.front())) t.erase(t.begin());
    return t;
}

// Portable helper to convert IPv4 sockaddr_in to string (uses InetNtop/inet_ntop)
static std::string sockaddr_in_to_string(const struct sockaddr_in &addr) {
    char buf[INET6_ADDRSTRLEN] = {0};
#ifdef _WIN32
    // InetNtopA is available in ws2tcpip.h
    if (InetNtopA(AF_INET, (PVOID)&addr.sin_addr, buf, (DWORD)sizeof(buf)) != NULL)
        return std::string(buf);
    return std::string("unknown");
#else
    if (inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf)) != NULL)
        return std::string(buf);
    return std::string("unknown");
#endif
}


// (removed duplicate definition at bottom)

// Global variables
static std::string g_brokerID = "";
static std::string g_user = "<YOUR_CTP_INVESTOR_ID>";
static std::string g_password = "<YOUR_CTP_PASSWORD>";
static std::vector<std::string> g_marketAddrs = {
    "tcp://182.254.243.31:30011",
    "tcp://182.254.243.31:30012", 
    "tcp://182.254.243.31:30013"
};
static std::vector<std::string> g_instruments = {"ru2701", "rb2601", "au2512", "ag2601"};

// --- EA2ctp 信号桥接 ---
static std::string g_eaSignalFile = "ctp_signals.csv";  // EA 写 / CTP 读
static std::mutex g_signalMutex;

// --- 交易 / 行情源配置 ---
static std::vector<std::string> g_tradeAddrs;
static std::string g_marketSource = "both";       // both | ctp | gateway
static std::string g_orderMode = "parallel";      // parallel | ctp_first | ctp_only
static int g_orderPort = 4445;
static int g_ctpFirstTimeoutMs = 3000;
static std::string g_gwBase = "http://127.0.0.1:8222";
static std::string g_gwToken;
static std::string g_appID = "client_DT_0001";
static std::string g_authCode = "7K8IFY3Y3LQX4NPE";
static std::string g_productInfo = "Deep Think Future";

// CTP 行情主路状态
static std::atomic<bool> g_ctpMdConnected(false);
static std::atomic<long long> g_ctpMdLastTickMs(0);

// 交易组件
static CtpTrader g_ctpTrader;
static GwClient g_gwClient;
static OrderRouter g_orderRouter;
static OrderServer g_orderServer;

// 网关品种（小写前缀）-> CTP 合约
static std::map<std::string, std::string> g_gwSymToContract;

// Symbol mapping
static std::map<std::string, std::string> g_symbMap;
// Reverse map: alias -> CTP (built from g_symbMap)
static std::map<std::string, std::string> g_symbRevMap;

// Market data storage
struct MarketData {
    double bidPrice = 0.0;
    double askPrice = 0.0;
    double lastPrice = 0.0;
    double volume = 0.0;
    std::string updateTime;
    bool connected = false;
};

static std::map<std::string, MarketData> g_marketData;
static std::mutex g_dataMutex;

// --- TCP feed server state ----
static int g_feed_port = 2222;
static std::mutex g_clients_mutex;
static std::vector<SOCKET> g_client_sockets;
static std::map<int, std::vector<std::string>> g_client_subs;
static bool g_run_server = true;
// Feed server credentials
static std::string g_feed_login = "localhost";
static std::string g_feed_password = "localhost";

static bool send_all(SOCKET s, const char* buf, int len) {
    int total = 0;
    while (total < len) {
        int sent = send(s, buf + total, len - total, 0);
        if (sent <= 0) return false;
        total += sent;
    }
    return true;
}
static void close_sock(SOCKET s) { closesocket(s); }
static void remove_client(SOCKET s) {
    std::lock_guard<std::mutex> lk(g_clients_mutex);
    auto it = std::find(g_client_sockets.begin(), g_client_sockets.end(), s);
    if (it != g_client_sockets.end()) g_client_sockets.erase(it);
    g_client_subs.erase((int)s);
    close_sock(s);
}

// Simple thread-safe logger that writes to stdout and a log file
static std::ofstream g_log_ofs;
static std::mutex g_log_mutex;
void LogMessage(const std::string &msg) {
    std::lock_guard<std::mutex> lk(g_log_mutex);
    // timestamp
    std::time_t t = std::time(nullptr);
    char tb[64];
    std::strftime(tb, sizeof(tb), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    std::string line = std::string("[") + tb + "] " + msg;
    std::cout << line << std::endl;
    if (g_log_ofs.is_open()) {
        g_log_ofs << line << std::endl;
        g_log_ofs.flush();
    }
}

// Format a tick line for an instrument. Some instruments (like 'ruj') require integer prices.
static int FormatTickLine(const std::string &symbol, char* buf, size_t bufsize, double bid, double ask) {
    // case-insensitive compare for ru j
    std::string up = symbol;
    std::transform(up.begin(), up.end(), up.begin(), [](unsigned char c){ return std::toupper(c); });
    /*
    if (up == "RUJ") {
        // use integer formatting (no decimals) with real timestamp
        return _snprintf(buf, (int)bufsize, "%s %lld %.0f %.0f\r\n", symbol.c_str(), (long long)std::time(nullptr), std::floor(bid + 0.5), std::floor(ask + 0.5));
    }
    */
    // default: 4 decimal places with real timestamp
    return _snprintf(buf, (int)bufsize, "%s %lld %.4f %.4f\r\n", symbol.c_str(), (long long)std::time(nullptr), bid, ask);
}
/*
// Directory where per-client raw dumps will be written
static std::string g_dump_dir = "C:\\datafeed";

// Persist raw per-client outbound stream for diagnostics (writes to absolute path)
static void DumpClientRaw(int clientId, const char* data, int len) {
    try {
        std::lock_guard<std::mutex> lk(g_log_mutex);
#ifdef _WIN32
        // ensure directory exists (no error if already exists)
        _mkdir(g_dump_dir.c_str());
#else
        mkdir(g_dump_dir.c_str(), 0755);
#endif
        std::ostringstream fname;
        fname << g_dump_dir << "\\uni_client_" << clientId << ".log";
        std::ofstream ofs(fname.str(), std::ios::app | std::ios::binary);
        if (ofs.is_open()) {
            ofs.write(data, len);
            ofs.flush();
        }
    } catch(...) {
        // ignore diagnostics write failures
    }
}
*/
static void BroadcastQuote(const char* ctpInstrument, double bid, double ask) {
    // CTP 原始 InstrumentID 通常为大写（RU2701），内部统一小写后再生成别名
    std::string ctpSym = LowerCopy(std::string(ctpInstrument));
    std::string ctpSymUp = UpperCopy(ctpSym);
    std::string contSym = ContinuousAlias(ctpSym);       // e.g. ru2701 -> ru
    std::string contSymUp = UpperCopy(contSym);
    std::string contractSym = ContractAlias(ctpSym);    // e.g. ru2701 -> ru2701s
    std::string contractSymUp = UpperCopy(contractSym);

    char bufCtp[256], bufCont[256], bufContr[256];
    int lenCtp = FormatTickLine(ctpSym, bufCtp, sizeof(bufCtp), bid, ask);
    int lenCont = FormatTickLine(contSym, bufCont, sizeof(bufCont), bid, ask);
    int lenContr = FormatTickLine(contractSym, bufContr, sizeof(bufContr), bid, ask);
    if (lenCtp <= 0 || lenCont <= 0 || lenContr <= 0) return;

    std::lock_guard<std::mutex> lk(g_clients_mutex);
    if (g_client_sockets.empty()) return;

    for (auto it = g_client_sockets.begin(); it != g_client_sockets.end();) {
        SOCKET s = *it;
        auto subs_it = g_client_subs.find((int)s);
        // 如果 subs 还没被 ClientHandler 设置（空 vector），跳过——不推不拒
        if (subs_it == g_client_subs.end()) { ++it; continue; }
        if (subs_it->second.empty()) { ++it; continue; }
        // 有订阅列表，检查是否匹配
        bool subscribed = false;
        for (const auto &sym : subs_it->second) {
            if (sym == "*") { subscribed = true; break; }
            if (sym == ctpSymUp || sym == contSymUp || sym == contractSymUp) {
                subscribed = true; break;
            }
        }
        if (!subscribed) { ++it; continue; }

        bool ok = true;
        ok = ok && (send(s, bufCtp, lenCtp, 0) == lenCtp);
        ok = ok && (send(s, bufCont, lenCont, 0) == lenCont);
        ok = ok && (send(s, bufContr, lenContr, 0) == lenContr);
        if (!ok) { remove_client(s); it = g_client_sockets.begin(); continue; }
        ++it;
    }
}

static void ClientHandler(SOCKET client) {
    const char* login_prompt = "Login:\r\n";
    send_all(client, login_prompt, (int)strlen(login_prompt));
    char ch;
    // determine client IP for logging
    std::string client_ip = "unknown";
    {
        struct sockaddr_in peer;
        int peerlen = sizeof(peer);
        if (getpeername(client, (struct sockaddr*)&peer, &peerlen) == 0) {
            client_ip = sockaddr_in_to_string(peer);
        }
    }
    auto recv_line = [&](std::string &out)->bool {
        out.clear();
        while (true) {
            int r = recv(client, &ch, 1, 0);
            if (r <= 0) return false;
            if (ch == '\r') continue;
            if (ch == '\n') break;
            out.push_back(ch);
            if (out.size() > 1024) break;
        }
        return true;
    };

    std::string login_line;
    if (!recv_line(login_line)) { remove_client(client); return; }
    const char* pass_prompt = "Password: \r\n";
    send_all(client, pass_prompt, (int)strlen(pass_prompt));
    std::string password_line;
    if (!recv_line(password_line)) { remove_client(client); return; }
    const char* access_line = "Access \r\n";
    send_all(client, access_line, (int)strlen(access_line));
    // validate credentials
    const char* granted_line = "granted\r\n";
    const char* denied_line  = "denied\r\n";
    if (login_line == g_feed_login && password_line == g_feed_password) {
        send_all(client, granted_line, (int)strlen(granted_line));
        //LogMessage(std::string("FeedServer: client ") + client_ip + " authenticated as '" + login_line + "'");
    } else {
        send_all(client, denied_line, (int)strlen(denied_line));
        //LogMessage(std::string("FeedServer: client ") + client_ip + " failed auth using login '" + login_line + "'");
        // close connection
        remove_client(client);
        return;
    }
    // after granted, expect subscription line
    std::string sline;
    
    // 给客户端最多 500ms 发送订阅行——零超时会导致几乎所有客户端都默认 '*'
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(client, &readfds);
    timeval tv = {0, 500000}; // 500ms 等待客户端发送订阅行
    
    if (select((int)client + 1, &readfds, NULL, NULL, &tv) > 0 && FD_ISSET(client, &readfds)) {
        // 有数据可读，尝试读出完整一行
        if (!recv_line(sline)) {
            //LogMessage(std::string("FeedServer: failed to read available subscription line from client ") + client_ip);
            sline = "*";
        }
    } else {
        // 500ms 内没收到订阅行，降级为全订阅
        //LogMessage(std::string("FeedServer: no subscription line received from client ") + client_ip + ", applying default '*' subscription");
        sline = "*";
    }
    
    // log received subscription line (raw)
    //LogMessage(std::string("FeedServer: received post-auth line from ") + client_ip + ": '" + sline + "'");

    std::vector<std::string> subs;
    const std::string prefix = "> Symbols:";
    size_t pos = sline.find(prefix);
    if (pos != std::string::npos) {
        // 订阅格式："> Symbols:IF;IH;IC;..." 或 "> Symbols:IF IH IC ..."
        std::string tail = sline.substr(pos + prefix.size());
        std::istringstream ss(tail);
        std::string tok;
        while (std::getline(ss, tok, ';')) { 
            // trim
            while(!tok.empty() && isspace((unsigned char)tok.front())) tok.erase(tok.begin());
            while(!tok.empty() && isspace((unsigned char)tok.back())) tok.pop_back();
            if (!tok.empty()) {
                // 订阅统一转为大写以避免大小写不匹配
                std::transform(tok.begin(), tok.end(), tok.begin(), [](unsigned char c){ return std::toupper(c); });
                subs.push_back(tok);
            }
        }
    } else {
        // 降级：整行按分号或空格分割解析
        std::istringstream ss(sline);
        std::string tok;
        while (std::getline(ss, tok, ';')) {
            // trim whitespace in-place
            while(!tok.empty() && std::isspace((unsigned char)tok.front())) tok.erase(tok.begin());
            while(!tok.empty() && std::isspace((unsigned char)tok.back())) tok.pop_back();
            if (!tok.empty()) {
                std::transform(tok.begin(), tok.end(), tok.begin(), [](unsigned char c){ return std::toupper(c); });
                subs.push_back(tok);
            }
        }
    }

    // if we ended up with no subscriptions, subscribe to all by default
    if (subs.empty()) {
        subs.push_back(std::string("*"));
        //LogMessage(std::string("FeedServer: no subscriptions parsed from client ") + client_ip + ", defaulting to '*'");
    }

    {
        std::lock_guard<std::mutex> lk(g_clients_mutex);
        g_client_subs[(int)client] = subs;
    }
    // log subscription list
    {
        std::string out = std::string("FeedServer: client ") + client_ip + " subscribed to: ";
        for (size_t i=0;i<subs.size();++i) { if (i) out += ","; out += subs[i]; }
        //LogMessage(out);
    }

    // Send immediate response to acknowledge symbol subscription
    send_all(client, "> Subscription received", 23);
    
    // 给新连接客户端发送一次全量快照，让其立即拿到所有已知合约的 tick
    {
        std::lock_guard<std::mutex> data_lk(g_dataMutex);
        if (g_marketData.empty()) {
            // If no market data available, send a dummy tick to prevent timeout
            send_all(client, "IF 0 0.0 0.0", 15);
            //LogMessage(std::string("FeedServer: sent dummy tick to ") + client_ip + " (no market data available)");
        } else {
            for (const auto &pair : g_marketData) {
                const std::string &ctpSym = pair.first;  // 以 CTP 合约名为 key
                const MarketData &md = pair.second;
                std::string contSym = ContinuousAlias(ctpSym);
                std::string contractSym = ContractAlias(ctpSym);
                char sbuf[256];
                int slen;
                slen = FormatTickLine(ctpSym, sbuf, sizeof(sbuf), md.bidPrice, md.askPrice);
                if (slen > 0) send_all(client, sbuf, slen);
                slen = FormatTickLine(contSym, sbuf, sizeof(sbuf), md.bidPrice, md.askPrice);
                if (slen > 0) send_all(client, sbuf, slen);
                slen = FormatTickLine(contractSym, sbuf, sizeof(sbuf), md.bidPrice, md.askPrice);
                if (slen > 0) send_all(client, sbuf, slen);
            }
            //LogMessage(std::string("FeedServer: sent initial snapshot to ") + client_ip);
        }
    }

    // Track last data sent time to prevent timeout
    auto last_data_sent = std::chrono::steady_clock::now();
    
    while (g_run_server) {
        fd_set readfds; FD_ZERO(&readfds); FD_SET(client, &readfds);
        timeval tv; tv.tv_sec = 0; tv.tv_usec = 100000; // 100ms timeout for responsiveness
        int rv = select((int)client + 1, &readfds, NULL, NULL, &tv);
        if (rv < 0) break;
        
        // Check for incoming data from client
        if (rv > 0 && FD_ISSET(client, &readfds)) {
            int r = recv(client, &ch, 1, 0);
            if (r <= 0) break;
            if (ch != '\n') {
                while (true) { int rr = recv(client, &ch, 1, 0); if (rr <= 0) break; if (ch == '\n') break; }
            }
        }
        
        // Send keep-alive ping every 30 seconds to prevent timeout
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_data_sent);
        if (elapsed.count() >= 30) {
            const char* ping_msg = "> Ping\r\n";
            send_all(client, ping_msg, (int)strlen(ping_msg));
            last_data_sent = now;
            //LogMessage(std::string("FeedServer: sent ping to client ") + client_ip);
        }
    }

    remove_client(client);
}

//+------------------------------------------------------------------+
//| EA2ctp 桥接：品种前缀 → CTP 完整合约                            |
//| 给定 MT4 symbol 前缀（ru/ag/au），在 g_instruments 里找首个匹配   |
//| 例："ru" → "ru2701"（前缀匹配 + 长度 > 前缀）                    |
//+------------------------------------------------------------------+
static std::string MatchInstrument(const std::string &prefix) {
    // 先精确匹配（如果 EA 直接给了完整合约代码）
    for (const auto &inst : g_instruments) {
        if (inst == prefix) return inst;
    }
    // 前缀匹配（MT4 symbol 如 "ru" 匹配 "ru2701"）
    std::string p = prefix;
    // 转小写比较
    for (auto &c : p) c = std::tolower((unsigned char)c);
    for (const auto &inst : g_instruments) {
        if (inst.size() >= p.size() && inst.substr(0, p.size()) == p) return inst;
    }
    return "";
}

//+------------------------------------------------------------------+
//| EA2ctp 桥接：signal 文件 watcher 线程                             |
//| 每秒扫一次 ctp_signals.csv，读到新行 → 解析 → Submit             |
//| 行格式：BUY,symPrefix,type,offset,lots,bidAskDif,comment         |
//| 例：BUY,ru,M,OPEN,1,5,ma_golden                                  |
//+------------------------------------------------------------------+
// 判断是否绝对路径（盘符或根目录开头）
static bool IsAbsPath(const std::string &p) {
    if (p.empty()) return false;
    if (p[0] == '\\' || p[0] == '/') return true;
    return p.size() > 1 && p[1] == ':';
}

// 取 exe 所在目录，用于把配置里的相对路径锚定到程序自身位置，
// 使整个交付目录可整体拷贝而无需改配置
static std::string ExeDirPath() {
    char buf[MAX_PATH] = {0};
    if (!GetModuleFileNameA(NULL, buf, MAX_PATH)) return ".";
    std::string s(buf);
    size_t pos = s.find_last_of("\\/");
    return pos == std::string::npos ? std::string(".") : s.substr(0, pos);
}

static bool CtpMdActive();  // 前置声明，定义见下方行情回调区

static void SignalWatcher() {
    static std::atomic<uint64_t> eaSeq(0);
    std::string signalPath = g_eaSignalFile;
    if (!IsAbsPath(signalPath)) signalPath = ExeDirPath() + "\\" + signalPath;
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(800));
        // 交易或行情未就绪时不动信号文件：否则会把有效信号读走清空，
        // 而 Submit 因 ctp_not_ready 失败不落盘，导致信号丢失
        if (!g_ctpTrader.IsReady()) continue;
        if (!CtpMdActive()) continue;
        std::ifstream ifs(signalPath);
        if (!ifs.good()) continue;

        std::vector<std::string> lines;
        std::string line;
        while (std::getline(ifs, line)) {
            // 去 \r
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty() || line[0] == '#') continue;
            lines.push_back(line);
        }
        ifs.close();
        if (lines.empty()) continue;

        // 清空原文件（EA 端用 FILE_OPEN_ALWAYS 重新写）
        std::ofstream ofs(signalPath, std::ios::trunc);
        ofs.close();

        for (const auto &raw : lines) {
            // CSV 解析（7 列：action,sym,type,offset,lots,bidAskDif,comment）
            std::vector<std::string> fields;
            std::istringstream ss(raw);
            std::string tok;
            while (std::getline(ss, tok, ',')) fields.push_back(tok);
            if (fields.size() < 7) {
                std::cerr << "[EA2CTP] bad line (need 7 fields): " << raw << std::endl;
                continue;
            }
            std::string action   = trim_copy(fields[0]);  // BUY/SELL
            std::string symPre   = trim_copy(fields[1]);  // ru/ag/au
            std::string typeStr  = trim_copy(fields[2]);  // M/L/D
            std::string offset   = trim_copy(fields[3]);  // OPEN/CLOSE
            int lots             = atoi(trim_copy(fields[4]).c_str());
            double bidAskDif     = atof(trim_copy(fields[5]).c_str());
            std::string comment  = trim_copy(fields[6]);

            // 补全 CTP 合约代码
            std::string ctpSym = MatchInstrument(symPre);
            if (ctpSym.empty()) {
                std::cerr << "[EA2CTP] unknown symbol prefix: " << symPre
                          << " (instruments: ";
                for (const auto &i : g_instruments) std::cerr << i << ",";
                std::cerr << ")" << std::endl;
                continue;
            }

            // 拼 OrderCommand
            OrderCommand cmd;
            cmd.buy   = (action == "BUY" || action == "buy");
            cmd.lots  = lots;
            cmd.symbol = ctpSym;
            cmd.type  = typeStr.empty() ? 'M' : (char)std::toupper(typeStr[0]);
            cmd.open  = (offset == "OPEN" || offset == "open");
            cmd.bidAskDif = bidAskDif;
            cmd.comment = comment;
            cmd.loginNr = g_user;
            cmd.groupName = "ea_ma";

            // 用当前行情 bid/ask 填参考价
            {
                std::lock_guard<std::mutex> lk(g_dataMutex);
                auto it = g_marketData.find(ctpSym);
                if (it != g_marketData.end() && it->second.connected) {
                    cmd.price = cmd.buy ? it->second.askPrice : it->second.bidPrice;
                }
            }
            if (cmd.price <= 0.0) cmd.price = 0.0;  // 无行情 → CTP 端会拒

            uint64_t seq = ++eaSeq;
            std::cout << "[EA2CTP] submitting #" << seq << ": "
                      << action << "," << ctpSym << "," << cmd.type << ","
                      << (cmd.open ? "OPEN" : "CLOSE") << ",lot=" << cmd.lots
                      << ",price=" << cmd.price
                      << ",comment=" << comment << std::endl;

            OrderReply reply = g_orderRouter.Submit(cmd);
            std::cout << "[EA2CTP] reply: " << reply.text << std::endl;
        }
    }
}

static void ServerAcceptLoop(int listen_port) {
    SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock == INVALID_SOCKET) { std::cerr << "FeedServer: unable to create listen socket\n"; return; }
    BOOL opt = TRUE; setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
    struct sockaddr_in servaddr; memset(&servaddr,0,sizeof(servaddr));
    servaddr.sin_family = AF_INET; servaddr.sin_port = htons((unsigned short)listen_port); servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(listen_sock, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0) { std::cerr << "FeedServer: bind failed\n"; closesocket(listen_sock); return; }
    if (listen(listen_sock, 5) != 0) { std::cerr << "FeedServer: listen failed\n"; closesocket(listen_sock); return; }
    while (g_run_server) {
        struct sockaddr_in cliaddr; int addrlen = sizeof(cliaddr);
        SOCKET client = accept(listen_sock, (struct sockaddr*)&cliaddr, &addrlen);
        if (client == INVALID_SOCKET) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); continue; }
        {
            std::lock_guard<std::mutex> lk(g_clients_mutex);
            g_client_sockets.push_back(client);
            // 不在此处创建空 vector——等 ClientHandler 完整握手后再设置订阅列表
            // 这样 BroadcastQuote 遍历到 "subs not found" 时会跳过（而不是误当全订阅）
        }
        std::thread t(ClientHandler, client);
        t.detach();
    }
    closesocket(listen_sock);
}

static void parse_config_ini() {
    std::ifstream ifs("config.ini");
    if (!ifs.is_open()) {
        std::cout << "Warning: config.ini not found, using default values" << std::endl;
        return;
    }
    
    std::string line;
    while (std::getline(ifs, line)) {
        auto posc = line.find_first_of("#;");
        if (posc != std::string::npos) line = line.substr(0, posc);
        line = trim_copy(line);
        if (line.empty()) continue;
        
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        
        std::string key = trim_copy(line.substr(0, pos));
        std::string val = trim_copy(line.substr(pos + 1));
        std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c){ return std::tolower(c); });
        
        if (key == "brokerid" || key == "broker") g_brokerID = val;
        else if (key == "userlogin" || key == "user") g_user = val;
        else if (key == "userpassword" || key == "password" || key == "pass") g_password = val;
        else if (key == "feed_login" || key == "feedlogin") g_feed_login = val;
        else if (key == "feed_password" || key == "feedpassword") g_feed_password = val;
        else if (key == "feedport" || key == "port") {
            int p = atoi(val.c_str());
            if (p > 0 && p < 65536) g_feed_port = p;
        }
        else if (key == "connectaddress" || key == "marketaddr") {
            g_marketAddrs.clear();
            std::istringstream ss(val);
            std::string addr;
            while (std::getline(ss, addr, ',')) {
                addr = trim_copy(addr);
                if (!addr.empty()) g_marketAddrs.push_back(addr);
            }
        }
        else if (key == "instruments") {
            g_instruments.clear();
            std::istringstream ss(val);
            std::string inst;
            while (std::getline(ss, inst, ',')) {
                inst = trim_copy(inst);
                if (!inst.empty()) g_instruments.push_back(inst);
            }
        }
        else if (key == "ea_signal_file" || key == "signal_file") {
            g_eaSignalFile = trim_copy(val);
        }
        else if (key == "tradeaddress" || key == "tradeaddr") {
            g_tradeAddrs.clear();
            std::istringstream ss(val);
            std::string addr;
            while (std::getline(ss, addr, ',')) {
                addr = trim_copy(addr);
                if (!addr.empty()) g_tradeAddrs.push_back(addr);
            }
        }
        else if (key == "market_source" || key == "marketsource") {
            std::string v = LowerCopy(val);
            if (v == "both" || v == "ctp" || v == "gateway") g_marketSource = v;
        }
        else if (key == "order_mode" || key == "ordermode") {
            std::string v = LowerCopy(val);
            if (v == "parallel" || v == "ctp_first" || v == "ctp_only") g_orderMode = v;
        }
        else if (key == "order_port" || key == "orderport") {
            int p = atoi(val.c_str());
            if (p > 0 && p < 65536) g_orderPort = p;
        }
        else if (key == "ctp_first_timeout_ms" || key == "ctpfirsttimeoutms") {
            int v = atoi(val.c_str());
            if (v > 0) g_ctpFirstTimeoutMs = v;
        }
        else if (key == "gw_base" || key == "gwbase") g_gwBase = val;
        else if (key == "gw_token" || key == "gwtoken") g_gwToken = val;
        else if (key == "appid" || key == "app_id") g_appID = val;
        else if (key == "authcode" || key == "auth_code") g_authCode = val;
        else if (key == "userproductinfo" || key == "user_product_info") g_productInfo = val;
    }
}

static void parse_symb_config() {
    std::ifstream ifs("symbConfig.ini");
    if (!ifs.is_open()) return;
    
    std::string line;
    while (std::getline(ifs, line)) {
        auto posc = line.find_first_of("#;");
        if (posc != std::string::npos) line = line.substr(0, posc);
        line = trim_copy(line);
        if (line.empty()) continue;
        
        auto pos = line.find('=');
        if (pos == std::string::npos) continue;
        
        std::string left = trim_copy(line.substr(0, pos));
        std::string right = trim_copy(line.substr(pos + 1));
        if (!left.empty() && !right.empty()) {
            g_symbMap[left] = right;
        }
    }
}

static void BuildReverseSymbolMap() {
    g_symbRevMap.clear();
    for (const auto &p : g_symbMap) {
        const std::string &ctp = p.first;
        const std::string &alias = p.second;
        if (!alias.empty()) g_symbRevMap[alias] = ctp;
    }
}

// 网关品种映射：ru2701 -> ru（小写字母前缀）
static void BuildGatewaySymbolMap() {
    g_gwSymToContract.clear();
    for (const auto &inst : g_instruments) {
        std::string prefix = LowerCopy(ContractPrefix(inst));
        if (!prefix.empty() &&
            g_gwSymToContract.find(prefix) == g_gwSymToContract.end()) {
            g_gwSymToContract[prefix] = inst;
        }
    }
}

static std::vector<std::string> GatewaySymbols() {
    std::vector<std::string> out;
    for (const auto &p : g_gwSymToContract) out.push_back(p.first);
    return out;
}

// CTP 行情是否处于主路可用状态（最近 10 秒有 tick）
static bool CtpMdActive() {
    if (g_marketSource == "gateway") return false;
    if (!g_ctpMdConnected.load()) return false;
    return (NowMs() - g_ctpMdLastTickMs.load()) < 10000;
}

// 网关行情作为备路：CTP 主路正常时忽略
static void OnGatewayTick(const std::string &gwSymbol, double bid, double ask) {
    if (g_marketSource == "ctp") return;
    if (CtpMdActive()) return;

    std::string ctpSym;
    auto it = g_gwSymToContract.find(LowerCopy(gwSymbol));
    if (it != g_gwSymToContract.end()) {
        ctpSym = it->second;
    } else {
        for (const auto &inst : g_instruments) {
            if (UpperCopy(inst) == UpperCopy(gwSymbol)) {
                ctpSym = inst;
                break;
            }
        }
    }
    if (ctpSym.empty()) return;

    MarketData data;
    data.bidPrice = bid;
    data.askPrice = ask;
    data.lastPrice = 0.0;
    data.volume = 0.0;
    data.updateTime = "";
    data.connected = true;
    {
        std::lock_guard<std::mutex> lock(g_dataMutex);
        g_marketData[ctpSym] = data;
    }
    BroadcastQuote(ctpSym.c_str(), bid, ask);
}

// 网关持仓轮询发现新仓，作为成交事件广播
static void OnGatewayPosition(const GwPosition &p) {
    char buf[160];
    _snprintf(buf, sizeof(buf), "TRADE %ld,%.4f,%.2f,GW",
              p.ticket, p.openPrice, p.lots);
    buf[sizeof(buf) - 1] = '\0';
    LogMessage("Gateway position opened: " + std::string(buf));
    g_orderServer.BroadcastLine(std::string(buf));
}

// CTP Spi Implementation
class CSimpleMdSpi : public CThostFtdcMdSpi {
private:
    CThostFtdcMdApi* m_pMdApi;
    std::string m_brokerID;
    std::string m_userID;
    std::string m_password;
    
public:
    CSimpleMdSpi(const std::string& brokerID, const std::string& userID, const std::string& password)
        : m_brokerID(brokerID), m_userID(userID), m_password(password), m_pMdApi(nullptr) {}
    
    ~CSimpleMdSpi() {
        if (m_pMdApi) {
            m_pMdApi->Release();
            m_pMdApi = nullptr;
        }
    }
    
    void Connect(const std::string& frontAddr) {
        m_pMdApi = CThostFtdcMdApi::CreateFtdcMdApi("", false, false);
        m_pMdApi->RegisterSpi(this);
        m_pMdApi->RegisterFront(const_cast<char*>(frontAddr.c_str()));
        m_pMdApi->Init();
        std::cout << "Connecting to CTP server: " << frontAddr << std::endl;
    }
    
    void Login() {
        CThostFtdcReqUserLoginField req = {0};
        // Use safer string copying methods
        m_brokerID.copy(req.BrokerID, sizeof(req.BrokerID) - 1);
        req.BrokerID[sizeof(req.BrokerID) - 1] = '\0';
        m_userID.copy(req.UserID, sizeof(req.UserID) - 1);
        req.UserID[sizeof(req.UserID) - 1] = '\0';
        m_password.copy(req.Password, sizeof(req.Password) - 1);
        req.Password[sizeof(req.Password) - 1] = '\0';
        
        int ret = m_pMdApi->ReqUserLogin(&req, 0);
        std::cout << "Login request sent, result: " << ret << std::endl;
    }
    
    void SubscribeMarketData(const std::vector<std::string>& instruments) {
        char** ppInstrumentID = new char*[instruments.size()];
        for (size_t i = 0; i < instruments.size(); i++) {
            ppInstrumentID[i] = const_cast<char*>(instruments[i].c_str());
        }
        
        int ret = m_pMdApi->SubscribeMarketData(ppInstrumentID, static_cast<int>(instruments.size()));
        std::cout << "Subscribe market data, result: " << ret << std::endl;
        
        delete[] ppInstrumentID;
    }
    
    // CTP Callbacks
    virtual void OnFrontConnected() {
        std::cout << "CTP Front Connected" << std::endl;
        g_ctpMdConnected = true;
        Login();
    }
    
    virtual void OnFrontDisconnected(int nReason) {
        std::cout << "CTP Front Disconnected, reason: " << nReason << std::endl;
        g_ctpMdConnected = false;
        g_ctpMdLastTickMs = 0;
        {
            std::lock_guard<std::mutex> lock(g_dataMutex);
            for (auto& pair : g_marketData) {
                pair.second.connected = false;
            }
        }
    }
    
    virtual void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, 
                               CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) {
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            std::cout << "Login failed, ErrorID: " << pRspInfo->ErrorID 
                     << ", ErrorMsg: " << pRspInfo->ErrorMsg << std::endl;
        } else {
            std::cout << "Login successful" << std::endl;
            SubscribeMarketData(g_instruments);
        }
    }
    
    virtual void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData) {
        if (!pDepthMarketData) return;
        g_ctpMdLastTickMs = NowMs();
        if (g_marketSource == "gateway") return;  // 只允许网关行情

        std::string instrumentID = pDepthMarketData->InstrumentID;
        
        MarketData data;
        data.bidPrice = pDepthMarketData->BidPrice1;
        data.askPrice = pDepthMarketData->AskPrice1;
        data.lastPrice = pDepthMarketData->LastPrice;
        data.volume = pDepthMarketData->Volume;
        data.updateTime = pDepthMarketData->UpdateTime;
        data.connected = true;
        
        {
            std::lock_guard<std::mutex> lock(g_dataMutex);
            g_marketData[instrumentID] = data;
        }
        try {
            BroadcastQuote(instrumentID.c_str(), data.bidPrice, data.askPrice);
        } catch(...) {
            // ignore send errors
        }
    }
};

// Display function
void display_market_data() {
    while (true) {
        // Clear screen (Windows)
        #ifdef _WIN32
        system("cls");
        #else
        system("clear");
        #endif
        
        std::cout << "=== CTP Real-time Market Data (Bid/Ask) ===" << std::endl;
        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        char time_buffer[26];
        ctime_s(time_buffer, sizeof(time_buffer), &now_time);
        std::cout << "Time: " << time_buffer;
        std::cout << std::endl;
        
        std::lock_guard<std::mutex> lock(g_dataMutex);
        
        std::cout << std::left << std::setw(15) << "Symbol" 
                  << std::setw(12) << "Bid Price" 
                  << std::setw(12) << "Ask Price" 
                  << std::setw(12) << "Last Price" 
                  << std::setw(12) << "Volume" 
                  << std::setw(15) << "Update Time" 
                  << std::setw(10) << "Status" 
                  << std::endl;
        std::cout << std::string(80, '-') << std::endl;
        
        for (const auto& instrument : g_instruments) {
            std::string displaySymbol = ContractAlias(instrument);  // 合约别名（带 s）
            
            auto it_data = g_marketData.find(instrument);
            if (it_data != g_marketData.end()) {
                const MarketData& data = it_data->second;
                std::cout << std::left << std::setw(15) << displaySymbol
                          << std::setw(12) << std::fixed << std::setprecision(2) << data.bidPrice
                          << std::setw(12) << data.askPrice
                          << std::setw(12) << data.lastPrice
                          << std::setw(12) << data.volume
                          << std::setw(15) << data.updateTime
                          << std::setw(10) << (data.connected ? "Connected" : "Disconnected")
                          << std::endl;
            } else {
                std::cout << std::left << std::setw(15) << displaySymbol
                          << std::setw(12) << "N/A"
                          << std::setw(12) << "N/A"
                          << std::setw(12) << "N/A"
                          << std::setw(12) << "N/A"
                          << std::setw(15) << "N/A"
                          << std::setw(10) << "Waiting"
                          << std::endl;
            }
        }
        
        std::cout << std::endl << "Press Ctrl+C to exit..." << std::endl;
        
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

int main() {
    std::cout << "Simple CTP Console Program - Loading configuration..." << std::endl;
    
    // Parse configuration
    parse_config_ini();
    parse_symb_config();
    // prepare reverse mapping (alias -> CTP) so clients can subscribe using either name
    BuildReverseSymbolMap();
    // 网关品种（小写前缀）映射
    BuildGatewaySymbolMap();
    // open log file (append)
    g_log_ofs.open("feedserver.log", std::ios::app);
    
    // Initialize Winsock on Windows
    #ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed" << std::endl;
        return -1;
    }
    // start TCP feed server (listening on g_feed_port)
    std::thread(ServerAcceptLoop, g_feed_port).detach();

    // EA2ctp 桥接：signal file watcher
    std::cout << "- EA2ctp signal watcher: " << g_eaSignalFile << std::endl;
    std::thread(SignalWatcher).detach();
    #endif
    
    std::cout << "Configuration loaded:" << std::endl;
    std::cout << "- User: " << g_user << std::endl;
    std::cout << "- Market Addresses: " << std::endl;
    for (const auto& addr : g_marketAddrs) {
        std::cout << "  * " << addr << std::endl;
    }
    std::cout << "- Instruments: " << std::endl;
    for (const auto& inst : g_instruments) {
        std::cout << "  * " << inst << std::endl;
    }
    std::cout << "- Market source: " << g_marketSource
              << " | order mode: " << g_orderMode
              << " | order port: " << g_orderPort << std::endl;
    std::cout << "- Gateway: " << g_gwBase << std::endl;

    // ---- 交易与网关初始化 ----
    g_gwClient.Configure(g_gwBase, g_gwToken);
    g_gwClient.SetTickCallback([](const std::string &sym, double bid, double ask) {
        OnGatewayTick(sym, bid, ask);
    });
    g_gwClient.SetPositionCallback([](const GwPosition &p) {
        OnGatewayPosition(p);
    });

    g_orderRouter.Configure(g_orderMode, g_ctpFirstTimeoutMs, "orders.csv");
    g_orderRouter.SetCtp(&g_ctpTrader);
    g_orderRouter.SetGw(&g_gwClient);
    g_orderRouter.SetNotify([](const std::string &line) {
        g_orderServer.BroadcastLine(line);
    });
    g_orderRouter.Start();

    g_orderServer.Configure(g_orderPort);
    g_orderServer.SetRouter(&g_orderRouter);
    g_orderServer.Start();

    g_ctpTrader.Configure(g_brokerID, g_user, g_password, g_appID, g_authCode, g_productInfo);
    g_ctpTrader.SetTradeCallback([](const CtpTradeEvent &ev) {
        g_orderRouter.OnCtpTrade(ev);
    });
    g_ctpTrader.SetOrderCallback([](const CtpOrderEvent &ev) {
        g_orderRouter.OnCtpOrder(ev);
    });
    // tradeaddress 留空时沿用 connectaddress（行情前置），保证配置齐全即启用交易
    if (g_tradeAddrs.empty()) {
        g_tradeAddrs = g_marketAddrs;
        LogMessage("tradeaddress 未配置，沿用 connectaddress 作为交易前置");
    }
    g_ctpTrader.Start(g_tradeAddrs);

    if (g_gwClient.IsConfigured()) {
        g_gwClient.StartPositionPoll();
        if (g_marketSource != "ctp") {
            g_gwClient.StartStream(GatewaySymbols());
        }
    } else {
        LogMessage("gateway base not configured");
    }

    // Create CTP SPI instance
    CSimpleMdSpi spi(g_brokerID, g_user, g_password);
    
    // Connect to first available market address
    if (!g_marketAddrs.empty()) {
        spi.Connect(g_marketAddrs[0]);
    } else {
        std::cerr << "No market addresses configured" << std::endl;
        return -1;
    }
    
    // Start display thread
    std::thread display_thread(display_market_data);
    
    // Wait for user to press Ctrl+C
    std::cout << "Program started. Press Ctrl+C to exit." << std::endl;
    
    #ifdef _WIN32
    // Windows: wait for console input
    std::cin.get();
    #else
    // Linux: wait for signal
    pause();
    #endif
    
    // Cleanup
    display_thread.detach();
    g_orderServer.Stop();
    g_orderRouter.Stop();
    g_gwClient.StopAll();
    g_ctpTrader.Stop();

    #ifdef _WIN32
    WSACleanup();
    #endif
    if (g_log_ofs.is_open()) g_log_ofs.close();
    
    return 0;
}
