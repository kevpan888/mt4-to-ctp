#include "order_server.h"
#include "app_util.h"

#include <winsock2.h>
#include <windows.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

bool SendRaw(SOCKET s, const std::string &data) {
    int total = 0;
    int len = (int)data.size();
    while (total < len) {
        int n = send(s, data.c_str() + total, len - total, 0);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}

void SendLine(SOCKET s, const std::string &line) {
    SendRaw(s, line + "\r\n");
}

// 解析 10 列 CSV：BUYorSELL, lotnr, symbol, price, loginNr, groupName,
//                orderType, OPENorCLOSE, BidAskDif, comment
bool ParseCommand(const std::string &line, OrderCommand &cmd, std::string &err) {
    std::vector<std::string> t = SplitCsv(line);
    if (t.size() < 8) {
        err = "ERR bad format, need 8+ fields";
        return false;
    }
    std::string action = UpperCopy(t[0]);
    if (action == "BUY") cmd.buy = true;
    else if (action == "SELL") cmd.buy = false;
    else {
        err = "ERR bad action";
        return false;
    }
    cmd.lots = std::atoi(t[1].c_str());
    cmd.symbol = t[2];
    cmd.price = std::atof(t[3].c_str());
    cmd.loginNr = t[4];
    cmd.groupName = t[5];
    std::string typeStr = UpperCopy(t[6]);
    cmd.type = typeStr.empty() ? '?' : typeStr[0];
    std::string oc = UpperCopy(t[7]);
    if (oc == "OPEN") cmd.open = true;
    else if (oc == "CLOSE") cmd.open = false;
    else {
        err = "ERR bad OPENorCLOSE";
        return false;
    }
    cmd.bidAskDif = t.size() > 8 ? std::atof(t[8].c_str()) : 0.0;

    std::string comment;
    for (size_t i = 9; i < t.size(); ++i) {
        if (!comment.empty()) comment += ",";
        comment += t[i];
    }
    cmd.comment = comment;
    return true;
}

} // namespace

void OrderServer::Configure(int port) {
    m_port = port;
}

void OrderServer::SetRouter(OrderRouter *router) {
    m_router = router;
}

void OrderServer::Start() {
    if (m_port <= 0) {
        LogMessage("OrderServer: order_port not configured, order entry disabled");
        return;
    }
    if (m_acceptThread) return;
    m_stop = false;
    m_acceptThread = new std::thread(&OrderServer::AcceptLoop, this);
}

void OrderServer::Stop() {
    m_stop = true;
    {
        std::lock_guard<std::mutex> lk(m_clientsMutex);
        for (size_t i = 0; i < m_clients.size(); ++i) {
            closesocket((SOCKET)m_clients[i]);
        }
        m_clients.clear();
    }
    if (m_acceptThread) {
        m_acceptThread->join();
        delete m_acceptThread;
        m_acceptThread = nullptr;
    }
}

void OrderServer::AcceptLoop() {
    SOCKET listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listenSock == INVALID_SOCKET) {
        LogMessage("OrderServer: create socket failed");
        return;
    }
    BOOL opt = TRUE;
    setsockopt(listenSock, SOL_SOCKET, SO_REUSEADDR, (const char *)&opt, sizeof(opt));

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)m_port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listenSock, (sockaddr *)&addr, sizeof(addr)) != 0) {
        LogMessage("OrderServer: bind failed on port " + std::to_string(m_port));
        closesocket(listenSock);
        return;
    }
    if (listen(listenSock, 5) != 0) {
        LogMessage("OrderServer: listen failed");
        closesocket(listenSock);
        return;
    }
    LogMessage("OrderServer listening on port " + std::to_string(m_port));

    while (!m_stop.load()) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(listenSock, &rfds);
        timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        int sel = select((int)listenSock + 1, &rfds, nullptr, nullptr, &tv);
        if (sel <= 0) continue;
        SOCKET client = accept(listenSock, nullptr, nullptr);
        if (client == INVALID_SOCKET) continue;
        {
            std::lock_guard<std::mutex> lk(m_clientsMutex);
            m_clients.push_back((unsigned long long)client);
        }
        LogMessage("OrderServer: client connected");
        std::thread(&OrderServer::ClientLoop, this, (unsigned long long)client).detach();
    }
    closesocket(listenSock);
}

void OrderServer::RemoveClient(unsigned long long sock) {
    std::lock_guard<std::mutex> lk(m_clientsMutex);
    std::vector<unsigned long long>::iterator it =
        std::find(m_clients.begin(), m_clients.end(), sock);
    if (it != m_clients.end()) m_clients.erase(it);
}

void OrderServer::ClientLoop(unsigned long long sockRaw) {
    SOCKET client = (SOCKET)sockRaw;
    std::string pending;
    char buf[8192];

    while (!m_stop.load()) {
        int n = recv(client, buf, sizeof(buf), 0);
        if (n <= 0) break;
        pending.append(buf, n);
        size_t pos;
        while ((pos = pending.find('\n')) != std::string::npos) {
            std::string line = pending.substr(0, pos);
            pending.erase(0, pos + 1);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (line.empty()) continue;

            OrderCommand cmd;
            std::string err;
            if (!ParseCommand(line, cmd, err)) {
                SendLine(client, err);
                continue;
            }
            if (!m_router) {
                SendLine(client, "ERR router not ready");
                continue;
            }
            OrderReply reply = m_router->Submit(cmd);
            SendLine(client, reply.text);
        }
        if (pending.size() > 8192) pending.clear();
    }
    RemoveClient(sockRaw);
    closesocket(client);
    LogMessage("OrderServer: client disconnected");
}

void OrderServer::BroadcastLine(const std::string &line) {
    std::lock_guard<std::mutex> lk(m_clientsMutex);
    if (m_clients.empty()) return;
    std::string data = line + "\r\n";
    for (size_t i = 0; i < m_clients.size();) {
        SOCKET s = (SOCKET)m_clients[i];
        if (!SendRaw(s, data)) {
            closesocket(s);
            m_clients.erase(m_clients.begin() + i);
        } else {
            ++i;
        }
    }
}
