#pragma once
#include <string>
#include <vector>
#include <set>
#include <mutex>
#include <thread>
#include <atomic>
#include <functional>

struct GwOrderResult {
    bool ok = false;
    long ticket = 0;
    std::string error;
};

struct GwOrderInfo {
    long ticket = 0;
    std::string symbol;
};

struct GwPosition {
    long ticket = 0;
    std::string symbol;
    int cmd = 0;           // 0=buy 1=sell
    double lots = 0.0;
    double openPrice = 0.0;
};

// aq 网关客户端：HTTP + SSE，全部使用 Winsock 手写，无第三方依赖
class GwClient {
public:
    GwClient() = default;
    ~GwClient();

    void Configure(const std::string &base, const std::string &token);
    bool IsConfigured() const;

    void StartStream(const std::vector<std::string> &symbols);
    void StopStream();
    void StartPositionPoll();
    void StopPositionPoll();
    void StopAll();

    // symbol 传网关品种级代码（如 ru），不带月份
    GwOrderResult MarketOrder(const std::string &symbol, bool buy, int lots,
                              const std::string &comment);
    GwOrderResult LimitOrder(const std::string &symbol, bool buy, int lots,
                             double price, const std::string &comment);

    std::vector<GwOrderInfo> QueryOrders(bool *ok = nullptr);
    std::vector<GwPosition> QueryPositions(bool *ok = nullptr);

    // symbolFilter 空表示撤全部；返回成功撤单数
    int CancelAll(const std::string &symbolFilter, std::string &err, int &attempted);

    void SetTickCallback(std::function<void(const std::string &, double, double)> cb);
    void SetPositionCallback(std::function<void(const GwPosition &)> cb);

    long long LastTickMs() const;

private:
    void StreamLoop();
    void PollLoop();

    GwOrderResult DoOrder(const std::string &body);
    std::string BuildRequest(const std::string &method, const std::string &path,
                             const std::string &body, bool keepAlive);

    std::string m_base;
    std::string m_token;
    std::string m_host;
    int m_port = 80;

    std::atomic<bool> m_streamStop{false};
    std::atomic<bool> m_pollStop{false};
    std::atomic<bool> m_streamRunning{false};
    std::atomic<bool> m_pollRunning{false};
    std::atomic<long long> m_lastTickMs{0};

    std::thread *m_streamThread = nullptr;
    std::thread *m_pollThread = nullptr;

    std::mutex m_sockMutex;
    unsigned long long m_streamSock = 0;   // 仅用于 Stop 时打断阻塞

    std::vector<std::string> m_symbols;
    std::set<long> m_seenTickets;
    bool m_seedDone = false;

    std::function<void(const std::string &, double, double)> m_tickCb;
    std::function<void(const GwPosition &)> m_posCb;
};
