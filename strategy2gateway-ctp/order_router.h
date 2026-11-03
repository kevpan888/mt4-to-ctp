#pragma once
#include <string>
#include <map>
#include <deque>
#include <mutex>
#include <atomic>
#include <memory>
#include <thread>
#include <condition_variable>
#include <functional>
#include <fstream>

#include "ctp_trader.h"
#include "gw_client.h"

// 解析后的 10 列指令
struct OrderCommand {
    bool buy = false;
    int lots = 0;
    std::string symbol;      // CTP 合约级：ru2701
    double price = 0.0;
    std::string loginNr;     // 只落本地订单表与日志，不发两侧
    std::string groupName;   // 同上
    char type = 'M';         // M/L/S/D
    bool open = true;        // OPEN/CLOSE
    double bidAskDif = 0.0;
    std::string comment;
};

struct OrderReply {
    bool ok = false;
    std::string text;
};

class OrderRouter {
public:
    void Configure(const std::string &mode, int ctpFirstTimeoutMs,
                   const std::string &ledgerFile);
    void SetCtp(CtpTrader *ctp);
    void SetGw(GwClient *gw);
    void SetNotify(std::function<void(const std::string &)> fn);

    void Start();
    void Stop();

    OrderReply Submit(const OrderCommand &cmd);

    // CTP 回调转投
    void OnCtpTrade(const CtpTradeEvent &ev);
    void OnCtpOrder(const CtpOrderEvent &ev);

private:
    struct PendingFirst {
        std::string groupKey;
        std::string symbol;
        bool buy = true;
        std::string comment;
        int ctpFilled = 0;
        int gwSent = 0;
        long long deadlineMs = 0;
    };

    // parallel 模式下网关腿的异步请求：主线程等最多 3s，超时先回 PENDING，
    // worker 线程拿到真实 ticket 后补落盘并推送 GW_TICKET 广播
    struct GwAsyncPending {
        std::string groupKey;
        OrderCommand cmd;          // 用于 PlaceGw
        int lots = 0;
        double price = 0.0;        // 指令原价（网关用）
        // 结果
        std::atomic<bool> ready{false};
        GwOrderResult result;
    };

    OrderReply HandleCancel(const OrderCommand &cmd);
    GwOrderResult PlaceGw(const OrderCommand &cmd, int lots, double price);
    void HandleFill(const CtpTradeEvent &ev);
    void CheckDeadlines();
    void WorkerLoop();
    void GwAsyncLoop();
    void Persist(const std::string &groupKey, const std::string &source,
                 const std::string &key, const std::string &symbol,
                 const std::string &action, const std::string &offset,
                 const std::string &type, double price, int volume,
                 int filled, const std::string &status,
                 const std::string &login, const std::string &group,
                 const std::string &comment);
    void Notify(const std::string &line);
    std::string NextGroupKey();

    std::atomic<int> m_groupSeq{0};

    std::string m_mode = "parallel";
    int m_timeoutMs = 3000;
    std::string m_ledgerFile = "orders.csv";

    CtpTrader *m_ctp = nullptr;
    GwClient *m_gw = nullptr;
    std::function<void(const std::string &)> m_notify;

    std::mutex m_pendingMutex;
    std::map<std::string, PendingFirst> m_pending;

    std::mutex m_qMutex;
    std::condition_variable m_qCv;
    std::deque<CtpTradeEvent> m_queue;
    std::atomic<bool> m_stop{false};
    std::thread *m_worker = nullptr;

    // parallel 模式网关腿异步 worker
    std::mutex m_gwAsyncMutex;
    std::condition_variable m_gwAsyncCv;
    std::deque<std::shared_ptr<GwAsyncPending>> m_gwAsyncQueue;
    std::thread *m_gwAsyncThread = nullptr;

    // 3s 有界等待：每个 Submit 投递后 wait_for，被 worker 通知或超时
    std::mutex m_gwWaitMutex;
    std::condition_variable m_gwWaitCv;

    std::mutex m_fileMutex;
    std::ofstream m_file;
    bool m_fileReady = false;
};
