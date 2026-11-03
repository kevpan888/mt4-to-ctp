#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>

#include "order_router.h"

// 4445 文本指令端口：每行一条 10 列 CSV，逐条应答，并异步广播 TRADE 行
class OrderServer {
public:
    void Configure(int port);
    void SetRouter(OrderRouter *router);
    void Start();
    void Stop();
    void BroadcastLine(const std::string &line);

private:
    void AcceptLoop();
    void ClientLoop(unsigned long long sock);
    void RemoveClient(unsigned long long sock);

    int m_port = 0;
    OrderRouter *m_router = nullptr;
    std::atomic<bool> m_stop{false};
    std::thread *m_acceptThread = nullptr;

    std::mutex m_clientsMutex;
    std::vector<unsigned long long> m_clients;
};
