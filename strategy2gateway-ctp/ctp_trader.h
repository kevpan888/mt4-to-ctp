#pragma once
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>
#include <functional>
#include <thread>

#include "ThostFtdcTraderApi.h"

// 成交事件（推送给路由层）
struct CtpTradeEvent {
    std::string ref;        // 报单引用
    std::string instrument; // 合约
    bool buy = false;
    bool open = false;      // 是否开仓
    double price = 0.0;
    int volume = 0;
};

// 委托状态事件
struct CtpOrderEvent {
    std::string ref;
    char status = '?';
    int volume = 0;
    int filled = 0;
    std::string msg;
};

// CTP 交易接口封装：认证 -> 登录 -> 结算确认 -> 持仓查询 -> 报单/撤单
class CtpTrader {
public:
    CtpTrader() = default;
    ~CtpTrader();

    void Configure(const std::string &brokerID, const std::string &user,
                   const std::string &password, const std::string &appID,
                   const std::string &authCode, const std::string &productInfo);

    // 多前置逗号分隔地址；全部注册，由 API 自行容错
    bool Start(const std::vector<std::string> &fronts);
    void Stop();

    bool IsReady() const;
    bool IsConnected() const;
    std::string StatusText() const;

    std::string NextOrderRef();

    // marketOrder=true 时 TimeCondition=IOC，false 时 GFD；价格由调用方算好
    bool InsertOrder(const std::string &ref, const std::string &instrument,
                     bool buy, bool openOffset, bool marketOrder, double price,
                     int volume, const std::string &comment, std::string &errMsg);

    // instrumentFilter 为空或 "*" 表示全撤；返回成功发出的撤单请求数
    int CancelAll(const std::string &instrumentFilter, std::string &errMsg);

    void RefreshPositions();

    void SetTradeCallback(std::function<void(const CtpTradeEvent &)> cb);
    void SetOrderCallback(std::function<void(const CtpOrderEvent &)> cb);

private:
    friend class CtpTraderSpiImpl;

    void HandleFrontConnected();
    void HandleFrontDisconnected(int reason);
    void HandleRspAuthenticate(CThostFtdcRspInfoField *pRspInfo, bool bIsLast);
    void HandleRspUserLogin(CThostFtdcRspUserLoginField *pRspLogin,
                            CThostFtdcRspInfoField *pRspInfo, bool bIsLast);
    void HandleRspSettlement(CThostFtdcRspInfoField *pRspInfo, bool bIsLast);
    void HandleRspError(CThostFtdcRspInfoField *pRspInfo);
    void HandleRspQryPosition(CThostFtdcInvestorPositionField *pPos,
                              CThostFtdcRspInfoField *pRspInfo, bool bIsLast);
    void HandleRtnOrder(CThostFtdcOrderField *pOrder);
    void HandleRtnTrade(CThostFtdcTradeField *pTrade);
    void HandleErrOrderInsert(CThostFtdcInputOrderField *pOrder, CThostFtdcRspInfoField *pRspInfo);
    void HandleErrOrderAction(CThostFtdcOrderActionField *pAction, CThostFtdcRspInfoField *pRspInfo);

    void DoLogin();
    void DoSettlementConfirm();
    void ApplyTradeToPosition(const std::string &instrument, char direction,
                              char offsetFlag, int volume);
    bool TodayVolumeEnough(const std::string &instrument, bool closingLong, int volume) const;
    void MaintenanceLoop();

    struct ActiveOrder {
        int frontId = 0;
        int sessionId = 0;
        std::string exchangeId;
        std::string orderSysId;
        std::string instrument;
        int volume = 0;
        int filled = 0;
        char status = '?';
    };

    struct PosInfo {
        int position = 0;
        int today = 0;
        int yd = 0;
        int frozen = 0;
    };

    CThostFtdcTraderApi *m_api = nullptr;
    CThostFtdcTraderSpi *m_spi = nullptr;

    std::string m_brokerID;
    std::string m_user;
    std::string m_password;
    std::string m_appID;
    std::string m_authCode;
    std::string m_productInfo;

    std::atomic<bool> m_connected{false};
    std::atomic<bool> m_ready{false};
    std::atomic<bool> m_stop{false};
    std::atomic<int> m_reqId{0};
    std::atomic<int> m_orderRefSeq{0};

    int m_frontId = 0;
    int m_sessionId = 0;

    mutable std::mutex m_mutex;                 // 保护 m_active / 持仓
    std::map<std::string, ActiveOrder> m_active;
    std::map<std::string, PosInfo> m_posLong;
    std::map<std::string, PosInfo> m_posShort;

    std::map<std::string, PosInfo> m_queryLong;
    std::map<std::string, PosInfo> m_queryShort;
    bool m_querying = false;

    std::function<void(const CtpTradeEvent &)> m_tradeCb;
    std::function<void(const CtpOrderEvent &)> m_orderCb;

    std::vector<std::string> m_fronts;
    std::thread *m_maintThread = nullptr;
};
