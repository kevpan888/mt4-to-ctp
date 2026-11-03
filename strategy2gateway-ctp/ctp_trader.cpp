#include "ctp_trader.h"
#include "app_util.h"

#include <algorithm>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <thread>

namespace {

template <size_t N>
void SetStr(char (&dst)[N], const std::string &src) {
    size_t n = src.size();
    if (n >= N) n = N - 1;
    if (n > 0) std::memcpy(dst, src.data(), n);
    dst[n] = '\0';
}

bool IsTerminalStatus(char s) {
    return s == THOST_FTDC_OST_AllTraded ||        // '0'
           s == THOST_FTDC_OST_PartTradedNotQueueing || // '2'
           s == '4' ||                             // NoTradeNotQueueing
           s == THOST_FTDC_OST_Canceled;           // '5'
}

std::string ErrText(CThostFtdcRspInfoField *p) {
    if (!p || p->ErrorID == 0) return std::string();
    char buf[160];
    _snprintf(buf, sizeof(buf), "ErrorID=%d ErrorMsg=%s",
              p->ErrorID, p->ErrorMsg);
    buf[sizeof(buf) - 1] = '\0';
    return std::string(buf);
}

} // namespace

class CtpTraderSpiImpl : public CThostFtdcTraderSpi {
public:
    explicit CtpTraderSpiImpl(CtpTrader *owner) : m_owner(owner) {}

    void OnFrontConnected() override { m_owner->HandleFrontConnected(); }
    void OnFrontDisconnected(int nReason) override { m_owner->HandleFrontDisconnected(nReason); }
    void OnHeartBeatWarning(int nTimeLapse) override {
        LogMessage("CTP trade heartbeat warning, lapse=" + std::to_string(nTimeLapse));
    }
    void OnRspAuthenticate(CThostFtdcRspAuthenticateField *, CThostFtdcRspInfoField *pRspInfo,
                           int, bool bIsLast) override {
        m_owner->HandleRspAuthenticate(pRspInfo, bIsLast);
    }
    void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspLogin,
                        CThostFtdcRspInfoField *pRspInfo, int, bool bIsLast) override {
        m_owner->HandleRspUserLogin(pRspLogin, pRspInfo, bIsLast);
    }
    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField *,
                                    CThostFtdcRspInfoField *pRspInfo, int, bool bIsLast) override {
        m_owner->HandleRspSettlement(pRspInfo, bIsLast);
    }
    void OnRspError(CThostFtdcRspInfoField *pRspInfo, int, bool) override {
        m_owner->HandleRspError(pRspInfo);
    }
    void OnRspQryInvestorPosition(CThostFtdcInvestorPositionField *pPos,
                                  CThostFtdcRspInfoField *pRspInfo, int, bool bIsLast) override {
        m_owner->HandleRspQryPosition(pPos, pRspInfo, bIsLast);
    }
    void OnRspOrderInsert(CThostFtdcInputOrderField *pOrder, CThostFtdcRspInfoField *pRspInfo,
                          int, bool bIsLast) override {
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            m_owner->HandleErrOrderInsert(pOrder, pRspInfo);
        }
    }
    void OnRspOrderAction(CThostFtdcInputOrderActionField *, CThostFtdcRspInfoField *pRspInfo,
                          int, bool bIsLast) override {
        if (pRspInfo && pRspInfo->ErrorID != 0) {
            LogMessage("CTP cancel rejected: " + ErrText(pRspInfo));
        }
    }
    void OnRtnOrder(CThostFtdcOrderField *pOrder) override { m_owner->HandleRtnOrder(pOrder); }
    void OnRtnTrade(CThostFtdcTradeField *pTrade) override { m_owner->HandleRtnTrade(pTrade); }
    void OnErrRtnOrderInsert(CThostFtdcInputOrderField *pOrder,
                             CThostFtdcRspInfoField *pRspInfo) override {
        m_owner->HandleErrOrderInsert(pOrder, pRspInfo);
    }
    void OnErrRtnOrderAction(CThostFtdcOrderActionField *pAction,
                             CThostFtdcRspInfoField *pRspInfo) override {
        m_owner->HandleErrOrderAction(pAction, pRspInfo);
    }

private:
    CtpTrader *m_owner;
};

CtpTrader::~CtpTrader() {
    Stop();
}

void CtpTrader::Configure(const std::string &brokerID, const std::string &user,
                          const std::string &password, const std::string &appID,
                          const std::string &authCode, const std::string &productInfo) {
    m_brokerID = brokerID;
    m_user = user;
    m_password = password;
    m_appID = appID;
    m_authCode = authCode;
    m_productInfo = productInfo;
}

bool CtpTrader::Start(const std::vector<std::string> &fronts) {
    if (fronts.empty()) {
        LogMessage("CTP trade: no tradeaddress configured, trading via CTP disabled");
        return false;
    }
    m_fronts = fronts;
    m_stop = false;
    m_api = CThostFtdcTraderApi::CreateFtdcTraderApi("");
    if (!m_api) {
        LogMessage("CTP trade: CreateFtdcTraderApi failed");
        return false;
    }
    m_spi = new CtpTraderSpiImpl(this);
    m_api->RegisterSpi(m_spi);
    m_api->SubscribePrivateTopic(THOST_TERT_QUICK);
    m_api->SubscribePublicTopic(THOST_TERT_QUICK);
    for (size_t i = 0; i < m_fronts.size(); ++i) {
        m_api->RegisterFront(const_cast<char *>(m_fronts[i].c_str()));
        LogMessage("CTP trade front: " + m_fronts[i]);
    }
    m_api->Init();
    m_maintThread = new std::thread(&CtpTrader::MaintenanceLoop, this);
    return true;
}

void CtpTrader::Stop() {
    m_stop = true;
    m_ready = false;
    if (m_api) {
        m_api->Release();
        m_api = nullptr;
    }
    if (m_maintThread) {
        m_maintThread->join();
        delete m_maintThread;
        m_maintThread = nullptr;
    }
    if (m_spi) {
        delete static_cast<CtpTraderSpiImpl *>(m_spi);
        m_spi = nullptr;
    }
}

bool CtpTrader::IsReady() const { return m_ready.load(); }
bool CtpTrader::IsConnected() const { return m_connected.load(); }

std::string CtpTrader::StatusText() const {
    if (m_ready.load()) return "ready";
    if (m_connected.load()) return "connecting";
    return "off";
}

std::string CtpTrader::NextOrderRef() {
    int v = ++m_orderRefSeq;
    char buf[16];
    _snprintf(buf, sizeof(buf), "%012d", v);
    buf[sizeof(buf) - 1] = '\0';
    return std::string(buf);
}

void CtpTrader::HandleFrontConnected() {
    m_connected = true;
    m_ready = false;
    LogMessage("CTP trade front connected, authenticating...");
    CThostFtdcReqAuthenticateField req;
    std::memset(&req, 0, sizeof(req));
    SetStr(req.BrokerID, m_brokerID);
    SetStr(req.UserID, m_user);
    SetStr(req.AppID, m_appID);
    SetStr(req.AuthCode, m_authCode);
    SetStr(req.UserProductInfo, m_productInfo);
    int ret = m_api->ReqAuthenticate(&req, ++m_reqId);
    if (ret != 0) {
        LogMessage("CTP ReqAuthenticate failed, ret=" + std::to_string(ret));
    }
}

void CtpTrader::HandleFrontDisconnected(int reason) {
    m_connected = false;
    m_ready = false;
    LogMessage("CTP trade front disconnected, reason=" + std::to_string(reason));
}

void CtpTrader::HandleRspAuthenticate(CThostFtdcRspInfoField *pRspInfo, bool bIsLast) {
    if (!bIsLast) return;
    std::string err = ErrText(pRspInfo);
    if (!err.empty()) {
        LogMessage("CTP authenticate failed: " + err);
        return;
    }
    DoLogin();
}

void CtpTrader::DoLogin() {
    CThostFtdcReqUserLoginField req;
    std::memset(&req, 0, sizeof(req));
    SetStr(req.BrokerID, m_brokerID);
    SetStr(req.UserID, m_user);
    SetStr(req.Password, m_password);
    SetStr(req.UserProductInfo, m_productInfo);
    int ret = m_api->ReqUserLogin(&req, ++m_reqId);
    LogMessage("CTP trade login request, ret=" + std::to_string(ret));
}

void CtpTrader::HandleRspUserLogin(CThostFtdcRspUserLoginField *pRspLogin,
                                   CThostFtdcRspInfoField *pRspInfo, bool bIsLast) {
    if (!bIsLast) return;
    std::string err = ErrText(pRspInfo);
    if (!err.empty()) {
        LogMessage("CTP trade login failed: " + err);
        return;
    }
    if (pRspLogin) {
        m_frontId = pRspLogin->FrontID;
        m_sessionId = pRspLogin->SessionID;
        int maxRef = std::atoi(pRspLogin->MaxOrderRef);
        if (maxRef > m_orderRefSeq.load()) m_orderRefSeq = maxRef;
        LogMessage("CTP trade login ok, FrontID=" + std::to_string(m_frontId) +
                   " SessionID=" + std::to_string(m_sessionId) +
                   " MaxOrderRef=" + std::string(pRspLogin->MaxOrderRef));
    }
    DoSettlementConfirm();
}

void CtpTrader::DoSettlementConfirm() {
    CThostFtdcSettlementInfoConfirmField req;
    std::memset(&req, 0, sizeof(req));
    SetStr(req.BrokerID, m_brokerID);
    SetStr(req.InvestorID, m_user);
    int ret = m_api->ReqSettlementInfoConfirm(&req, ++m_reqId);
    LogMessage("CTP settlement confirm request, ret=" + std::to_string(ret));
}

void CtpTrader::HandleRspSettlement(CThostFtdcRspInfoField *pRspInfo, bool bIsLast) {
    if (!bIsLast) return;
    std::string err = ErrText(pRspInfo);
    if (!err.empty()) {
        LogMessage("CTP settlement confirm failed: " + err);
        return;
    }
    m_ready = true;
    LogMessage("CTP trade ready");
    RefreshPositions();
}

void CtpTrader::HandleRspError(CThostFtdcRspInfoField *pRspInfo) {
    std::string err = ErrText(pRspInfo);
    if (!err.empty()) LogMessage("CTP trade error response: " + err);
}

void CtpTrader::RefreshPositions() {
    if (!m_api || !m_ready.load()) return;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (m_querying) return;
        m_querying = true;
        m_queryLong.clear();
        m_queryShort.clear();
    }
    CThostFtdcQryInvestorPositionField req;
    std::memset(&req, 0, sizeof(req));
    SetStr(req.BrokerID, m_brokerID);
    SetStr(req.InvestorID, m_user);
    int ret = m_api->ReqQryInvestorPosition(&req, ++m_reqId);
    if (ret != 0) {
        LogMessage("CTP position query failed, ret=" + std::to_string(ret));
        std::lock_guard<std::mutex> lk(m_mutex);
        m_querying = false;
    }
}

void CtpTrader::HandleRspQryPosition(CThostFtdcInvestorPositionField *pPos,
                                     CThostFtdcRspInfoField *pRspInfo, bool bIsLast) {
    std::string err = ErrText(pRspInfo);
    if (!err.empty()) {
        LogMessage("CTP position query error: " + err);
        std::lock_guard<std::mutex> lk(m_mutex);
        m_querying = false;
        return;
    }
    if (pPos) {
        std::lock_guard<std::mutex> lk(m_mutex);
        PosInfo &info = (pPos->PosiDirection == THOST_FTDC_PD_Long)
                            ? m_queryLong[pPos->InstrumentID]
                            : m_queryShort[pPos->InstrumentID];
        info.position = pPos->Position;
        info.today = pPos->TodayPosition;
        info.yd = pPos->YdPosition;
        info.frozen = (pPos->PosiDirection == THOST_FTDC_PD_Long)
                          ? pPos->LongFrozen : pPos->ShortFrozen;
        LogMessage(std::string("CTP pos record ") + pPos->InstrumentID +
                   " dir=" + std::string(1, pPos->PosiDirection) +
                   " pos=" + std::to_string(pPos->Position) +
                   " today=" + std::to_string(pPos->TodayPosition) +
                   " yd=" + std::to_string(pPos->YdPosition));
    }
    if (bIsLast) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_posLong.swap(m_queryLong);
        m_posShort.swap(m_queryShort);
        m_queryLong.clear();
        m_queryShort.clear();
        m_querying = false;
        int longCount = 0, longVolume = 0, shortCount = 0, shortVolume = 0;
        for (const auto &kv : m_posLong) {
            if (kv.second.position > 0) { ++longCount; longVolume += kv.second.position; }
        }
        for (const auto &kv : m_posShort) {
            if (kv.second.position > 0) { ++shortCount; shortVolume += kv.second.position; }
        }
        LogMessage("CTP positions refreshed, long=" + std::to_string(longCount) +
                   "(" + std::to_string(longVolume) + ") short=" +
                   std::to_string(shortCount) + "(" + std::to_string(shortVolume) + ")");
    }
}

bool CtpTrader::TodayVolumeEnough(const std::string &instrument, bool closingLong,
                                  int volume) const {
    std::lock_guard<std::mutex> lk(m_mutex);
    const std::map<std::string, PosInfo> &m = closingLong ? m_posLong : m_posShort;
    auto it = m.find(instrument);
    if (it == m.end()) return false;
    return it->second.today >= volume;
}

bool CtpTrader::InsertOrder(const std::string &ref, const std::string &instrument,
                            bool buy, bool openOffset, bool marketOrder, double price,
                            int volume, const std::string &comment, std::string &errMsg) {
    errMsg.clear();
    if (!m_ready.load() || !m_api) {
        errMsg = "ctp_not_ready";
        return false;
    }
    if (volume <= 0) {
        errMsg = "bad_volume";
        return false;
    }

    char offsetFlag = THOST_FTDC_OF_Open;
    if (!openOffset) {
        bool closingLong = !buy;  // 卖出平多头 / 买入平空头
        offsetFlag = TodayVolumeEnough(instrument, closingLong, volume)
                         ? THOST_FTDC_OF_CloseToday
                         : THOST_FTDC_OF_Close;
    }

    CThostFtdcInputOrderField req;
    std::memset(&req, 0, sizeof(req));
    SetStr(req.BrokerID, m_brokerID);
    SetStr(req.InvestorID, m_user);
    SetStr(req.OrderRef, ref);
    SetStr(req.InstrumentID, instrument);
    SetStr(req.BusinessUnit, comment.substr(0, 20));

    req.OrderPriceType = THOST_FTDC_OPT_LimitPrice;
    req.Direction = buy ? THOST_FTDC_D_Buy : THOST_FTDC_D_Sell;
    req.CombOffsetFlag[0] = offsetFlag;
    req.CombHedgeFlag[0] = THOST_FTDC_HF_Speculation;
    req.LimitPrice = price;
    req.VolumeTotalOriginal = volume;
    req.TimeCondition = marketOrder ? THOST_FTDC_TC_IOC : THOST_FTDC_TC_GFD;
    req.VolumeCondition = THOST_FTDC_VC_AV;
    req.MinVolume = 1;
    req.ContingentCondition = THOST_FTDC_CC_Immediately;
    req.ForceCloseReason = THOST_FTDC_FCC_NotForceClose;
    req.IsAutoSuspend = 0;
    req.UserForceClose = 0;

    int ret = m_api->ReqOrderInsert(&req, ++m_reqId);
    if (ret != 0) {
        errMsg = "req_insert_ret_" + std::to_string(ret);
        return false;
    }

    ActiveOrder ao;
    ao.frontId = m_frontId;
    ao.sessionId = m_sessionId;
    ao.instrument = instrument;
    ao.volume = volume;
    ao.status = '?';
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_active[ref] = ao;
    }
    LogMessage("CTP insert ref=" + ref + " " + (buy ? "BUY" : "SELL") +
               (openOffset ? " OPEN " : " CLOSE ") + instrument + " vol=" +
               std::to_string(volume) + " px=" + FormatDouble(price, 4) +
               " " + (marketOrder ? "IOC" : "GFD"));
    return true;
}

int CtpTrader::CancelAll(const std::string &instrumentFilter, std::string &errMsg) {
    errMsg.clear();
    if (!m_ready.load() || !m_api) {
        errMsg = "ctp_not_ready";
        return 0;
    }
    std::string filter = TrimCopy(instrumentFilter);
    bool all = filter.empty() || filter == "*";

    std::vector<std::pair<std::string, ActiveOrder>> targets;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        for (auto &kv : m_active) {
            const ActiveOrder &ao = kv.second;
            if (IsTerminalStatus(ao.status)) continue;
            if (!all && ao.instrument != filter) continue;
            targets.push_back(std::make_pair(kv.first, ao));
        }
    }
    if (targets.empty()) return 0;

    int sent = 0;
    for (size_t i = 0; i < targets.size(); ++i) {
        const std::string &ref = targets[i].first;
        ActiveOrder ao = targets[i].second;

        // 刚报出的单可能还没收到交易所确认；等待 OrderSysID 后再撤，
        // 否则只能用 OrderRef 撤销，部分交易所会报 148（ExchangeID 无效）
        if (ao.orderSysId.empty()) {
            for (int k = 0; k < 10 && ao.orderSysId.empty(); ++k) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                std::lock_guard<std::mutex> lk(m_mutex);
                auto it = m_active.find(ref);
                if (it == m_active.end()) break;
                ao = it->second;
            }
        }
        if (IsTerminalStatus(ao.status)) continue;  // 等待期间已成交/已撤
        if (ao.orderSysId.empty()) {
            LogMessage("CTP cancel deferred ref=" + ref + " (no OrderSysID)");
            continue;
        }

        CThostFtdcInputOrderActionField req;
        std::memset(&req, 0, sizeof(req));
        SetStr(req.BrokerID, m_brokerID);
        SetStr(req.InvestorID, m_user);
        req.FrontID = ao.frontId;
        req.SessionID = ao.sessionId;
        SetStr(req.OrderRef, ref);
        // CTP 规定：OrderSysID 为空时 ExchangeID 也必须为空，否则报 148
        if (!ao.orderSysId.empty()) {
            SetStr(req.ExchangeID, ao.exchangeId);
            SetStr(req.OrderSysID, ao.orderSysId);
        }
        req.ActionFlag = THOST_FTDC_AF_Delete;
        int ret = m_api->ReqOrderAction(&req, ++m_reqId);
        if (ret == 0) ++sent;
        else LogMessage("CTP cancel ref=" + ref + " failed, ret=" + std::to_string(ret));
    }
    LogMessage("CTP cancel requests sent=" + std::to_string(sent) +
               "/" + std::to_string(targets.size()) +
               " filter=" + (all ? "*" : filter));
    return sent;
}

void CtpTrader::HandleRtnOrder(CThostFtdcOrderField *pOrder) {
    if (!pOrder) return;
    std::string ref = pOrder->OrderRef;
    LogMessage("CTP rtn order ref=" + ref + " status=" +
               std::string(1, pOrder->OrderStatus) + " vol=" +
               std::to_string(pOrder->VolumeTotalOriginal) + " traded=" +
               std::to_string(pOrder->VolumeTraded) + " exch=" +
               std::string(pOrder->ExchangeID) + " sys=" +
               std::string(pOrder->OrderSysID) + " msg=" +
               std::string(pOrder->StatusMsg));

    CtpOrderEvent ev;
    ev.ref = ref;
    ev.status = pOrder->OrderStatus;
    ev.volume = pOrder->VolumeTotalOriginal;
    ev.filled = pOrder->VolumeTraded;
    ev.msg = pOrder->StatusMsg;

    bool known = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_active.find(ref);
        if (it != m_active.end()) {
            ActiveOrder &ao = it->second;
            ao.frontId = pOrder->FrontID;
            ao.sessionId = pOrder->SessionID;
            ao.exchangeId = pOrder->ExchangeID;
            ao.orderSysId = pOrder->OrderSysID;
            ao.status = pOrder->OrderStatus;
            ao.filled = pOrder->VolumeTraded;
            ao.volume = pOrder->VolumeTotalOriginal;
            known = true;
            if (IsTerminalStatus(ao.status)) {
                // 保留终态记录供撤单表遍历，实际不会再次撤单
            }
        }
    }
    if (known && m_orderCb) m_orderCb(ev);
}

void CtpTrader::HandleRtnTrade(CThostFtdcTradeField *pTrade) {
    if (!pTrade) return;
    char offset = pTrade->OffsetFlag;

    CtpTradeEvent ev;
    ev.ref = pTrade->OrderRef;
    ev.instrument = pTrade->InstrumentID;
    ev.buy = (pTrade->Direction == THOST_FTDC_D_Buy);
    ev.open = (offset == THOST_FTDC_OF_Open);
    ev.price = pTrade->Price;
    ev.volume = pTrade->Volume;

    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_active.find(ev.ref);
        if (it != m_active.end()) {
            it->second.filled += pTrade->Volume;
        }
    }
    ApplyTradeToPosition(ev.instrument, pTrade->Direction, offset, pTrade->Volume);

    LogMessage("CTP trade ref=" + ev.ref + " " + (ev.buy ? "BUY" : "SELL") +
               (ev.open ? " OPEN " : " CLOSE ") + ev.instrument +
               " px=" + FormatDouble(ev.price, 4) + " vol=" + std::to_string(ev.volume));
    if (m_tradeCb) m_tradeCb(ev);
}

void CtpTrader::ApplyTradeToPosition(const std::string &instrument, char direction,
                                     char offsetFlag, int volume) {
    std::lock_guard<std::mutex> lk(m_mutex);
    // 开仓按委托方向定多空；平仓方向相反（卖平=平多，买平=平空）
    bool isLong = (offsetFlag == THOST_FTDC_OF_Open)
                      ? (direction == THOST_FTDC_D_Buy)
                      : (direction == THOST_FTDC_D_Sell);
    PosInfo &info = isLong ? m_posLong[instrument] : m_posShort[instrument];
    if (offsetFlag == THOST_FTDC_OF_Open) {
        info.position += volume;
        info.today += volume;
    } else if (offsetFlag == THOST_FTDC_OF_CloseToday) {
        info.position = std::max(0, info.position - volume);
        info.today = std::max(0, info.today - volume);
    } else {
        info.position = std::max(0, info.position - volume);
        info.yd = std::max(0, info.yd - volume);
    }
}

void CtpTrader::HandleErrOrderInsert(CThostFtdcInputOrderField *pOrder,
                                     CThostFtdcRspInfoField *pRspInfo) {
    std::string ref = pOrder ? pOrder->OrderRef : std::string();
    LogMessage("CTP order insert error ref=" + ref + " " + ErrText(pRspInfo));
    std::lock_guard<std::mutex> lk(m_mutex);
    m_active.erase(ref);
}

void CtpTrader::HandleErrOrderAction(CThostFtdcOrderActionField *pAction,
                                     CThostFtdcRspInfoField *pRspInfo) {
    std::string ref = pAction ? pAction->OrderRef : std::string();
    LogMessage("CTP cancel error ref=" + ref + " " + ErrText(pRspInfo));
}

void CtpTrader::SetTradeCallback(std::function<void(const CtpTradeEvent &)> cb) {
    m_tradeCb = cb;
}

void CtpTrader::SetOrderCallback(std::function<void(const CtpOrderEvent &)> cb) {
    m_orderCb = cb;
}

void CtpTrader::MaintenanceLoop() {
    while (!m_stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        static int tick = 0;
        if (++tick >= 60) {  // 约 30 秒
            tick = 0;
            if (m_ready.load()) RefreshPositions();
        }
    }
}
