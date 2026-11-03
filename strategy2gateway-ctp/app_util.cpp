#include "app_util.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <ctime>

std::string TrimCopy(const std::string &s) {
    std::string t = s;
    while (!t.empty() && std::isspace((unsigned char)t.front())) t.erase(t.begin());
    while (!t.empty() && std::isspace((unsigned char)t.back())) t.pop_back();
    return t;
}

std::string UpperCopy(const std::string &s) {
    std::string t = s;
    std::transform(t.begin(), t.end(), t.begin(),
                   [](unsigned char c) { return (char)std::toupper(c); });
    return t;
}

std::string LowerCopy(const std::string &s) {
    std::string t = s;
    std::transform(t.begin(), t.end(), t.begin(),
                   [](unsigned char c) { return (char)std::tolower(c); });
    return t;
}

std::vector<std::string> SplitCsv(const std::string &line) {
    std::vector<std::string> out;
    std::string cur;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (c == ',') {
            out.push_back(TrimCopy(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(TrimCopy(cur));
    return out;
}

std::string ContractPrefix(const std::string &contract) {
    std::string p;
    for (size_t i = 0; i < contract.size(); ++i) {
        char c = contract[i];
        if (std::isalpha((unsigned char)c)) p.push_back(c);
        else break;
    }
    return p;
}

std::string ContinuousAlias(const std::string &ctpContract) {
    return LowerCopy(ContractPrefix(ctpContract));
}

std::string ContractAlias(const std::string &ctpContract) {
    std::string s = LowerCopy(ctpContract);
    if (s.empty()) return s;
    // 避免已带 s 的合约（如 ru2701s）再追加一次 s
    if (s.size() >= 2 && s.back() == 's' && std::isdigit((unsigned char)s[s.size() - 2])) {
        return s;
    }
    s.push_back('s');
    return s;
}

std::string FormatDouble(double v, int digits) {
    char buf[64];
    _snprintf(buf, sizeof(buf), "%.*f", digits, v);
    buf[sizeof(buf) - 1] = '\0';
    return std::string(buf);
}

long long NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

std::string NowStamp() {
    std::time_t t = std::time(nullptr);
    char tb[64];
    std::strftime(tb, sizeof(tb), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
    return std::string(tb);
}
