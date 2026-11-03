#pragma once
#include <string>
#include <vector>

// 由 simple_ctp_console.cpp 提供的全局日志（同时写控制台与 feedserver.log）
void LogMessage(const std::string &msg);

std::string TrimCopy(const std::string &s);
std::string UpperCopy(const std::string &s);
std::string LowerCopy(const std::string &s);

// 按逗号拆分并去除首尾空白（保留空字段）
std::vector<std::string> SplitCsv(const std::string &line);

// 取合约的字母前缀：ru2701 -> ru，IF2512 -> IF
std::string ContractPrefix(const std::string &contract);

// 品种别名规则：
//   连续主力 = 品种代码小写（ru2701 -> ru，IF2512 -> if）
//   具体合约 = 合约代码小写 + s（ru2701 -> ru2701s，IF2512 -> if2512s）
std::string ContinuousAlias(const std::string &ctpContract);
std::string ContractAlias(const std::string &ctpContract);

// 固定小数位格式化（用于 JSON / 日志）
std::string FormatDouble(double v, int digits);

long long NowMs();
std::string NowStamp();
