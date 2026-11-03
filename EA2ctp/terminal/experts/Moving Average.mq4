//+------------------------------------------------------------------+
//|                                               Moving Average.mq4 |
//|                      Copyright © 2005, MetaQuotes Software Corp. |
//|                                       http://www.metaquotes.net/ |
//+------------------------------------------------------------------+
//|  EA2ctp 改造版（兼容老版 MT4 build 225）：                         |
//|  - 注释掉所有 OrderSend/OrderClose 本地下单                       |
//|  - 每次产生决策写一行 CSV 到 experts/files/ctp_signals.csv        |
//|  - build 225 无 DoubleToString/IntegerToString：全部字符串拼接    |
//|    靠 MQL4 隐式转换把 double/int 转成 string                      |
//|  - break 只能在 for 循环内使用                                   |
//+------------------------------------------------------------------+
#define MAGICMA  20050610
#define SIGNAL_FILE "ctp_signals.csv"

extern double Lots               = 1;
extern double BidAskDif          = 5;
extern double MovingPeriod       = 12;
extern double MovingShift        = 6;

//+------------------------------------------------------------------+
//| 老版 MQL4 build 225：字符串 + 数值 → 隐式自动转 string           |
//| FileOpen 只认 FILE_CSV|FILE_WRITE                               |
//| FileWrite 写一行完整字符串                                       |
//+------------------------------------------------------------------+
void WriteSignal(string action, string sym, string type, string offset,
                 double lots, double bidAskDif, string comment)
  {
   int handle = FileOpen(SIGNAL_FILE, FILE_CSV|FILE_WRITE);
   if(handle < 0) { Print("WriteSignal FAILED: ", GetLastError()); return; }
   // 隐式类型转换：double/int + string 自动转成 string
   string line = action + "," + sym + "," + type + "," + offset + ","
                 + lots + "," + bidAskDif + "," + comment;
   FileWrite(handle, line);
   FileFlush(handle);
   FileClose(handle);
   Print("[EA2CTP] SIGNAL: ", line);
  }

//+------------------------------------------------------------------+
int CalculateCurrentOrders(string symbol)
  {
   int buys=0,sells=0;
   for(int i=0;i<OrdersTotal();i++)
     {
      if(OrderSelect(i,SELECT_BY_POS,MODE_TRADES)==false) break;
      if(OrderSymbol()==Symbol() && OrderMagicNumber()==MAGICMA)
        {
         if(OrderType()==OP_BUY)  buys++;
         if(OrderType()==OP_SELL) sells++;
        }
     }
   if(buys>0) return(buys);
   else       return(-sells);
  }

//+------------------------------------------------------------------+
void CheckForOpen()
  {
   double ma;
   if(Volume[0]>1) return;
   ma=iMA(NULL,0,MovingPeriod,MovingShift,MODE_SMA,PRICE_CLOSE,0);
   if(Open[1]>ma && Close[1]<ma)
     {
      Print("[EA2CTP] MA death cross -> SELL OPEN");
      WriteSignal("SELL", Symbol(), "M", "OPEN", Lots, BidAskDif, "ma_death");
      return;
     }
   if(Open[1]<ma && Close[1]>ma)
     {
      Print("[EA2CTP] MA golden cross -> BUY OPEN");
      WriteSignal("BUY", Symbol(), "M", "OPEN", Lots, BidAskDif, "ma_golden");
      return;
     }
  }

//+------------------------------------------------------------------+
void CheckForClose()
  {
   double ma;
   if(Volume[0]>1) return;
   ma=iMA(NULL,0,MovingPeriod,MovingShift,MODE_SMA,PRICE_CLOSE,0);
   for(int i=0;i<OrdersTotal();i++)
     {
      if(OrderSelect(i,SELECT_BY_POS,MODE_TRADES)==false) break;
      if(OrderMagicNumber()!=MAGICMA || OrderSymbol()!=Symbol()) continue;
      if(OrderType()==OP_BUY)
        {
         if(Open[1]>ma && Close[1]<ma)
           {
            Print("[EA2CTP] MA death cross -> SELL CLOSE");
            WriteSignal("SELL", Symbol(), "M", "CLOSE", OrderLots(), BidAskDif, "ma_death_close");
           }
         break;
        }
      if(OrderType()==OP_SELL)
        {
         if(Open[1]<ma && Close[1]>ma)
           {
            Print("[EA2CTP] MA golden cross -> BUY CLOSE");
            WriteSignal("BUY", Symbol(), "M", "CLOSE", OrderLots(), BidAskDif, "ma_golden_close");
           }
         break;
        }
     }
  }

//+------------------------------------------------------------------+
void start()
  {
   if(Bars<100 || IsTradeAllowed()==false) return;
   if(CalculateCurrentOrders(Symbol())==0) CheckForOpen();
   else                                    CheckForClose();
  }
//+------------------------------------------------------------------+
