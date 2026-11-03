//+------------------------------------------------------------------+
//|                                                     综合指标.mq4 |
//|                                                                  |
//|                                                                  |
//+------------------------------------------------------------------+
#property copyright ""
#property link      ""
#property indicator_chart_window
#property indicator_buffers 4
#property indicator_color1 DarkBlue
#property indicator_color2 MediumBlue
#property indicator_color3 Blue
#property indicator_color4 DodgerBlue

//---- input parameters
extern string USERNAME="waihui";
extern string PASSWORD="waihui123";
extern double val;

//---- buffers

double new0[];

double new1[];
double new2[];
double new3[];



//+------------------------------------------------------------------+
//| Custom indicator initialization function                         |
//+------------------------------------------------------------------+
int init()
  {
//---- indicators
   IndicatorBuffers(4);
   SetIndexStyle(0,DRAW_LINE,1,100);
   SetIndexBuffer(0,new0);
   SetIndexStyle(1,DRAW_LINE,1,100);
   SetIndexBuffer(1,new1);
   SetIndexStyle(2,DRAW_LINE,1,100);
   SetIndexBuffer(2,new2);
   
    SetIndexStyle(3,DRAW_LINE,1,100);
   SetIndexBuffer(3,new3);

   
//----
   return(0);
  }
//+------------------------------------------------------------------+
//| Custom indicator deinitialization function                       |
//+------------------------------------------------------------------+
int deinit()
  {
//----
   
//----
   return(0);
  }
//+------------------------------------------------------------------+
//| Custom indicator iteration function                              |
//+------------------------------------------------------------------+
int start()
  {
   if((Year()==2010&&Month()>5)||Year()>2010) 
   {
    Alert("已超过授权日期");
     return(0);
   }
   
   if(USERNAME!="waihui") 
   {
     Alert("用户名错误");
     return(0);
   }
   
   if(PASSWORD!="waihui123") 
   {
     Alert("密码错误");
     return(0);
   }
   int timezone,localhour;
   if(TimeHour(TimeLocal())>TimeHour(TimeCurrent()))
   {
       timezone=TimeHour(TimeLocal())-TimeHour(TimeCurrent());
   }
   else
   {
       timezone=24+TimeHour(TimeLocal())-TimeHour(TimeCurrent());
   }
   int i,counted_bars=IndicatorCounted();
   if(counted_bars>0) counted_bars--;
   int limit=Bars-counted_bars;
   for(i=limit-1; i>=0; i--)
   {
      new0[i]=EMPTY_VALUE;
      new1[i]=EMPTY_VALUE;
      new2[i]=EMPTY_VALUE;
      new3[i]=EMPTY_VALUE;
      
      if(TimeHour(Time[i])+timezone<24)
      {
         localhour=TimeHour(Time[i])+timezone;
      }
      else
      {
         localhour=TimeHour(Time[i])+timezone-24;
      }
       if(localhour>=14&&localhour<=23)
      {  
          new0[i]=val;
      }
      else
      {
          new3[i]=val;
      }
      
  
      //double val=iMA(NULL,0,3,0,MODE_SMA,PRICE_CLOSE,i);
    /*
      if(TimeHour(Time[i])>=6&&TimeHour(Time[i])<12)
      {  
          new0[i]=val;
      }
       if(TimeHour(Time[i])>=12&&TimeHour(Time[i])<18)
      {  
          new1[i]=val;
      }
       if(TimeHour(Time[i])>=18&&TimeHour(Time[i])<24)
      {  
          new2[i]=val;
      }
       if(TimeHour(Time[i])>=24||TimeHour(Time[i])<6)
      {  
          new3[i]=val;
      }
     */
   }  
      
    
    
//----
   
   return(0);
  }