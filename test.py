#%%
from tvDatafeed import TvDatafeed,Interval
import requests
import telegram
import os
import datetime
import logging
import pandas as pd
import numpy as np
import math as ma
import matplotlib.pyplot as plt
import mplfinance as mpf
logging.basicConfig(level=logging.DEBUG)
bull_out=pd.DataFrame()
bear_out=pd.DataFrame()
#%%
#For telegram msg
bot_token = '1964493702:AAF6PTyxnuu9Akrjc3MEwKimcWtORtXzufo'
group_id = '@VolSR_python'
bot = telegram.Bot(token=bot_token)
#%%
### GET TRADING VIEW DATA USING TvDatafeed
#_stock_name,_exchange,lookback,_tresholdvalue='L_TFH','NSE',9,5

def Vol_SR(_stock_name,_exchange,_tresholdvalue=5,lookback=9):
    OHLC_data=TvDatafeed().get_hist(_stock_name,_exchange,Interval.in_daily,n_bars=3000)
    # getting only data between 9:15 - 15:30
    OHLC_data = OHLC_data[
        (OHLC_data.index.date > OHLC_data.index.date[0])
        & (OHLC_data.index.time >= datetime.time(9, 15))
        & (OHLC_data.index.time <= datetime.time(15, 30))
    ]
    resampling_dict = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }

    OHLC_data = OHLC_data.resample("D").apply(resampling_dict).dropna()
    OHLC_data.insert(0,"symbol",_stock_name)
    _latestrow=OHLC_data.tail(1)

    # Calculate Volume change
    OHLC_data["VolChange"]=(OHLC_data["volume"]/OHLC_data["volume"].shift(1))-1
    OHLC_data["stdev"]=OHLC_data["VolChange"].rolling(lookback).std()
    OHLC_data["signal"]=abs(OHLC_data["VolChange"]/OHLC_data["stdev"].shift(1))
    OHLC_data["levelhi"]=np.where(OHLC_data["signal"]>_tresholdvalue,OHLC_data["high"].shift(1),0)
    OHLC_data["levello"]=np.where(OHLC_data["signal"]>_tresholdvalue,OHLC_data["low"].shift(1),0)
    OHLC_data=OHLC_data[['levelhi','levello'] ]
    OHLC_data=OHLC_data[OHLC_data["levelhi"]>1].tail(3)
    OHLC_data["Occurance"]=np.arange(len(OHLC_data),0,-1)
    OHLC_data =(pd.concat([_latestrow,OHLC_data],axis=1).fillna(method='bfill'))
    OHLC_data.reset_index(level=0, inplace=True)
    #%%
    up=OHLC_data[(OHLC_data["Occurance"]==1)&(OHLC_data["high"]>OHLC_data["levelhi"])&(OHLC_data["low"]<=OHLC_data["levelhi"])&(OHLC_data["close"]>OHLC_data["levelhi"])]
    down=OHLC_data[(OHLC_data["Occurance"]==1)&(OHLC_data["high"]>=OHLC_data["levello"])&(OHLC_data["low"]<OHLC_data["levello"])&(OHLC_data["close"]<OHLC_data["levello"])]
    #%%
    global bull_out
    global bear_out
    bull_out=bull_out.append(up,ignore_index=True).drop_duplicates(subset=['symbol'])   
    bear_out=bear_out.append(down,ignore_index=True)
    return bull_out,bear_out

#%%
#Vol_SR('L_TFH','NSE')
#StockList=['BHARATFORG','BANDHANBNK','BAJAJ_AUTO','NAM_INDIA','AARTIIND','LUPIN','UPL','BERGEPAINT','ICICIPRULI','ACC','FEDERALBNK','GRANULES']
StockList=['AARTIIND','ABBOTINDIA','ABFRL','ACC','ADANIENT','ADANIPORTS','ALKEM','AMARAJABAT','AMBUJACEM','APLLTD','APOLLOHOSP','APOLLOTYRE','ASHOKLEY','ASIANPAINT','ASTRAL','AUBANK','AUROPHARMA','AXISBANK','BAJAJ_AUTO','BAJAJFINSV','BAJFINANCE','BALKRISIND','BANDHANBNK','BANKBARODA','BATAINDIA','BEL','BERGEPAINT','BHARATFORG','BHARTIARTL','BHEL','BIOCON','BOSCHLTD','BPCL','BRITANNIA','CADILAHC','CANBK','CANFINHOME','CHOLAFIN','CIPLA','COALINDIA','COFORGE','COLPAL','CONCOR','COROMANDEL','CROMPTON','CUB','CUMMINSIND','DABUR','DALBHARAT','DEEPAKNTR','DELTACORP','DIVISLAB','DIXON','DLF','DRREDDY','EICHERMOT','ESCORTS','EXIDEIND','FEDERALBNK','GAIL','GLENMARK','GMRINFRA','GODREJCP','GODREJPROP','GRANULES','GRASIM','GUJGASLTD','HAL','HAVELLS','HCLTECH','HDFC','HDFCAMC','HDFCBANK','HDFCLIFE','HEROMOTOCO','HINDALCO','HINDPETRO','HINDUNILVR','IBULHSGFIN','ICICIBANK','ICICIGI','ICICIPRULI','IDEA','IDFCFIRSTB','IEX','IGL','INDHOTEL','INDIACEM','INDIAMART','INDIGO','INDUSINDBK','INDUSTOWER','INFY','IOC','IPCALAB','IRCTC','ITC','JINDALSTEL','JKCEMENT','JSWSTEEL','JUBLFOOD','KOTAKBANK','L_TFH','LALPATHLAB','LICHSGFIN','LT','LTI','LTTS','LUPIN','M_M','M_MFIN','MANAPPURAM','MARICO','MARUTI','MCDOWELL_N','MCX','METROPOLIS','MFSL','MGL','MINDTREE','MOTHERSUMI','MPHASIS','MRF','MUTHOOTFIN','NAM_INDIA','NATIONALUM','NAUKRI','NAVINFLUOR','NESTLEIND','NMDC','NTPC','OBEROIRLTY','OFSS','ONGC','PAGEIND','PEL','PERSISTENT','PETRONET','PFC','PFIZER','PIDILITIND','PIIND','PNB','POLYCAB','POWERGRID','PVR','RAMCOCEM','RBLBANK','RECLTD','RELIANCE','SAIL','SBILIFE','SBIN','SHREECEM','SIEMENS','SRF','SRTRANSFIN','STAR','SUNPHARMA','SUNTV','SYNGENE','TATACHEM','TATACONSUM','TATAMOTORS','TATAPOWER','TATASTEEL','TCS','TECHM','TITAN','TORNTPHARM','TORNTPOWER','TRENT','TVSMOTOR','UBL','ULTRACEMCO','UPL','VEDL','VOLTAS','WIPRO','ZEEL']


for i in range(len(StockList)):
    Vol_SR(StockList[i],'NSE')
    #print(StockList[i])
## Copy the screened stocks to csv and post to telegram
if len(bull_out) and os.path.isfile(f'bullish_stocks_{datetime.date.today()}.csv'):
    print ('The File ',os.path.basename(f'bullish_stocks_{datetime.date.today()}.csv'),' already existed')
    earlier_screened_stocks = pd.read_csv(f'bullish_stocks_{datetime.date.today()}.csv').drop_duplicates(subset=['symbol'])
    earlier_screened_stocks_list = list(earlier_screened_stocks['symbol'])
    new_screened_stocks = bull_out[bull_out.symbol.isin(earlier_screened_stocks.symbol)==False] #S-P example:-S[S.email.isin(P.email) == False]
    new_screened_stocks.to_csv(f'bullish_stocks_{datetime.date.today()}.csv',header=False,mode='a',index=False)    
    new_screened_stocks_list = list(new_screened_stocks['symbol'])
    print(new_screened_stocks_list)
    if len(new_screened_stocks_list):
        bot.send_message(chat_id=group_id,text='#######VolSR BUY STOCK #######')
        for stock in new_screened_stocks_list:
            bot.send_message(chat_id=group_id,text=f'{stock} :: Screened at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
else:
    if len(bull_out):
        print('bull stock found')
        _ScreenedStocks=pd.concat([bull_out],axis=0).drop_duplicates(subset=['symbol'])
        _ScreenedStocks.to_csv(f'bullish_stocks_{datetime.date.today()}.csv',index=False)
        bot.send_message(chat_id=group_id,text='#####VolSR BUY STOCK ######')
        for stock in _ScreenedStocks['symbol']:
             bot.send_message(chat_id=group_id,text=f'{stock} :: Screened at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')

if len(bear_out) and os.path.isfile(f'bearish_stocks_{datetime.date.today()}.csv'):
    print ('The File ',os.path.basename(f'bearish_stocks_{datetime.date.today()}.csv'),' already existed')
    earlier_screened_stocks = pd.read_csv(f'bearish_stocks_{datetime.date.today()}.csv').drop_duplicates(subset=['symbol'])
    earlier_screened_stocks_list = list(earlier_screened_stocks['symbol'])
    new_screened_stocks = bear_out[bear_out.symbol.isin(earlier_screened_stocks.symbol)==False]#S-P example:-S[S.email.isin(P.email) == False]
    new_screened_stocks.to_csv(f'bearish_stocks_{datetime.date.today()}.csv',header=False,mode='a',index=False)
    new_screened_stocks_list = list(new_screened_stocks['symbol'])
    print(new_screened_stocks_list)
    if len(new_screened_stocks_list):
        bot.send_message(chat_id=group_id,text='#######VolSR SELL STOCK #######')
        for stock in new_screened_stocks_list:
            bot.send_message(chat_id=group_id,text=f'{stock} :: Screened at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')
else:
    if len(bear_out):
        print('bull stock found')
        _ScreenedStocks=pd.concat([bear_out],axis=0).drop_duplicates(subset=['symbol'])
        _ScreenedStocks.to_csv(f'bearish_stocks_{datetime.date.today()}.csv',index=False)
        bot.send_message(chat_id=group_id,text='#####VolSR SELL STOCK ######')
        for stock in _ScreenedStocks['symbol']:
             bot.send_message(chat_id=group_id,text=f'{stock} :: Screened at {datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")}')


#%%
   

#%%
#bull_out.to_csv(r'C:\Users\Ragendth\Desktop\bull_output.csv', index = None,mode='a', header=True,sep=',',encoding='utf-8')
#elbear_out.to_csv(r'C:\Users\Ragendth\Desktop\bear_output.csv', index = None,mode='a', header=True,sep=',',encoding='utf-8')
