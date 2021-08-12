# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import os
from dotenv import load_dotenv
import sqlalchemy
import pymysql
import ta
import pandas as pd
import numpy as np
import yfinance as yf
pymysql.install_as_MySQLdb()
import smtplib
from pretty_html_table import build_table
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import datetime as dt

import pandas_market_calendars as mcal
import plotly.express as px


# %%
class Recommender:
    def __init__(self,name):
        self.name = name
        self.conn = self.getDbConn()
        
    def getDbConn(self):
        load_dotenv()
        DB_ACCESS_KEY = os.getenv("DB_ACCESS_KEY")
        engine =            sqlalchemy.create_engine(DB_ACCESS_KEY+'/'                       +self.name)
        return engine.connect()

    def gettables(self):
        query = f"""SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{self.name}'"""
        df = pd.read_sql(query, self.conn)
        df['Schema'] = self.name
        return df

    def maxdate(self):
        req = self.name+'.'+f'`{self.gettables().TABLE_NAME[0]}`'
        return pd.read_sql(f"SELECT MAX(Date) FROM {req}",self.conn)

    def updateDB(self):
        maxdate=self.maxdate()['MAX(Date)'][0]
        print('DB MaxDate =', maxdate)
        for symbol in self.gettables().TABLE_NAME:
            data = yf.download(symbol, start=maxdate)
            data = data[data.index > maxdate]
            data = data.reset_index()
            data.to_sql(symbol, self.conn, if_exists='append')
        print(f'{self.name} successfully updated')   

    def MACDdecision(self,df):
        df['MACD_diff'] = ta.trend.macd_diff(df.Close)
        df['Decision MACD'] = np.where((df.MACD_diff > 0) & (df.MACD_diff.shift(1) < 1), 
                                    True, False)

    def Goldencrossdecision(self,df):
        df['SMA20'] = ta.trend.sma_indicator(df.Close, window=20)
        df['SMA50'] = ta.trend.sma_indicator(df.Close, window=50)
        df['Signal'] = np.where(df['SMA20'] > df['SMA50'], True, False)
        df['Decision GC'] = df.Signal.diff()

    def RSI_SMAdecision(self,df):
        df['RSI'] = ta.momentum.rsi(df.Close, window=10)
        df['SMA200'] = ta.trend.sma_indicator(df.Close, window=200)
        df['Decision RSI/SMA'] = np.where((df.Close > df.SMA200) & (df.RSI < 30),
                                            True, False)
    def getprices(self):
        prices = []
        for table, schema in zip(self.gettables().TABLE_NAME, self.gettables().Schema):
            req = schema+'.'+f'`{table}`'
            prices.append(pd.read_sql(f"SELECT Date, Close FROM {req}",self.conn))
        return prices

    def applytechnicals(self):
        prices = self.getprices()
        for frame in prices:
            self.MACDdecision(frame)
            self.Goldencrossdecision(frame)
            self.RSI_SMAdecision(frame)
        return prices 

    def recommend(self):
        indicators = ['Decision MACD','Decision GC','Decision RSI/SMA']
        sigColumns=['Name','Symbol','Decision MACD','Decision GC','Decision RSI/SMA']
        dfSignals=pd.DataFrame(columns=sigColumns)
        for symbol, frame in zip(self.gettables().TABLE_NAME,self.applytechnicals()):
            if frame.empty is False:
                macd, gc, rsi, sig ='', '', '', ''
                for indicator in indicators:
                    if frame[indicator].iloc[-1] == True: # only chk today's result in the last row
                        if 'Decision MACD' == indicator:
                            macd, sig = 'X', 'MACD'
                        if 'Decision GC' == indicator:
                            gc, sig = 'X', 'GC' 
                        if 'Decision RSI/SMA' == indicator:
                            rsi, sig = 'X', 'RSI'
                if sig != '':
                    dfSignals = dfSignals.append(
                        {
                        'Name': self.name,
                        'Symbol' : symbol.upper(),
                        'Decision MACD' : macd,
                        'Decision GC' : gc,
                        'Decision RSI/SMA' : rsi,
                        'Signal' : sig
                        },ignore_index=True
                    )
        return dfSignals.set_index('Name')


# %%
class CreateEmails:
    smtp_server ='smtp.gmail.com'
    port = 587
    def __init__(self, name, dfSignals, pdf):
        self.name = name
        self.dfSignals =dfSignals
        self.pdf = pdf
        self.sender = os.getenv("SENDER_EMAIL")
        self.receivers = os.getenv("RECEIVER_EMAILS")
        self.password = os.getenv("PASSWORD")
        
        
    def sendEmails(self):    
        message = MIMEMultipart()
        message['Subject'] = f'{self.name} buying signals report'
        message['From'] = self.sender
        message['To'] = self.receivers
        now = dt.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        header = f'<h2>{self.name} buying signals report created at {now}</h2>'
        body = build_table(self.dfSignals, "green_light",text_align ="center")
        footer = f'<h2>Good Luck!!!</h2>'
        
        img = MIMEImage(self.pdf, "pdf" )
        img.add_header('Content-Disposition', 'attachment', filename=self.name+".pdf")
        body_content = header + body + footer
        message.attach(MIMEText(body_content, "html"))
        message.attach(img)
        msg_body = message.as_string()
        server =smtplib.SMTP(self.smtp_server, self.port)
        server.starttls()
        server.login(self.sender,self.password)
        server.sendmail(self.sender,self.receivers,msg_body)
        #
        server.quit()


# %%
class DbQouteData:
    def __init__(self, name, tickers_lst, days):
        self.name = name
        self.tickers_lst = tickers_lst
        self.days = days
        self.conn = self.getDbConn()
                
    def getDbConn(self):
        load_dotenv()
        DB_ACCESS_KEY = os.getenv("DB_ACCESS_KEY")
        engine =            sqlalchemy.create_engine(DB_ACCESS_KEY+'/'                       +self.name)
        return engine.connect()
    
    # Step1: Calcualte the start and end dates based on the input days from the valid NYSC calendar
    def get_start_end_dates(self):
    # get the last valid NYSE bus dates for the last 40 calendar dates using market calendar
        nyse = mcal.get_calendar('NYSE')
        schedule_nyse = nyse.schedule(
            (dt.datetime.today()-dt.timedelta(40)).strftime("%Y-%m-%d"),
            dt.datetime.today().strftime("%Y-%m-%d"))
    #check today's market closed or not
        if dt.datetime.now(dt.timezone.utc).hour >= schedule_nyse.market_close[-1].hour:
            market_closed_indicator = 0 # now is after 4PM ET- market closed-- we have today's data
        else:
            market_closed_indicator = 1  # else yesterday's data      
        end = schedule_nyse.market_close[-1-market_closed_indicator].strftime("%Y-%m-%d")
        start = schedule_nyse.market_close[-self.days-market_closed_indicator].strftime("%Y-%m-%d")
        return start, end
    def getDbCloseQuote(self, symbol,date):
        req = self.name+'.'+f'`{symbol}`'
        sql = f"SELECT `Adj Close` FROM {req} where Date = '{date}'"
        result = self.conn.execute(sql)
        adjCloseQ = 0.0
        for row in result:
            adjCloseQ = row['Adj Close']
        return adjCloseQ
    def calcChgs(self,symbol,start,end):
        startQ= self.getDbCloseQuote(symbol,start)
        endQ= self.getDbCloseQuote(symbol,end)
        if (endQ == 0.0) :
            return 0.0
        else:
            return (endQ-startQ)/startQ*100
    # Step2: Get the quotes data from Database based on the input parm 'days'
    def calc_percent_chgs(self):
        start, end = self.get_start_end_dates() #call step1
        print(f'get quotes start---{dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}')
        start, end = self.get_start_end_dates() #call step1
        print(f'Start Date={start}  End Date={end}')
        chgs=[]
        for symbol in self.tickers_lst:
            chg = self.calcChgs(symbol,start, end)
            chgs.append(chg)
        return chgs
    
    


# %%
class TksChgsByDays:
    #days_lst = (2,5,10) # 1D/1W/2W
    def __init__(self, name, tks, days):
        self.name = name
        self.tks = tks
        self.days =days    
    def get_chgs(self):
        tksChgsByDays = []
        for day in self.days:
            dbQouteData =DbQouteData(self.name, self.tks,day)
            tkerChgs = dbQouteData.calc_percent_chgs()
            tksChgsByDays.append(tkerChgs) 
        return tksChgsByDays


# %%
class PlotChgs:
    def __init__(self, name, chgs):
        self.name = name
        self.chgs = chgs
    def doPlot(self):
        fig = px.bar(self.chgs,width=1000, height=400)
        fig.update_layout(barmode = 'group', bargap = 0.2, bargroupgap = 0.0)
        fig.update_layout(
            title=self.name + " Percentage Changes",
            title_x=0.5,
            xaxis_tickangle=-45,
            xaxis_showticklabels= True,
            xaxis_type = 'category',
            xaxis_title="Symbols",
            yaxis_title="Percentage",
            legend_title="Days",
            font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
            )
        )
        fig.show()
        pdf = fig.to_image(format="pdf")  
        return pdf


# %%
class StockGrpBuyingReport:
    def __init__(self,name, days):
        self.name = name
        self.days = days
    def createBuyingRpt(self):
    # udpate DB
        stkGrp = Recommender(self.name)
        stkGrp.updateDB()
    # create report
        dfStkGrpSignals =stkGrp.recommend()
    # create changes
        stkGrpSymbolLst =dfStkGrpSignals.Symbol.to_list()
        stkGrpChgsByDays = TksChgsByDays(self.name, stkGrpSymbolLst, self.days)
        stkGrpTksChgsByDays = stkGrpChgsByDays.get_chgs()
        # print(dfStkGrpSignals) 
        df = pd.DataFrame(stkGrpTksChgsByDays).transpose()
        df['Symbol']= ('(' + dfStkGrpSignals.Signal +') ' + dfStkGrpSignals.Symbol).to_list() # remove signal col
        df=df.set_index('Symbol',drop = True)
        daysNames =[]
        for day in self.days:
            daysNames.append(str(day)+'days Chgs')
        df.columns = daysNames
        # print(df)
        
    # create plot
        stkGrpchgsPlot=PlotChgs(self.name, df)
        svg = stkGrpchgsPlot.doPlot()
        
    # create email
        stkGrpEmails = CreateEmails(self.name,dfStkGrpSignals.drop(columns=['Signal']),svg) #remove signal col
        stkGrpEmails.sendEmails()
    
class ArkxEtfLstBuyingRpt:
    def __init__(self,etfLst, days):
        self.etfLst = etfLst
        self.days = days
    def createArkxEtfBuyingRpt(self):
        for etf in self.etfLst:
            etfStockGrpBuyingReport = StockGrpBuyingReport(etf, self.days)
            etfStockGrpBuyingReport.createBuyingRpt()


# %%
arkxEtfLstBuyingRpt = ArkxEtfLstBuyingRpt(['ARKK','ARKF','ARKW','ARKQ','ARKX','ARKG','DJIA','SP100','CMY1'],[2,5,10])
# arkxEtfLstBuyingRpt = ArkxEtfLstBuyingRpt(['ARKX'],[2,5,10])
arkxEtfLstBuyingRpt.createArkxEtfBuyingRpt()


# %%
# etfStockGrpBuyingReport = StockGrpBuyingReport('ARKK',[2,5,10])
# etfStockGrpBuyingReport.createBuyingRpt()


# %%
# djia = Recommender('djia')
# djia.updateDB()
# dfdjiaSignals =djia.recommend()
# djiaEmails = CreateEmails('DJIA',dfdjiaSignals)
# djiaEmails.sendEmails()


# %%
# arkk = Recommender('arkk')
# # arkk.updateDB()
# dfarkkSignals =arkk.recommend()
# print(dfarkkSignals)
# tkersLst = dfarkkSignals.Symbol.to_list()
# print(tkersLst)
# dbQouteData =DbQouteData('arkk', tkersLst,2)
# chgs = dbQouteData.calc_percent_chgs()
# # arkkEmails = CreateEmails('ARKK',dfarkkSignals)
# # arkkEmails.sendEmails()


