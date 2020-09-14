import pandas as pd
import json
import csv
from csv import DictWriter
import time
import ccxt
import gspread
from datetime import datetime

# กำหนด PARAMETER ที่จำเป็น
                                        
apiKey = "XXXXXXXXXXXXXXXXXX"     #SENSEBOT1
secret = "XXXXXXXXXXXXXXXXXX"     #SENSEBOT1
exchange = ccxt.ftx({'apiKey' : apiKey ,'secret' : secret ,'enableRateLimit': True})
exchange.headers = {'FTX-SUBACCOUNT':"XXXXXXXXXXXXXXXXXX",}      #SENSEBOT1

pair = "BTC-PERP"
portName = "CF CALCULATION BOT"

def updatepaired():

        dfpaired        = pd.read_csv("pairedOrder.csv")
        dfpositionBuy   = pd.read_csv("holdBuy.csv")
        dfpositionSell  = pd.read_csv("holdSell.csv")

        Loop = True

        while Loop :

            BUYQTY = dfpositionBuy['QTY($)'].sum()
            SELLQTY = dfpositionSell['QTY($)'].sum()
            if SELLQTY == 0 or BUYQTY == 0 :
                Loop = False
            elif SELLQTY != 0 and BUYQTY != 0 :
                if SELLQTY >= BUYQTY :
                    for i in range (len(dftradinglog)) :
                        if dfpositionBuy['QTY($)'].iloc[i] > dfpositionSell['QTY($)'].iloc[-i-1]  :  
                            a =  round(dfpositionBuy['QTY($)'].iloc[i] - dfpositionSell['QTY($)'].iloc[-i-1],4)
                            qty = round(dfpositionBuy.iat[i,6],4)
                            cost = round(dfpositionBuy.iat[i,7],4)   #
                            fee = round(dfpositionBuy.iat[i,8],4)    #
                            dfpositionBuy.iat[i,6] = round(dfpositionSell['QTY($)'].iloc[-i-1],4)
                            dfpositionBuy.iat[i,7] = round(dfpositionBuy.iat[i,6]*dfpositionBuy['PRICE'].iloc[i],4)      #
                            dfpositionBuy.iat[i,8] = round((dfpositionBuy.iat[i,8]/qty)*dfpositionSell['QTY($)'].iloc[-i-1],4)        #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[-i-1])
                                wr.writerow(dfpositionBuy.iloc[i])
                            dfpositionBuy.iat[i,6] = a
                            dfpositionBuy.iat[i,7] = round(cost-dfpositionBuy.iat[i,7],4)            #
                            dfpositionBuy.iat[i,8] = round(fee-dfpositionBuy.iat[i,8],4)            #
                            dfpositionSell.drop(dfpositionSell.index[-i-1],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[i] < dfpositionSell['QTY($)'].iloc[-i-1]  : 
                            a =  round(dfpositionSell['QTY($)'].iloc[-i-1] - dfpositionBuy['QTY($)'].iloc[i],4)
                            qty = round(dfpositionSell.iat[-i-1,6],4)
                            cost = round(dfpositionSell.iat[-i-1,7],4)   #
                            fee = round(dfpositionSell.iat[-i-1,8],4)    #
                            dfpositionSell.iat[-i-1,6] = round(dfpositionBuy['QTY($)'].iloc[i],4)
                            dfpositionSell.iat[-i-1,7] = round(dfpositionSell.iat[-i-1,6]*dfpositionSell['PRICE'].iloc[-i-1],4)      #
                            dfpositionSell.iat[-i-1,8] = round((dfpositionSell.iat[-i-1,8]/qty)*dfpositionBuy['QTY($)'].iloc[i],4)       #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[-i-1])
                                wr.writerow(dfpositionBuy.iloc[i])
                            dfpositionSell.iat[-i-1,6] = a
                            dfpositionSell.iat[-i-1,7] = round(cost-dfpositionSell.iat[-i-1,7],4)            #
                            dfpositionSell.iat[-i-1,8] = round(fee-dfpositionSell.iat[-i-1,8],4)             #
                            dfpositionBuy.drop(dfpositionBuy.index[i],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[i] == dfpositionSell['QTY($)'].iloc[-i-1]  :  
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[-i-1])
                                wr.writerow(dfpositionBuy.iloc[i])
                            dfpositionBuy.drop(dfpositionBuy.index[i],inplace = True)
                            dfpositionSell.drop(dfpositionSell.index[-i-1],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break

                if SELLQTY < BUYQTY :
                    for i in range (len(dftradinglog)) :
                        if dfpositionBuy['QTY($)'].iloc[-i-1] > dfpositionSell['QTY($)'].iloc[i]  :  
                            a =  round(dfpositionBuy['QTY($)'].iloc[-i-1] - dfpositionSell['QTY($)'].iloc[i],4)
                            qty = round(dfpositionBuy.iat[-i-1,6],4)
                            cost = round(dfpositionBuy.iat[-i-1,7],4)   #
                            fee = round(dfpositionBuy.iat[-i-1,8],4)    #
                            dfpositionBuy.iat[-i-1,6] = round(dfpositionSell['QTY($)'].iloc[i],4)
                            dfpositionBuy.iat[-i-1,7] = round(dfpositionBuy.iat[-i-1,6]*dfpositionBuy['PRICE'].iloc[-i-1],4)      #
                            dfpositionBuy.iat[-i-1,8] = round((dfpositionBuy.iat[-i-1,8]/qty)*dfpositionSell['QTY($)'].iloc[i],4)        #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[i])
                                wr.writerow(dfpositionBuy.iloc[-i-1])
                            dfpositionBuy.iat[-i-1,6] = a
                            dfpositionBuy.iat[-i-1,7] = round(cost-dfpositionBuy.iat[-i-1,7],4)            #
                            dfpositionBuy.iat[-i-1,8] = round(fee-dfpositionBuy.iat[-i-1,8],4)             #
                            dfpositionSell.drop(dfpositionSell.index[i],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[-i-1] < dfpositionSell['QTY($)'].iloc[i]  : 
                            a =  round(dfpositionSell['QTY($)'].iloc[i] - dfpositionBuy['QTY($)'].iloc[-i-1],4)
                            qty = round(dfpositionSell.iat[i,6],4)
                            cost = round(dfpositionSell.iat[i,7],4)   #
                            fee = round(dfpositionSell.iat[i,8],4)    #
                            dfpositionSell.iat[i,6] = round(dfpositionBuy['QTY($)'].iloc[-i-1],4)
                            dfpositionSell.iat[i,7] = round(dfpositionSell.iat[i,6]*dfpositionSell['PRICE'].iloc[i],4)      #
                            dfpositionSell.iat[i,8] = round((dfpositionSell.iat[i,8]/qty)*dfpositionBuy['QTY($)'].iloc[-i-1],4)        #
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[i])
                                wr.writerow(dfpositionBuy.iloc[-i-1])
                            dfpositionSell.iat[i,6] = a
                            dfpositionSell.iat[i,7] = round(cost-dfpositionSell.iat[i,7],4)            #
                            dfpositionSell.iat[i,8] = round(fee-dfpositionSell.iat[i,8],4)             #
                            dfpositionBuy.drop(dfpositionBuy.index[-i-1],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break    
                        elif dfpositionBuy['QTY($)'].iloc[-i-1] == dfpositionSell['QTY($)'].iloc[i]  :  
                            with open("pairedOrder.csv", "a+", newline='') as fp:
                                wr = csv.writer(fp, dialect='excel')
                                wr.writerow(dfpositionSell.iloc[i])
                                wr.writerow(dfpositionBuy.iloc[-i-1])
                            dfpositionBuy.drop(dfpositionBuy.index[-i-1],inplace = True)
                            dfpositionSell.drop(dfpositionSell.index[i],inplace = True)
                            dfpositionBuy.to_csv("holdBuy.csv",index = False, encoding = 'utf8')
                            dfpositionSell.to_csv("holdSell.csv",index = False, encoding = 'utf8')
                            break

def updateposition():

    QTYneworders = 0

    for i in range (MarkupFuction,len(dftradinglog),1):
        if((dftradinglog['ID'][i] not in (dfpositionBuy['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpositionSell['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpaired['ID']).tolist())):
            QTYneworders += 1
    print("QTY NEW ORDERS =",QTYneworders)

    for i in range (MarkupFuction,len(dftradinglog),1):  # update tradinglog ใหม่ล่าสุด ลงใน position
        if((dftradinglog['ID'][i] not in (dfpositionBuy['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpositionSell['ID'].tolist())) and (dftradinglog['ID'][i] not in (dfpaired['ID']).tolist())):
            if dftradinglog['SIDE'][i] == "buy" :
                with open("holdBuy.csv", "a+", newline='') as fp:
                    wr = csv.writer(fp, dialect='excel')
                    wr.writerow(dftradinglog.loc[i])       
            elif dftradinglog['SIDE'][i] == "sell" :
                with open("holdSell.csv", "a+", newline='') as fp:
                    wr = csv.writer(fp, dialect='excel')
                    wr.writerow(dftradinglog.loc[i])
            updatepaired()

def getCalculateCF() :
    print("CALCULATING PNL","(",portName,")")
    dfPaired    = pd.read_csv("pairedOrder.csv")
    dfCalculate = pd.DataFrame([])

    dfPairedBuy = dfPaired.loc[(dfPaired['SIDE'] == 'buy')].reset_index()
    dfPairedSell = dfPaired.loc[(dfPaired['SIDE'] == 'sell')].reset_index()

    dfCalculate['TIMESTAMP']  = dfPairedSell['TIMESTAMP']
    dfCalculate['PRICE BUY']  = dfPairedBuy['PRICE']
    dfCalculate['PRICE SELL'] = dfPairedSell['PRICE']
    dfCalculate['QTY($)']     = dfPairedSell['QTY($)']
    dfCalculate['PNL($)']     = (dfPairedSell['PRICE']-dfPairedBuy['PRICE'])*dfPairedSell['QTY($)']-(dfPairedSell['FEE($)']+dfPairedBuy['FEE($)'])

    dfCalculate['TIMESTAMP'] = pd.to_datetime(dfCalculate['TIMESTAMP'], unit='ms')

    PNL = dfCalculate[dfCalculate['TIMESTAMP']>datetime(2020,7,7)]
    SUMPNL0 = PNL.resample('D', on='TIMESTAMP')['PNL($)'].sum()
    SUMPNL = SUMPNL0.to_frame('SUM').reset_index()
    ACCUMPNL = SUMPNL0.cumsum()                           # ได้ตารางออกมาเป็น Type series
    ACCUMPNL = ACCUMPNL.to_frame('ACCUM').reset_index()   # แปลงกลับมาให้เป็น DataFrame
    print(SUMPNL)
    print("ACC.CF       ",round(ACCUMPNL['ACCUM'].iloc[-1],6))

def getPrice():
    PRICE = json.dumps(exchange.fetch_ticker(pair)['last']) # ราคา btc-perp ล่าสุด
    PRICE = (float(PRICE))
    return PRICE

def getRecheck() :
    SUM = 0
    for i in range (len(dfpositionBuy)) :
        SUM += dfpositionBuy['PRICE'][i]*dfpositionBuy['QTY($)'][i]

    AVG = SUM/dfpositionBuy['QTY($)'].sum()
    DD  = (AVG-getPrice())*dfpositionBuy['QTY($)'].sum()

    print("-------------------RECHECK POSITIONS-------------------")
    print("TOTAL QTY. =",round(dftradinglog['QTY($)'].sum(),4))
    print("SUM PAIRED AND HOLD QTY. =",round(dfpaired['QTY($)'].sum()+dfpositionBuy['QTY($)'].sum()+dfpositionSell['QTY($)'].sum(),4))
    print("PAIRED QTY. =",round(dfpaired['QTY($)'].sum(),4))
    print("TOTAL HOLD BUY QTY.=",round(dfpositionBuy['QTY($)'].sum(),4))
    print("TOTAL HOLD SELL QTY. =",round(dfpositionSell['QTY($)'].sum(),4))
    print("BTC PRICE =",getPrice())
    print("DD. =",DD)


# ====================================================================================

MarkupFuction = 0
MAINLOOP = True
while MAINLOOP :
    
    dftradinglog    = pd.read_csv("tradingLog.csv")
    dfpaired        = pd.read_csv("pairedOrder.csv")
    dfpositionBuy   = pd.read_csv("holdBuy.csv")
    dfpositionSell  = pd.read_csv("holdSell.csv")

    BUYQTY = dfpositionBuy['QTY($)'].sum()
    SELLQTY = dfpositionSell['QTY($)'].sum()

    print("UPDATE AND PAIRING POSITION PLEASE WAIT A MINUTE :)","(",portName,")")
    
    if BUYQTY != 0 and SELLQTY != 0 :
        updatepaired()
    elif BUYQTY == 0 or SELLQTY == 0 :             
        updateposition()
        for i in range (1,len(dftradinglog),1):
            if ((dftradinglog['ID'][i] in (dfpositionBuy['ID'].tolist())) or (dftradinglog['ID'][i] in (dfpositionSell['ID'].tolist())) or (dftradinglog['ID'][i] in (dfpaired['ID']).tolist())) :
                getCalculateCF()
                MAINLOOP = False
                getRecheck()
                print("FINISH UPDATED !",portName)
                print("-------------------------------------------------------")
                break