import ccxt
import json
import pandas as pd
import gspread
import csv
import time
import requests
from datetime import datetime

from google.colab import auth
auth.authenticate_user()
import gspread
from oauth2client.client import GoogleCredentials
 
def getBalance():
  dfBalance = pd.DataFrame(exchange.fetch_balance(),columns=['USD','BTC','BTC-1225','free','used','total'])
  dfBalanceList = dfBalance.values.tolist()
  display(dfBalance)
 
def getJsonPrice():
  # 2.4 ใช้ json ในการดึงข้อมูลราคา PRODUCT ที่เราสนใจจะเทรด
  pairTicker = json.dumps(exchange.fetch_ticker(pair))
  #r2 = json.dumps(exchange.fetch_ticker('XRP/USD'))
  JsonPrice = json.loads(pairTicker)
  print('BTC-1225 =',JsonPrice['last'],'$')
  return JsonPrice['last']
 
  # 9.การตรวจเช็คออเดอร์ที่ เปิด PENDING อยู่ (ลบ COLUMN ที่ไม่ได้ใช้ออก และจัดระเบียบใหม่)
def getPendingOrder():
  print("=============================================")
  print(" ")
  print("PENDING ORDER") 
  dfPendingOrder = pd.DataFrame(exchange.fetch_open_orders(pair),columns=['id','datetime','symbol','type','side','price','amount','filled','remaining','cost'])
  display(dfPendingOrder)
  print(" ")
 
  # 11.การตรวจเช็คออเดอร์ที่ MATCH BUY (FILTER ออกมาเป็นตาราง)
def getMatchBuyOrder():
  print("=============================================")
  print(" ")
  print("MATCH BUY ORDER")
  dfMatchBuy = pd.DataFrame(filter(lambda x:x['side'] == "buy",exchange.fetchMyTrades(pair)),columns=['id','datetime','symbol','side','price','amount','cost'])
  display(dfMatchBuy)
  print(" ")
 
  # 11.การตรวจเช็คออเดอร์ที่ MATCH SELL (FILTER ออกมาเป็นตาราง)
def getMatchSellOrder():
  print("=============================================")
  print(" ")
  print("MATCH SELL ORDER")
  dfMatchSell = pd.DataFrame(filter(lambda x:x['side'] == "sell",exchange.fetchMyTrades(pair)),columns=['id','datetime','symbol','side','price','amount','cost'])
  display(dfMatchSell)
  print(" ")
 
def getTime():
  localTime = time.localtime()
  Time = time.strftime("%m/%d/%Y, %H:%M:%S",localTime)
  print(Time)

def getsumExposure() :
  dfRes = getUpdateDataFromGsheet()['dfRes']
  sumExposure = []
  totalSellExposure = dfRes[dfRes['SIDE']=="sell"]['COST($)'].sum()
  totalBuyExposure  = dfRes[dfRes['SIDE']=="buy"]['COST($)'].sum()
  countSellExposure = dfRes[dfRes['SIDE']=="sell"]['COST($)'].count()
  countBuyExposure  = dfRes[dfRes['SIDE']=="buy"]['COST($)'].count()

  sumExposureValue = totalBuyExposure-totalSellExposure

  sumExposure.append(round(sumExposureValue,2))        #sumExposure[0]
  sumExposure.append(round(totalBuyExposure,3))        #sumExposure[1]
  sumExposure.append(round(totalSellExposure,3))       #sumExposure[2]
  sumExposure.append(countSellExposure)                #sumExposure[3]
  sumExposure.append(countBuyExposure)                 #sumExposure[4]
  return sumExposure
 
def getUpdateDataFromGsheet():
  gc = gspread.authorize(GoogleCredentials.get_application_default())
  sh = gc.open_by_key('1nw0Dd9IRZUfPp9ZV6GS7obPO2vZrKKBXpGk5bX-j4SY')
  worksheet1 = sh.sheet1

  res = worksheet1.get_all_records()
  dfRes = pd.DataFrame(res)

  RETURN = {"dfRes":dfRes,"worksheet1":worksheet1}
  return RETURN

def getUpdateGGRecord():

  recordInGsheet = getUpdateDataFromGsheet()["dfRes"]['ID'].tolist()
  dfMyTrade = pd.DataFrame(exchange.fetchMyTrades(pair),columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
  dfMyTradeList = dfMyTrade.values.tolist()

  dfMyTradeFee = pd.DataFrame(exchange.fetchMyTrades(pair),columns=['id','fee'])
  dfMyTradeFeeList = dfMyTradeFee.values.tolist()

  for i in range (len(dfMyTradeList)):
    dfMyTradeList[i].append((dfMyTradeFeeList[i][1])['cost'])
    if int(dfMyTradeList[i][0]) not in recordInGsheet :
      getUpdateDataFromGsheet()['worksheet1'].insert_row(dfMyTradeList[i],(len(getUpdateDataFromGsheet()['dfRes'])+2))

  recordInGsheet = getUpdateDataFromGsheet()["dfRes"]['ID'].tolist()

  return recordInGsheet

def getNetSize():
  netSize = json.dumps(exchange.privateGetAccount())
  netSize = json.loads(netSize)
  netSize = json.dumps(netSize["result"])
  netSize = json.loads(netSize)
  netSizeDict = json.dumps(netSize["positions"])
  netSizeDict = json.loads(netSizeDict)
  if len(netSizeDict) > 0 :
    netSizeDict = netSizeDict[0]
    print('COLLATERAL(FREE/TOTAL) =',"%.2f" % netSize['freeCollateral'],'/',"%.2f" % netSize['collateral'],'USD')
    sumExposure = getsumExposure()
    print('EXPOSURE(NOW/MAX.)     =',"%.2f" % sumExposure[0],'/',"%.2f" % jsonPrice,'USD')
    print(netSizeDict['future'],'POSITION      =',"%.2f" % netSizeDict['netSize'],'BTC')

'''
def getBackupRecord():
  drive.mount('drive')
  !cp HCxEsport1TRADINGLOG.csv "drive/My Drive/Database/HCxEsport1"
  print("BACKUP TRADINGLOG IN GOOGLE SHEET SUCCESSFUL!")
  print(" ")
  print("=============================================")
  print(" ")
'''

#==============================================================================
# UPDATE PENDING ORDER เพื่อป้องกันการเปิด POSITION เกินกำหนด

def getUpdatePending():
 
  dfMyOrder = pd.DataFrame(exchange.fetchOpenOrders(pair),columns=['id','timestamp','datetime','symbol','side','price','amount','cost'])
  dfMyOrderList = dfMyOrder.values.tolist()
 
  return dfMyOrderList
 
def getDetails() :
  getUpdateGGRecord()
  a = exchange.private_get_positions()['result'][0]['netSize']
  b = getUpdateDataFromGsheet()['dfRes']
  sell = b[ b['SIDE'] == 'sell']['QTY($)'].sum()
  buy = b[ b['SIDE'] == 'buy']['QTY($)'].sum()

  print('net size form ftx :',a)
  print('net size form acc :', round(buy - sell,4))

def getExecute():

  if jsonPrice < float(sumExposure[0])*(-1) :
    print("YOU HAVE TO TRADE IN BUY SIDE")
    print("WITH MAXIMUM BUY EXPOSURE =",(float(sumExposure[0])*(-1))-jsonPrice,'$')
    
    if (float(sumExposure[0])*(-1))-jsonPrice >= 10 :
    # Auto feed
      usd = (float(sumExposure[0])*(-1))-jsonPrice
      price = float(jsonPrice)
      size_order = round(usd/price,4)

    # Manual feed
      #usd = float(input("INPUT USD ="))
      #price = float(input("BUY LIMIT PRICE ="))
      #size_order = round(usd/price,4)

      if size_order > (float(sumExposure[0])*(-1))-jsonPrice :
        print("YOUR EXPOSURE IS OVER !")
      else :
        print(pair,"limit","buy",price,usd,'$','(',size_order,'BTC',')')
        confitmOrder = str(input("CONFIRM YOUR ORDER ? (Y/N) ="))
        if confitmOrder == "Y":
          exchange.create_order(pair,"limit","buy",size_order, price)
          print("Order Execute")
          getPendingOrder()
        elif confitmOrder == "N":
          print("Canceled open order")
    elif (float(sumExposure[0])*(-1))-jsonPrice < 10 :
      print("Size too small (less than 10 $)")

    
 
  if jsonPrice > float(sumExposure[0])*(-1) :
    print("YOU HAVE TO TRADE IN SELL SIDE")
    print("WITH MAXIMUM SELL EXPOSURE =",jsonPrice-(float(sumExposure[0])*(-1)),'$')

    if (jsonPrice-(float(sumExposure[0])*(-1))) >= 10 :
      # Auto feed
      usd = float(jsonPrice-(float(sumExposure[0])*(-1)))
      price = float(jsonPrice)
      size_order = round(usd/price,4)

      # Manual feed
      #usd = float(input("INPUT USD ="))
      #price = float(input("SELL LIMIT PRICE ="))
      #size_order = round(usd/price,4)

      if size_order > (jsonPrice-(float(sumExposure[0])*(-1))) :
        print("YOUR EXPOSURE IS OVER !")
      else :
        print(pair,"limit","sell",price,usd,'$','(',size_order,'BTC',')')
        confitmOrder = str(input("CONFIRM YOUR ORDER ? (Y/N) ="))
        if confitmOrder == "Y":
          exchange.create_order(pair, "limit" , "sell", size_order, price)
          print("ORDER EXECUTE")
          getPendingOrder()
        elif confitmOrder == "N":
          print("Canceled open order")
    elif (jsonPrice-(float(sumExposure[0])*(-1))) < 10 :
      print("Size too small (less than 10 $)")

# ============================== ACCOUNT PARAMETER ============================== #

apiKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"    #HCxEsport
secret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"    #HCxEsport

exchange = ccxt.ftx({'apiKey':apiKey, 'secret':secret, 'enableRateLimit':True})
exchange.headers = {'FTX-SUBACCOUNT':'XXXXXX',}         #ELEPH

# ============================== PRODUCT PARAMETER ============================== #
pair = "BTC-1225"
getUpdateGGRecord()
jsonPrice = getJsonPrice()
getNetSize()
getDetails()
getPendingOrder()
getMatchBuyOrder()
getMatchSellOrder()

# ================================ OPEN NEW ORDER =============================== #

dfMyOrderList = getUpdatePending()
getUpdateGGRecord()
confitmOrder  = " "
confirmCancel = " "
pair = "BTC-1225"
jsonPrice = getJsonPrice()
sumExposure = getsumExposure()
print("ACTUAL EXPOSURE IN PORTFOLIO =",sumExposure[0],'$')
print(" ")

if len(dfMyOrderList) > 0 :
  print("YOU ALREADY HAVE A PENDING ORDER")
  print(dfMyOrderList)
  confirmCancel = input("DO YOU WANT TO CANCEL OLD ORDER ? (Y/N) ")
  if confirmCancel == "Y" :
    exchange.cancel_order(dfMyOrderList[0][0])
    print("ORDER CANCELED")
    sumExposure = getsumExposure()
    getExecute()
  else :
    print("ORDER STILL OPEN")
else:
  getExecute()
  getDetails()