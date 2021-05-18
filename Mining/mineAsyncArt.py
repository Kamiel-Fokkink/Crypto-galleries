#file: mineAsyncArt.py
#author: Kamiel Fokkink

#This file serves to mine the ethereum database for all relevant data from
#the Async Art gallery

from query import *
import pandas as pd

# Addresses for AsyncArt
AsyncArt1 = "0x6c424C25e9F1ffF9642cB5B7750b0Db7312c29ad"
AsyncArt2 = "0xb6dAe651468E9593E4581705a09c10A76AC1e0c8"

#Method IDs for AsyncArt1
mintArtwork = "0x98c8103e"
updateWhitelist = "0x0d392cd9"
setupContolToken1 = "0x1e9b0f52"
bid1 = "0x454a2ab3" #int tokenID
withdrawBid1 = "0x0eaaf4c8" #int tokenID
useControlToken = "0x755db7b3"
acceptBid1 = "0x2b1fd58a" #int tokenID
transferFrom = "0x23b872dd" #address from, address to, int tokenID
makeBuyPrice = "0xd91c070c"
setApprovalForAll = "0xa22cb465"
updateRoyaltyPercentage = "0x8cb6ddcb" #int platformPrimPerc, int platformSecPerc,
                                       #int artistSecPerc. V. Nice! :)
grantControlPermission1 = "0x41afca00"
approve = "0x095ea7b3"
safeTransferFrom = "0x42842e0e" #address from, address to, int tokenID

#Method IDs for AsyncArt2
setExpectedTokenSupply = "0x892918d4"
whitelistTokenForCreator = "0xe50d48f1"
setupControlToken2 = "0xf83036f0"
acceptBid2 = "0x02e9d5e4"
takeBuyPrice = "0x2d4fbf83" #Triggers a bunch of internal transactions
waiveFirstSaleRequirement = "0x7a718d71"
grantControlPermission2 = "0x0a29bb37"
updateTokenURI = "0x18e97fd1"
transfer = "0xa9059cbb" #address to, int value
updateMinterAddress = "0x2fcfb95a"

RelevantTransactionTypes = list([acceptBid1, transferFrom, safeTransferFrom,
                                 updateRoyaltyPercentage,acceptBid2,takeBuyPrice,
                                 transfer])
AATransactionTypes = {"0x2b1fd58a":"acceptBid1","0x23b872dd":"transferFrom",
                      "0x42842e0e":"safeTransferFrom","0x8cb6ddcb":"updateRoyaltyPercentage",
                      "0x02e9d5e4":"acceptBid2","0x2d4fbf83":"takeBuyPrice",
                      "0xa9059cbb":"transfer"}

def writeAsyncArtTransactions():
    """For the two contract addresses, collect the history of all transactions.
    If they are relevant, associated to a sale, put into a dataframe and save
    those as csv."""

    df = pd.DataFrame(columns=columnNames)

    result1 = query("account","txlist",address=AsyncArt1)
    df = df.append(pd.DataFrame(result1["result"]),sort=False)

    result2 = query("account","txlist",address=AsyncArt2)
    df = df.append(pd.DataFrame(result2["result"]),sort=False)

    outDf = pd.DataFrame(columns=columnNames+["type"])
    for index, row in df.iterrows():
        method = row[13][0:10]
        if (method in RelevantTransactionTypes):
            series = row.append(pd.Series(data=[AATransactionTypes[method]],index=["type"]))
            outDf = outDf.append(series,ignore_index=True)
        else:
            continue
    outDf.to_csv("../Data/Raw/AsyncArtTransactions.csv",index=False)

def writeAsyncArtInternalTransactions():
    """For all previously found transactions, collect the internal transactions
    associated to them, and save in a csv file."""

    inDf = pd.read_csv("AsyncArtTransactions.csv")
    internalTransactionTypes = ["acceptBid1","takeBuyPrice","acceptBid2"]
    outDf = pd.DataFrame(columns=internalColumns +["txType","txHash"])
    for index, row in inDf.iterrows():
        if (row[18] in internalTransactionTypes):
            result = query("account","txlistinternal",txhash=row[2])
            for res in result["result"]:
                series = pd.DataFrame(data=[list(res.values()) + ([row[18],row[2]])],
                                            columns=internalColumns +["txType","txHash"])
                outDf = outDf.append(series,ignore_index=True)
        else:
            continue
    outDf.to_csv("../Data/Raw/AsyncArtInternalTransactions.csv",index=False)
