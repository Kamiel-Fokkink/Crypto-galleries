#file: mineSuperRare.py
#author: Kamiel Fokkink

#This file serves to mine the ethereum database for all relevant data from
#the SuperRare gallery

from query import *
import pandas as pd

#First define all the constants, addresses of smart contracts and method IDs
#to identify a type of transaction

# Adresses
SuperRare1 = "0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0"
SuperRare2 = "0x2947f98c42597966a0ec25e92843c09ac17fbaa7"
SuperRare3 = "0x41a322b28d0ff354040e2cbc676f0320d8c8850d"
SuperRare4 = "0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656"
SuperRare5 = "0x65b49f7aee40347f5a90b714be4ef086f3fe5e2c"

#Method IDs for SuperRare1
InitWhitelist = "0xb85ecf93"
AddToWhitelist = "0xe43252d7" #address add
AddNewToken = "0xd9856c21" #str uri
SetApprovalForAll = "0xa22cb465"
TransferFrom = "0x23b872dd" #address from, address to, int tokenID
Transfer1 = "0xa9059cbb" #address to, int value
RemoveFromWhitelist = "0x8ab1d681" #address remove
TransferOwnership1 = "0xf2de38b" #address newOwner
Approve = "0x095ea7b3" #address to, int tokenID
SafeTransferFrom = "0x42842e0e" #address from, address to, int tokenID
SafeTransferFromB = "0xb88d4fde" #same as above + bytes data

#Method IDs for SuperRare2
SalePriceSet2 = "0x508c1dbd" #address originContract, int tokenID, int amount
Bid2 = "0xc0f4ed31" #int bid, address originContract, int tokenID
CancelBid2 = "0x39b6b1e5" #address, int tokenID
AcceptBid2 = "0x955a5a76" #address originContract, int tokenID
Buy2 = "0xcce7ec13" #address to, int amount
CancelAuction2 = "0x96b5a755" #int tokenID
SetRoyaltyFee = "0x3e4086e5" #int percentage

#Method IDs for SuperRare3
WhitelistCreator = "0x62f11dd2" #address creator
SalePriceSet3 = "0x053992c5" #int tokenID, int saleprice
Transfer = "0xa9059cbb" #address to, int tokenID
Bid3 = "0x454a2ab3" #int tokenID
Buy3 = "0xd96a094a" #int tokenID
AddNewTokenWithEditions = "0x019871e9" #string uri, int editions, int saleprice
CancelBid3 = "0x9703ef35" #int tokenID
AcceptBid3 = "0x2b1fd58a" #int tokenID

#Method IDs for SuperRare4
CancelAuction4 = "0x859b97fe" #address nft, int tokenID
Bid4 = "0x0f41ba4b" #address bidder, int tokenID, int value

#Method IDs for SuperRare5, many are the same as SuperRare2
TransferOwnership5 = "0xf2fde38b" #address newOwner
Bid5 = "0xc0f4ed31" #int amount, address originContract, int tokenID
SalePriceSet5 = "0x508c1dbd" #address originContract, int tokenID, int amount
CancelBid5 = "0x39b6b1e5" #address of token, int tokenID
AcceptBid5 = "0x955a5a76" #address originContract, int tokenID
Buy5 = "0xcce7ec13" #address to, int amount

RelevantSRTransactionTypes = list([AcceptBid2,Buy2,SetRoyaltyFee,Transfer,AcceptBid3,
                                   Buy3])
TransactionTypes = {"0x955a5a76":"AcceptBid2","0xcce7ec13":"Buy2",
                    "0x3e4086e5":"SetRoyaltyFee","0xa9059cbb":"Transfer",
                    "0x2b1fd58a":"AcceptBid3","0xd96a094a":"Buy3"}

def collectSuperRareTransactions(addr):
    """For one contract addresses, find the history of all transactions from the
    beginning until the present. Return as a pandas dataframe"""

    startblock = 4500000 #Block before start of SuperRare
    endblock = 12500000 #Block after now
    blockstep = 150000
    df = pd.DataFrame(columns=columnNames)
    while (startblock < endblock):
        result = query("account","txlist",address=addr,startblock=str(startblock),
                       endblock=str(startblock+blockstep))
        startblock = startblock + blockstep
        if (len(result["result"])==0):
            continue
        elif (len(result["result"])>9999):
            print("Too large block step, missed some transactions")
        else:
            df = df.append(pd.DataFrame(result["result"]),sort=False)
    return df

def writeAllSuperRareTransactions():
    """Collect all transactions for each of the 5 SuperRare contracts. Those
    that are relevant get put into a dataframe, and saved into a csv file.
    Because contract 5 has the same methodIDs as contract 2, the transaction types
    are updated in between."""

    df = collectSuperRareTransactions(SuperRare1)
    df = df.append(collectSuperRareTransactions(SuperRare2))
    df = df.append(collectSuperRareTransactions(SuperRare3))

    outDf = pd.DataFrame(columns=columnNames+["type"])
    for index, row in df.iterrows():
        method = row[13][0:10]
        if (method in RelevantSRTransactionTypes):
            series = row.append(pd.Series(data=[TransactionTypes[method]],index=["type"]))
            outDf = outDf.append(series,ignore_index=True)
        else:
            continue

    TransactionTypes["0x955a5a76"] = "AcceptBid5"
    TransactionTypes["0xcce7ec13"] = "Buy5"
    df2 = collectSuperRareTransactions(SuperRare4)
    df2 = df2.append(collectSuperRareTransactions(SuperRare5))
    for index, row in df2.iterrows():
        method = row[13][0:10]
        if (method in RelevantSRTransactionTypes):
            series = row.append(pd.Series(data=[TransactionTypes[method]],index=["type"]))
            outDf = outDf.append(series,ignore_index=True)
        else:
            continue
    outDf.to_csv("../Data/Raw/SuperRareTransactions.csv",index=False)

def writeAllSuperRareInternalTransactions():
    """For all the regular transactions collected in the previous step, a list of
    internal transactions associated to it are retrieved, and saved to a csv file."""

    inDf = pd.read_csv("../Data/Raw/SuperRareTransactions.csv")
    internalTransactionTypes = ["AcceptBid5","Buy5","AcceptBid3","Buy3","AcceptBid2","Buy2"]
    outDf = pd.DataFrame(columns=internalColumns+["txType","txHash"])
    for index, row in inDf.iterrows():
        if (row[18] in internalTransactionTypes):
            try:
                result = query("account","txlistinternal",txhash=row[2])
                for res in result["result"]:
                    series = pd.DataFrame([res])
                    series['txType'] = row[18]
                    series['txHash'] = row[2]
                    outDf = outDf.append(series,ignore_index=True)
            except:
                continue
        else:
            continue
    outDf.to_csv("../Data/Raw/SuperRareInternalTransactions.csv",index=False)
