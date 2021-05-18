#file: mineFoundation.py
#author: Kamiel Fokkink

#This file serves to mine the ethereum database for all relevant data from
#the Foundation gallery

from query import *
import pandas as pd

Foundation1 = "0xcDA72070E455bb31C7690a170224Ce43623d0B6f"
Foundation2 = "0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405"

Something = "0x03ec16d7" #7000, unclear what it does, no value, token transfer not inernal tx
TokenTransfer = "0x21506fff" #900, from Found1 to caller of tx, no value nor internal tx
TokenCreation = "0x4ce6931a" # 20000
ContractCreation = "0x60806040"
PlaceBid = "0x9979ef45" #24000, transfers value already
AcceptBid = "0x7430e0c6" #8000, Activates internal transactions, no incoming value

RelevantTransactionTypes = list([AcceptBid, PlaceBid])
FoundationTransactionTypes = {"0x7430e0c6":"AcceptBid","0x9979ef45":"PlaceBid"}

"""For Foundation, a user already transfers a value of ETH at the moment of
place bid. When someone else overbids, the previous value is transferred back.
At acceptBid, the money is already received, and internal transactions are
triggered to distribute that money and transfer the token. So to find out what
happens for a sale, it is required to know the last made bid, as well as the
accept bid. All the financial transactions are handled by contract 1, with 2 in
charge of token transfers."""

def collectFoundationTransactions(addr):
    """Collect all transactions for a given contract address"""

    startblock = 11000000 #Block before start of Foundation
    endblock = 12200000 #Block after now
    blockstep = 30000
    df = pd.DataFrame(columns=columnNames)
    while (startblock < endblock):
        result = query("account","txlist",address=addr,startblock=str(startblock),
                       endblock=str(startblock+blockstep))
        startblock = startblock + blockstep
        print(str(startblock) + ": " + str(len(result["result"])))
        if (len(result["result"])==0):
            continue
        elif (len(result["result"])>9999):
            print("Too large block step, missed some transactions")
        else:
            df = df.append(pd.DataFrame(result["result"]),sort=False)
    return df

def writeFoundationTransactions():
    """Collect all relevant transactions for contract 1, and save to csv."""

    df = collectFoundationTransactions(Foundation1)
    outDf = pd.DataFrame(columns=columnNames+["type"])
    for index, row in df.iterrows():
        method = row[13][0:10]
        if (method in RelevantTransactionTypes):
            series = row.append(pd.Series(data=[FoundationTransactionTypes[method]]
                            ,index=["type"]))
            outDf = outDf.append(series,ignore_index=True)
        else:
            continue
    outDf.to_csv("../Data/Raw/FoundationTransactions.csv",index=False)

def writeFoundationInternalTransactions():
    """Collect all internal transactions associated to each transaction that
    has previously been found."""

    inDf = pd.read_csv("../Data/FoundationTransactions.csv")
    internalTransactionTypes = ["AcceptBid","PlaceBid"]
    outDf = pd.DataFrame(columns=internalColumns +["txType","txHash"])
    for index, row in inDf.iterrows():
        result = query("account","txlistinternal",txhash=row[2])
        for res in result["result"]:
            series = pd.DataFrame(data=[list(res.values()) + ([row[18],row[2]])],
                                    columns=internalColumns +["txType","txHash"])
            outDf = outDf.append(series,ignore_index=True)

    outDf.to_csv("../Data/Raw/FoundationInternalTransactions.csv",index=False)
