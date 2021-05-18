#file: mineFoundation.py
#author: Kamiel Fokkink

#This file serves to mine the ethereum database for all relevant data from
#the Foundation gallery

from query import *
import pandas as pd

Foundation1 = "0xcDA72070E455bb31C7690a170224Ce43623d0B6f"
Foundation2 = "0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405"

# For Foundation at place bid you transfer a value of Ether already. When
# someone overbids, the previous value is transferred back. At acceptBid, the
# Ether is already in, and triggers internal txs to distribute that money
# Foundation2 handles no money in any transaction, only does transfer and 
# management of tokens

Something = "0x03ec16d7" #7000, unclear what it does, no value, token transfer not inernal tx
TokenTransfer = "0x21506fff" #900, from Found1 to caller of tx, no value nor internal tx
TokenCreation = "0x4ce6931a" # 20000
ContractCreation = "0x60806040"
PlaceBid = "0x9979ef45" #24000, transfers value already
AcceptBid = "0x7430e0c6" #8000, Activates internal transactions, no incoming value 

RelevantTransactionTypes = list([AcceptBid, PlaceBid])
FoundationTransactionTypes = {"0x7430e0c6":"AcceptBid","0x9979ef45":"PlaceBid"}

def collectFoundationTransactions(addr):
    startblock = 11000000 #Block before start of Foundation
    endblock = 12200000 #Block after now
    blockstep = 30000
    df = pd.DataFrame(columns=["blockNumber","timeStamp","hash","nonce","blockHash",
                               "transactionIndex","from","to","value","gas","gasPrice",
                               "isError","txreceipt_status","input","contractAddress",
                               "cumulativeGasUsed","gasUsed","confirmations"])
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
    inDf = pd.read_csv("../Data/FoundationTransactions.csv")
    internalTransactionTypes = ["AcceptBid","PlaceBid"]
    outDf = pd.DataFrame(columns=internalColumns +["txType","txHash"])
    i=0
    for index, row in inDf.iterrows():
        i += 1
        if ((i%100)==0): 
            print(i)
        result = query("account","txlistinternal",txhash=row[2])
        for res in result["result"]:
            series = pd.DataFrame(data=[list(res.values()) + ([row[18],row[2]])],
                                    columns=internalColumns +["txType","txHash"])
            outDf = outDf.append(series,ignore_index=True)

    outDf.to_csv("../Data/Raw/FoundationInternalTransactions.csv",index=False)