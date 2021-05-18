#file: mineMakersPlace.py
#author: Kamiel Fokkink 

#This file serves to mine the ethereum database for all relevant data from
#the MakersPlace gallery

from query import *
import pandas as pd

MakersPlace1 = "0x2d9E5de7D36f3830c010a28B29B3BDf5cA73198e"
MakersPlace2 = "0x2A46f2fFD99e19a89476E2f62270e0a35bBf0756"
MakersPlace3 = "0x7e3abdE9D9E80fA2d1A02c89E0eae91b233CDE35"
MakersPlace4 = "0xb7bc86cb0183af5853274ae4e20d36de387c4a64" 

transferOwnership1 = "0xf2fde38b"
transferFrom1 = "0x23b872dd"
safeTransferFrom1 = "0x42842e0e"
transfer2 = "0xa9059cbb"
unknown3a = "0xf9d83bb5" #transfers value, two internal txs
unknown3b = "0x71f4d38c" #no value, internal txs
purchase3 = "0xae77c237" #transfers value, sometimes internal txs
bid3 = "0x454a2ab3"
deposit3 = "0xe2bbb158"
artistFee3 = "0xaf8ee37f" #no value, one internal transaction to artist
transferToken4 = "0xbad42590" #no value, transfers token, one internal call to MP2

"""There are no methodIDs associated to inputs. Read in all normal and internal
transactions from address 3, as this is the only address that handles the 
transfer of money. Will be more difficult to link transactions together, but
collection of data is easy. Internal transactions are not always triggered by
a contract call, also sometimes by the artist selling a token."""

def writeMakersPlaceTransactions():
    startblock = 9000000#Block before start of MakersPlace3
    endblock = 12500000 #Block after now
    blockstep = 50000
    df = pd.DataFrame(columns=columnNames)
    while(startblock < endblock):
        result = query("account","txlist",address=MakersPlace3, #Only address 3 hanldes money
                       startblock=str(startblock), endblock=str(startblock+blockstep))
        startblock = startblock + blockstep
        print(str(startblock) + ": " + str(len(result["result"])))
        if (len(result["result"])==0): 
            continue
        elif (len(result["result"])>9999): 
            print("Too large block step, missed some transactions")
        else:
            df = df.append(pd.DataFrame(result["result"]),sort=False,ignore_index=True)
    df.to_csv("../Data/Raw/MakersPlaceTransactions.csv",index=False)

def writeMakersPlaceInternalTransactions():
    startblock = 9000000 #Block before start of Foundation
    middleblock = 11700000
    endblock = 12500000
    df = pd.DataFrame(columns=internalColumns)
    res1 = query("account","txlistinternal",address=MakersPlace3,
                 startblock=str(startblock),endblock=str(middleblock))
    df = df.append(pd.DataFrame(res1["result"]),sort=False)
    
    res2 = query("account","txlistinternal",address=MakersPlace3,
                 startblock=str(middleblock),endblock=str(endblock))
    df = df.append(pd.DataFrame(res2["result"]),sort=False,ignore_index=True)
    df.to_csv("../Data/Raw/MakersPlaceInternalTransactions.csv",index=False)

def helpIDS(df):
    ids = dict()
    i = 0
    for index, row in df.iterrows():
        method = row[13][0:10]
        if (method in ids):
            ids[method]+= 1
        else:
            ids[method]=1
            print(str(i) + ": " + row[2])
            i += 1
    return ids