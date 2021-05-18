#file: mineMakersPlace.py
#author: Kamiel Fokkink

#This file serves to mine the ethereum database for all relevant data from
#the Makers Place gallery

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

"""For Makers Place, most method IDs are not identified by their function, only a
hexadecimal string. It is unclear which transaction types represent which actions.
Hence, for this gallery, all transactions containing a financial value are
collected. Only contract 3 handles the transfer of money, so this mining process
consists of collecting all the regular and internal transactions associated to
this contract. It will be more difficult to link transactions belonging to the
same sale together, but the data collection process is really easy."""

def writeMakersPlaceTransactions():
    """Read in all transactions from contract 3, and save as csv."""

    startblock = 9000000#Block before start of MakersPlace3
    endblock = 12500000 #Block after now
    blockstep = 50000
    df = pd.DataFrame(columns=columnNames)
    while(startblock < endblock):
        result = query("account","txlist",address=MakersPlace3,
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
    """Read in all internal transactions from contract 3, and save as csv."""

    startblock = 9000000 #Block before start of Makers Place
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
