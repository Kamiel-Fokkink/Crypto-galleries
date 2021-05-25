#file: analyseAsyncArt.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the Async Art gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/AsyncArtTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/AsyncArtInternalTransactions.csv")

def collectInternalTxs(df,dfInt):
    """For all transactions, classify them into the primary or secondary market.
    Track the tokenIDs of artworks that have been sold. If a same artwork gets
    sold again, it will constitute a secondary sale. Return two dictionaries,
    one for each market, containing as keys the transaction hash of the sale, 
    and as values a list of internal transactions associated to it."""

    outPrim = dict()
    outSec = dict()
    soldIDs = set()
    for index, row in df.iterrows():
        txHash = row[2]
        match = dfInt[dfInt["txHash"]==txHash]
        tokenID = row[13][10:74]
        if (tokenID in soldIDs):
            outSec[txHash] = [list(match.iloc[i]) for i in range(match.shape[0])]
        elif (match.shape[0]==2 or match.shape[0]==3):
            outPrim[txHash] = [list(match.iloc[i]) for i in range(match.shape[0])]
            soldIDs.add(tokenID)
    return outPrim, outSec

def findFeesPrimary(txdict,df):
    """Find the values of the transactions associated to a primary sale. There
    is one gallery fee, and an artist fee. Some transactions of type acceptBid1 
    are slightly different, in the sense that they have 2 artists who both 
    receive half the fee."""

    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k, v in txdict.items():
        if (len(v) == 2):
            values = [int(df[df["hash"]==k]["value"])/(10**18)]
            for row in v:
                values.append(int(row[4])/(10**18))
            if (values[0]!=0):
                values = sorted(values)
                values.append(v[0][1])
                out = out.append(pd.DataFrame([values],columns=cols),ignore_index=True)
            else:
                values = sorted(values)[1:]
                values.append(values[0]+values[1])
                values.append(v[0][1])
                out = out.append(pd.DataFrame([values],columns=cols),ignore_index=True)
        elif(len(v)==3):
            if (v[0][12]=="acceptBid1"):
                values = []
                for row in v:
                    values.append(int(row[4])/(10**18))
                values = sorted(values)
                values.append(sum(values))
                values.append(v[0][1])
                values.pop(2)
                out = out.append(pd.DataFrame([values],columns=cols),ignore_index=True)
    return out

def processFeesPrimary(values):
    """Compute percentage fees and store as csv"""

    values["galleryPerc"] = values.apply(lambda row: round(row["galleryFee"]*100/row["sale"],2),axis=1)
    values["artistPerc"] = values.apply(lambda row: round(row["artistFee"]*100/row["sale"],2),axis=1)
    values.to_csv("../Data/Processed/AsyncArtPrimary.csv",index=False)

def findFeesSecondary(txdict,df):
    """Find the values of fees associated with secondary sales for gallery, 
    artist and seller."""

    cols = ["galleryFee","artistFee","sellerFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k,v in txdict.items():
        if len(v)==3:
            values = []
            for row in v:
                values.append(int(row[4])/(10**18))
            values.append(sum(values))
            values = sorted(values)
            values.append(v[0][1])
            out = out.append(pd.DataFrame([values],columns=cols),ignore_index=True)
    return out

def processFeesSecondary(values):
    """Compute percentage fees and store as csv"""

    values["galleryPerc"] = values.apply(lambda row: round(row["galleryFee"]*100/row["sale"],2),axis=1)
    values["artistPerc"] = values.apply(lambda row: round(row["artistFee"]*100/row["sale"],2),axis=1)
    values["sellerPerc"] = values.apply(lambda row: round(row["sellerFee"]*100/row["sale"],2),axis=1)
    values.to_csv("../Data/Processed/AsyncArtSecondary.csv",index=False)