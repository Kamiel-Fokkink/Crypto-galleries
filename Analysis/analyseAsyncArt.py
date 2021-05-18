#file: analyseAsyncArt.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the Async Art gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/AsyncArtTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/AsyncArtInternalTransactions.csv")

def collectInternalTxs(df,dfInt):    
    """For all transactions, classify them into the primary or secondary market
    based on the amount of internal transactions. One with 2 internal transactions
    corresponds to a primary sale, more is a secondary. Also record the tokenIDs
    of artworks when they get sold, to check which tokens are in the secondary
    market."""
    
    outPrim = dict()
    outSec = dict()
    soldIDs = set()
    for index, row in df.iterrows():
        txHash = row[2]
        match = dfInt[dfInt["txHash"]==txHash]
        if (match.shape[0]==2):
            outPrim[txHash] = [list(match.iloc[0]),list(match.iloc[1])]
            soldIDs.add(row[13][10:74])
        if (match.shape[0]>2):
            outSec[txHash] = [list(match.iloc[i]) for i in range(match.shape[0])]
    return outPrim, outSec, soldIDs

def findFeesPrimary(txdict,df):
    """Find the values of the transactions associated to a primary sale. There
    is one gallery fee, and an artist fee."""
    
    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k, v in txdict.items():
        value = int(df[df["hash"]==k]["value"])
        values = [value/(10**18)]
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
    return out

def processFeesPrimary(values):
    values["galleryPerc"] = values.apply(lambda row: row["galleryFee"]*100/row["sale"],axis=1)
    values["artistPerc"] = values.apply(lambda row: row["artistFee"]*100/row["sale"],axis=1)
    values.to_csv("../Data/Processed/AsyncArtPrimary.csv",index=False)
    #return values
    
def findFeesSecondary(txdict,df,soldIDs):
    """Find the values of fees associated with secondary sales. First check
    whether a sale is indeed on the secondary market, then extract fees."""
    
    cols = ["galleryFee","artistFee","sellerFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k,v in txdict.items():
        if (not (df[df["hash"]==v[0][13]]["input"].values[0][10:74] in soldIDs)): 
        #Remove transactions whose tokenID has not been sold yet, and thus are 
        #wrongly classified as secondary
            continue
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
    values["galleryPerc"] = values.apply(lambda row: row["galleryFee"]*100/row["sale"],axis=1)
    values["artistPerc"] = values.apply(lambda row: row["artistFee"]*100/row["sale"],axis=1)
    values["sellerPerc"] = values.apply(lambda row: row["sellerFee"]*100/row["sale"],axis=1)
    values.to_csv("../Data/Processed/AsyncArtSecondary.csv",index=False)