#file: analyseSuperRare.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the SuperRare gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/SuperRareTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/SuperRareInternalTransactions.csv")

def collectInternalTxs(df,dfInt):
    """This function connects regular transactions that are associated to
    internal transactions together. It distinguishes between those that have
    2 or 3 internal transactions, as the latter concerns resales. It will 
    output a dictionary, with as keys the transaction hash of regular 
    transactions, and as values a list of lists containing details of the 
    internal transactions."""
    
    outPrim = dict()
    outSec = dict()
    for index, row in df.iterrows():
        txHash = row[2]
        match = dfInt[dfInt["txHash"]==txHash]
        if (match.shape[0]==2):
            outPrim[txHash] = [list(match.iloc[0]),list(match.iloc[1])]
        if (match.shape[0]>2):
            outSec[txHash] = [list(match.iloc[i]) for i in range(match.shape[0])]
    return outPrim, outSec

def findFeesPrimary(txdict,df):
    """For all primary sales, it checks whether their data corresponds to a
    valid transaction with a gallery fee and artist fee. Then stores the
    values in a dataframe."""
    
    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k, v in txdict.items():
        if (df[df["hash"]==k]["type"].values[0]=="Buy3"): continue
        #These transactions do not contain a gallery fee, but return a previously placed bid
        value = int(df[df["hash"]==k]["value"])
        values = [value/(10**18)]
        for row in v:
            values.append(int(row[4])/(10**18))
        
        if (values[0]!=0):
            values = sorted(values)
            values.append(v[0][1])
            out = out.append(pd.DataFrame([values],columns=cols),
                             ignore_index=True)
        else:
            values = sorted(values)[1:]
            values.append(values[0] + values[1])
            values.append(v[0][1])
            out = out.append(pd.DataFrame([values],columns=cols),
                             ignore_index=True)    
    return out

def processFeesPrimary(values):
    values["galleryPerc"] = values.apply(lambda row: row["galleryFee"]*100/row["sale"],axis=1)
    values["artistPerc"] = values.apply(lambda row: row["artistFee"]*100/row["sale"],axis=1)
    values.to_csv("../Data/Processed/SuperRarePrimary.csv",index=False)
    #return values

def findFeesSecondary(txdict,df):
    """For all secondary sales, it checks whether their data corresponds to a
    valid transaction with a gallery fee and artist fee. Distinguish cases
    with 3 or 6 internal transactions. Here the second option corresponds to
    3 pairs of 2 transactions, where money is handled in two steps through a
    middle contract, in the end reaching 3 receivers. Stores the amounts
    of transferred money in a dataframe."""
    
    cols = ["galleryFee","artistFee","sellerFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k,v in txdict.items():
        txVal = int(df[df["hash"]==k]["value"].values[0])/(10**18)
        if len(v)==3:
            values = []
            receivers = set()
            for row in v:
                values.append(int(row[4])/(10**18))
                receivers.add(row[3])
            if (txVal > 0 and txVal < sum(values)):
                #These transactions return a previously placed bid
                continue
            values.append(sum(values))
            series = sorted(values)
            if (not len(receivers)==3):
                #These transactions transfer money to SuperRare multiple times
                continue
            series.append(v[0][1])
            out = out.append(pd.DataFrame([series],columns=cols),ignore_index=True)
        elif len(v)==6:
            values = set()
            receivers = set()
            for row in v:
                values.add(int(row[4])/(10**18))
                receivers.add(row[3])
            if (txVal > 0 and txVal < sum(values)):
                #These transactions return a previously placed bid
                continue
            values.add(sum(values))
            series = sorted(list(values))
            if (not len(receivers)==4):
                #These transactions transfer money to SuperRare multiple times
                continue
            series.append(v[0][1])
            out = out.append(pd.DataFrame([series],columns=cols),ignore_index=True)
        else:
            continue
    return out

def processFeesSecondary(values):
    values["galleryPerc"] = values.apply(lambda row: row["galleryFee"]*100/row["sale"],axis=1)
    values["artistPerc"] = values.apply(lambda row: row["artistFee"]*100/row["sale"],axis=1)
    values["sellerPerc"] = values.apply(lambda row: row["sellerFee"]*100/row["sale"],axis=1)
    values.to_csv("../Data/Processed/SuperRareSecondary.csv",index=False)
    #return values

def countOutliers(df):
    outliersG = 0
    outliersA = 0
    outliersS = 0
    means = feesSec.mean()
    meanG = means["galleryPerc"]
    meanA = means["artistPerc"]
    meanS = means["sellerPerc"]
    stds = feesSec.std()
    stdG = stds["galleryPerc"]
    stdA = stds["artistPerc"]
    stdS = stds["sellerPerc"]
    for index, row in df.iterrows():
        if ((row["galleryPerc"]-meanG)>stdG):
            outliersG += 1
        if ((row["artistPerc"]-meanA)>stdA):
            outliersA += 1
        if ((row["sellerPerc"]-meanS)>stdS):
            outliersS += 1
    out = {"G":outliersG,"A":outliersA,"S":outliersS}
    return out, timestamps        