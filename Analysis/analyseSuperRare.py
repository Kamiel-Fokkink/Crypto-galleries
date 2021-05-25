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
    2 or 3 internal transactions, as the latter concerns resales. Also for 
    transaction type Buy3, only one internal transaction happens, as here 
    SuperRare does not yet take a fee. The output is a dictionary, with as keys
    the transaction hash of the sale, and as values a list of lists containing 
    details of the internal transactions."""

    outPrim = dict()
    outSec = dict()
    df = df[df["isError"]==0]
    for index, row in df.iterrows():
        txHash = row[2]
        match = dfInt[dfInt["txHash"]==txHash]
        contract3 = row[18]=="Buy3"
        if (match.shape[0]==2):
            outPrim[txHash] = [list(match.iloc[0]),list(match.iloc[1])]
        if (match.shape[0]==1 and contract3):
            outPrim[txHash] = [list(match.iloc[0])]
        if (match.shape[0]>2):
            outSec[txHash] = [list(match.iloc[i]) for i in range(match.shape[0])]
    return outPrim, outSec

def findFeesPrimary(txdict,df):
    """For all primary sales, it checks whether their data corresponds to a
    valid transaction. Then stores the values in a dataframe."""

    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    for k, v in txdict.items():
        if (df[df["hash"]==k]["type"].values[0]=="Buy3"):
        #These transactions do not contain a gallery fee, but return a previously placed bid    
            value = int(df[df["hash"]==k]["value"])
            time = v[0][1]
            out = out.append(pd.DataFrame([[0,value,value,time]],columns=cols),
                                          ignore_index=True)
            continue
        
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
    """Compute percentage fees and store as csv"""

    values["galleryPerc"] = values.apply(lambda row: round(row["galleryFee"]*100/row["sale"],2),axis=1)
    values["artistPerc"] = values.apply(lambda row: round(row["artistFee"]*100/row["sale"],2),axis=1)
    values.to_csv("../Data/Processed/SuperRarePrimary.csv",index=False)

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
            if (txVal > 0 and txVal < sum(values)-0.00000001):
                #These transactions return a previously placed bid
                continue
            if (not len(receivers)==3):
                #These transactions transfer money to SuperRare multiple times
                continue
            values.append(sum(values))
            series = sorted(values)
            series.append(v[0][1])
            out = out.append(pd.DataFrame([series],columns=cols),ignore_index=True)
        elif len(v)==6:
            values = set()
            receivers = set()
            for row in v:
                values.add(int(row[4])/(10**18))
                receivers.add(row[3])
            if (txVal > 0 and txVal < sum(values)-0.00000001):
                #These transactions return a previously placed bid
                continue
            if (not len(receivers)==4):
                #These transactions transfer money to SuperRare multiple times
                continue
            values.add(sum(values))
            series = sorted(list(values))
            series.append(v[0][1])
            out = out.append(pd.DataFrame([series],columns=cols),ignore_index=True)
        else:
            continue
    return out

def processFeesSecondary(values):
    """Compute percentage fees and store as csv"""

    values["galleryPerc"] = values.apply(lambda row: round(row["galleryFee"]*100/row["sale"],2),axis=1)
    values["artistPerc"] = values.apply(lambda row: round(row["artistFee"]*100/row["sale"],2),axis=1)
    values["sellerPerc"] = values.apply(lambda row: round(row["sellerFee"]*100/row["sale"],2),axis=1)
    values.to_csv("../Data/Processed/SuperRareSecondary.csv",index=False)