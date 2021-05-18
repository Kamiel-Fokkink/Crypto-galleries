#file: analyseFoundation.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the Foundation gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/FoundationTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/FoundationInternalTransactions.csv")

def collectInternalTxs(df,dfInt):    
    out = dict()
    i = 0
    for index, row in df.iterrows():
        if (i%1000==0): print(i)
        i += 1
        txHash = row[2]
        match = dfInt[dfInt["txHash"]==txHash]
        if (match.shape[0]==2):
            out[txHash] = [list(match.iloc[0]),list(match.iloc[1])]
    return out

def findFeesPrimary(txdict,df):
    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    i = 0
    for k, v in txdict.items():
        if (i%500==0): print(i)
        i += 1
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
            values.append(values[0] + values[1])
            values.append(v[0][1])
            out = out.append(pd.DataFrame([values],columns=cols),ignore_index=True)    
    return out

def processFeesPrimary(values):
    values["galleryPerc"] = values.apply(lambda row: row["galleryFee"]*100/row["sale"],axis=1)
    values["artistPerc"] = values.apply(lambda row: row["artistFee"]*100/row["sale"],axis=1)
    values.to_csv("../Data/Processed/FoundationPrimary.csv",index=False)
    #return values