#file: analyseFoundation.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the Foundation gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/FoundationTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/FoundationInternalTransactions.csv")

"""For Foundation there were only records for primary sales. This is because
secondary sales are not handled by the known Foundation contracts, but instead
happen through OpenSea, a third party marketplace."""

def collectInternalTxs(df,dfInt):
    """Make a dictionary with as key the transaction hash, and as value a list
    of internal transactions that belong together."""

    out = dict()
    for index, row in df.iterrows():
        txHash = row[2]
        match = dfInt[dfInt["txHash"]==txHash]
        if (match.shape[0]==2):
            out[txHash] = [list(match.iloc[0]),list(match.iloc[1])]
    return out

def findFeesPrimary(txdict,df):
    """For all sales, compute the sale price and commission fees that went to
    each party involved. Store in a dataframe."""

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
            values.append(values[0] + values[1])
            values.append(v[0][1])
            out = out.append(pd.DataFrame([values],columns=cols),ignore_index=True)
    return out

def processFeesPrimary(values):
    """Compute percentage fees and store as csv"""

    values["galleryPerc"] = values.apply(lambda row: round(row["galleryFee"]*100/row["sale"],2),axis=1)
    values["artistPerc"] = values.apply(lambda row: round(row["artistFee"]*100/row["sale"],2),axis=1)
    values.to_csv("../Data/Processed/FoundationPrimary.csv",index=False)
