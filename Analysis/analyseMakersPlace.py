#file: analyseMakersPlace.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the Makers Place gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/MakersPlaceTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/MakersPlaceInternalTransactions.csv")

"""The collected data for Makers Place was considerably more chaotic than other
galleries. The main issues are: methodIDs are not specified, so transaction types
are unknown, and a sale can be triggered by multiple different contract calls.
Sometimes money is already transferred in a place bid transaction, where next an
accept bid transaction transfers the token, and distributes the money to the 
gallery and artist through internal transactions. These multiple transactions
associated to a same sale, are linked together by their tokenID, an input field.
A challenge that follows from here is to characterise a sale on whether it is
on the primary or secondary market. This was done by remembering the tokenIDs
that have been already sold, and if a sale happens again it must be the secondary
market. However, this approach was not perfect, as some sales appear to be
classified into the wrong market. Still, it is clear enough to determine the 
percentage fees."""

RelevantTransactionTypes = ['0xaf8ee37f','0xae77c237','0xf9d83bb5','0x7c2aca4a']

def collectTxHashes(df):
    """Collect all transaction hashes that are associated to a certain token.
    Output a dictionary with as keys tokenIDs and values a list of transaction
    hashes. Only relevant transaction types are considered. Here it is difficult
    to be certain which transaction types should are related to sales, but the 
    ones included are likely to be so."""
    
    missed = 0
    txs = dict()
    df = df[df["isError"]==0]
    for index,row in df.iterrows():
        method = row[13][:10]
        if (method in RelevantTransactionTypes):
            tokenID = row[13][10:74]
            if (tokenID in txs):
                txs[tokenID].append(row[2])
            else:
                txs[tokenID] = [row[2]]
        else:
            missed += 1
    print("Missed",missed,"transactions")
    
    return txs

def splitMarkets(txs,df):
    """Determine whether a sale was part of the primary or secondary market. 
    Check whether a token has been sold already, if so it is secondary. Some
    secondary sales get wrongly put into the primary market. This happens
    because not every true sale is part of the data, as most transaction types 
    are unknown, and some sales occured through transactions that were not 
    considered.
    '0xae77c237' is purchase, sale price is transferred and fees distributed.
    '0xf9d83bb5' is place bid, already transfers an amount of money.
    '0xaf8ee37f' is accept bid, only distributes fees internally, represents a
    sale in combination with place bid.
    '0x7c2aca4a' is a token transfer, without any money changing hands."""
    
    primary = dict()
    secondary = dict()
    for k,v in txs.items():
        saleCount = 0
        lastBid = '0x00'
        for txh in v:
            transaction = df[df["hash"]==txh]
            method = transaction["input"].values[0][:10]
            if (method=="0x7c2aca4a"):
                saleCount += 1
            if (method=="0xae77c237"):
                if (saleCount == 0):
                    primary[k] = [txh]
                else:
                    if (txh in secondary):
                        secondary[k].append(txh)
                    else:
                        secondary[k] = [txh]
                saleCount += 1
            elif (method=="0xf9d83bb5"):
                lastBid = txh                
            elif (method=="0xaf8ee37f"):
                if (saleCount==0):
                    primary[k] = [txh]
                    primary[k].append(lastBid)
                elif (saleCount>0):
                    if (txh in secondary):
                        secondary[k].append(txh)
                    else:
                        secondary[k] = [txh]
                    secondary[k].append(lastBid)
                saleCount += 1
                
    return primary, secondary
    
def processPrimary(txs,df,dfInt):
    """Find the values of the ETH transferred between parties for primary sales.
    Handles multiple possible cases for sales consisting of different 
    transaction types. Compute the percentage fees and save as csv. Around
    50% of transactions are incomplete, in the sense that there is not enough
    information to fill in all the financial values."""
    
    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    missed = 0
    for k,v in txs.items():
        try:
            if (len(v)==1):
                internalTxs = dfInt[dfInt["hash"]==v[0]]
                if (len(internalTxs)>1):
                    continue
                value = int(df[df["hash"]==v[0]]["value"].values[0])
                artist = int(internalTxs["value"].values[0])
                time = df[df["hash"]==v[0]]["timeStamp"].values[0]
            else:
                value = int(df[df["hash"]==v[1]]["value"].values[0])
                artist = int(dfInt[dfInt["hash"]==v[0]]["value"].values[0])
                time = dfInt[dfInt["hash"]==v[0]]["timeStamp"].values[0]
            gallery = value - artist
            data = [gallery/(10**18),artist/(10**18),value/(10**18),time]
            if (artist<value):
                out = out.append(pd.DataFrame([data],columns=cols),ignore_index=True)
            else:
                missed += 1
        except IndexError:
            missed += 1
            continue
    print("Missed",missed,"incomplete transactions")
    
    out["galleryPerc"] = out.apply(lambda row: round(row["galleryFee"]*100/row["sale"],2),axis=1)
    out["artistPerc"] = out.apply(lambda row: round(row["artistFee"]*100/row["sale"],2),axis=1)
    out.to_csv("../Data/Processed/MakersPlacePrimary.csv",index=False)
    
def processSecondary(txs,df,dfInt):
    """Find the values of the ETH transferred between parties for secondary
    sales. Handles multiple possible cases for sales consisting of different 
    transaction types. Compute the percentage fees and save as csv. Around 30%
    of transactions are incomplete."""
    
    cols = ["royaltyFee","sellerFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    missed = 0
    for k,v in txs.items():
        try:
            if (len(v)==1):
                value = int(df[df["hash"]==v[0]]["value"].values[0])
                seller = int(dfInt[dfInt["hash"]==v[0]]["value"].values[0])
                time = df[df["hash"]==v[0]]["timeStamp"].values[0]
            else:
                value = int(df[df["hash"]==v[1]]["value"].values[0])
                seller = int(dfInt[dfInt["hash"]==v[0]]["value"].values[0])
                time = dfInt[dfInt["hash"]==v[0]]["timeStamp"].values[0]
            royalty = value - seller
            data = [royalty/(10**18),seller/(10**18),value/(10**18),time]
            if (seller<value):
                out = out.append(pd.DataFrame([data],columns=cols),ignore_index=True)
            else:
                missed += 1
        except IndexError:
            missed += 1
            continue
    print("Missed",missed,"incomplete transactions")
    
    out["royaltyPerc"] = out.apply(lambda row: round(row["royaltyFee"]*100/row["sale"],2),axis=1)
    out["sellerPerc"] = out.apply(lambda row: round(row["sellerFee"]*100/row["sale"],2),axis=1)
    out.to_csv("../Data/Processed/MakersPlaceSecondary.csv",index=False)