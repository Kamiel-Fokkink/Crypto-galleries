#file: analyseMakersPlace.py
#author: Kamiel Fokkink

#This file serves to analyse the data and find results to answer the research
#question for collected data of the Makers Place gallery

import pandas as pd

df = pd.read_csv("../Data/Raw/MakersPlaceTransactions.csv")
dfInt = pd.read_csv("../Data/Raw/MakersPlaceInternalTransactions.csv")
        
"""Secondary sales also happen on the platform. One (old) blog says artists
receive a 5% royalty fee. A FAQ says that artists receive 10%, and galleries
2.5% of royalties."""

""""The collected data is a big chaos. Main issues: methodIDs are not specified;
at place bid people transfer money, which then either does or does not get
transferred back at random; sales can be triggered by different contract calls.
I filtered a LOT of transactions out, and included only those which are
most certainly sales. Of these, the percentages are stable. If more txs are
included, the percentages are alll over the place.
Also, for secondary transactions, it seems hard af. Almost every transaction
has only 1 internal tx related to it. Only a couple with 2, but these are all
reverted or error transactions. If secondary sales occur on this account, either
artists or galleries get no fee."""

def collectTxHashes(df):
    missed = 0
    txs = dict()
    for index,row in df.iterrows():
        method = row[13][:10]
        if (method=='0xaf8ee37f' or method=='0xae77c237' or method=='0xf9d83bb5'):
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
    primary = dict()
    secondary = dict()
    i = 0
    for k,v in txs.items():
        if (i%1000==0): print(i)
        i += 1
        saleCount = 0
        lastBid = '0x00'
        for txh in v:
            transaction = df[df["hash"]==txh]
            method = transaction["input"].values[0][:10]
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
    cols = ["galleryFee","artistFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    missed = 0
    i=0
    for k,v in txs.items():
        if (i%1000==0): print(i)
        i += 1
        try:
            if (len(v)==1):
                value = int(df[df["hash"]==v[0]]["value"].values[0])
                artist = int(dfInt[dfInt["hash"]==v[0]]["value"].values[0])
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
    
    out["galleryPerc"] = out.apply(lambda row: row["galleryFee"]*100/row["sale"],axis=1)
    out["artistPerc"] = out.apply(lambda row: row["artistFee"]*100/row["sale"],axis=1)
    out.to_csv("../Data/Processed/MakersPlacePrimary.csv")
    
def processSecondary(txs,df,dfInt):
    cols = ["royaltyFee","sellerFee","sale","timestamp"]
    out = pd.DataFrame(columns=cols)
    missed = 0
    i=0
    for k,v in txs.items():
        if (i%1000==0): print(i)
        i += 1
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
    
    out["royaltyPerc"] = out.apply(lambda row: row["royaltyFee"]*100/row["sale"],axis=1)
    out["sellerPerc"] = out.apply(lambda row: row["sellerFee"]*100/row["sale"],axis=1)
    out.to_csv("../Data/Processed/MakersPlaceSecondary.csv",index=False)
    
"""What happened was this: I collected all transaction hashes of the types 0xaf8ee37f,
0xae77c237, or 0xf9d83bb5. First one is accept sale, second is purchase, third
is place bid. Sales can be represented by 2 contract calls: purchase or accept
sale. The first one includes the value of the sale. The second needs to be used
in combination with bid, as the value is contained within the last placed bid.
These tx hashes are stored in a dictionary with keys the tokenIDs of the relevant
artwork. Then I split these txs into primary and secondary sales: for a tokenID
if a sale already happened, the next one will be a secondary. Finally, I calculate
the associated numbers to these sales, and write out a csv. A decent proportion
of transactions are incomplete, as they have no internal tx associated to them.
I left these out (50% of primary, and 30% of secondary)."""