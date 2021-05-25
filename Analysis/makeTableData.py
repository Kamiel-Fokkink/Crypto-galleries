#file: makeTableData.py
#author: Kamiel Fokkink

#This file serves to extract the most occurring percentages of fees from the
#processed data, and compute data to present in a table.

import pandas as pd
from datetime import datetime

SuperRarePrim = pd.read_csv("../Data/Processed/SuperRarePrimary.csv")
SuperRareSec = pd.read_csv("../Data/Processed/SuperRareSecondary.csv")
AsyncArtPrim = pd.read_csv("../Data/Processed/AsyncArtPrimary.csv")
AsyncArtSec = pd.read_csv("../Data/Processed/AsyncArtSecondary.csv")
FoundationPrim = pd.read_csv("../Data/Processed/FoundationPrimary.csv")
MakersPlacePrim = pd.read_csv("../Data/Processed/MakersPlacePrimary.csv")
MakersPlaceSec = pd.read_csv("../Data/Processed/MakersPlaceSecondary.csv")
    
def getTableDataPrimary(df):
    """Extract all occurrences of patterns of distribution between gallery and
    artist. Discard infrequent ones, as they might be errors or irregular
    transactions not associated to a sale. Count how often a pattern occurs,
    and check whether it is present during the whole duration of the gallery
    or only a certain time period. This found information is used in the result
    tables in the paper."""

    patterns = df.value_counts(subset=["galleryPerc","artistPerc"])
    cols = ["galleryPerc","artistPerc","transactions"]
    out = pd.DataFrame(columns=cols)
    for pattern in range(patterns.shape[0]):
        transactions = patterns.iloc[pattern]
        if (transactions>5):
            #Only consider patterns occurring sufficient times to be significant
            gallery = patterns.index[pattern][0]
            artist = patterns.index[pattern][1]
            out = out.append(pd.DataFrame([[gallery,artist,transactions]],
                                          columns=cols),ignore_index=True)
    out["first"] = out.apply(lambda row: datetime.fromtimestamp(df[df["artistPerc"]==row[1]].min()[3]), axis=1)
    out["last"] = out.apply(lambda row: datetime.fromtimestamp(df[df["artistPerc"]==row[1]].max()[3]), axis=1)
    return out

def getTableDataSecondary(df,MP):
    """Same as above, find frequently occurring payment patterns from data, only
    this time for the secondary market. A boolean input MP indicates whether
    it concerns MakersPlace. As this dataset does not contain a separate gallery
    or artist fees on secondary sales, but only a royalty fee, it needs to be
    handled differently."""
    
    if (not MP): 
        patterns = df.value_counts(subset=["galleryPerc","artistPerc","sellerPerc"])
        cols = ["galleryPerc","artistPerc","sellerPerc","transactions"]
    else:
        patterns = df.value_counts(subset=["royaltyPerc","sellerPerc"])
        cols = ["royaltyPerc","sellerPerc","transactions"]
    out = pd.DataFrame(columns=cols)
    for pattern in range(patterns.shape[0]):
        transactions = patterns.iloc[pattern]
        if (transactions>5):
            #Only consider patterns occurring sufficient times to be significant
            gallery = patterns.index[pattern][0]
            artist = patterns.index[pattern][1]
            if (not MP):
                seller = patterns.index[pattern][2]
                out = out.append(pd.DataFrame([[gallery,artist,seller,transactions]],
                                          columns=cols),ignore_index=True)
            else:
                out = out.append(pd.DataFrame([[gallery,artist,transactions]],
                                          columns=cols),ignore_index=True)
    if (not MP):
        out["first"] = out.apply(lambda row: datetime.fromtimestamp(df[df["sellerPerc"]==row[2]].min()[4]), axis=1)
        out["last"] = out.apply(lambda row: datetime.fromtimestamp(df[df["sellerPerc"]==row[2]].max()[4]), axis=1)
    else:
        out["first"] = out.apply(lambda row: datetime.fromtimestamp(df[df["sellerPerc"]==row[1]].min()[3]), axis=1)
        out["last"] = out.apply(lambda row: datetime.fromtimestamp(df[df["sellerPerc"]==row[1]].max()[3]), axis=1)    
    return out