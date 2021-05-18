#file: visualizeFees.py
#author: Kamiel Fokkink

#This file serves to visualize the fees from all the galleries
#over time

import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

SuperRarePrim = pd.read_csv("../Data/Processed/SuperRarePrimary.csv")
SuperRareSec = pd.read_csv("../Data/Processed/SuperRareSecondary.csv")
AsyncArtPrim = pd.read_csv("../Data/Processed/AsyncArtPrimary.csv")
AsyncArtSec = pd.read_csv("../Data/Processed/AsyncArtSecondary.csv")
FoundationPrim = pd.read_csv("../Data/Processed/FoundationPrimary.csv")
MakersPlacePrim = pd.read_csv("../Data/Processed/MakersPlacePrimary.csv")
MakersPlaceSec = pd.read_csv("../Data/Processed/MakersPlaceSecondary.csv")

def plot(df,name):
    fig, ax = plt.subplots()
    dates = [datetime.fromtimestamp(t) for t in df["timestamp"]]
    g = ax.scatter(dates,df["galleryPerc"],c="blue",label="gallery%")
    a = ax.scatter(dates,df["artistPerc"],c="orange",label="artist%")
    ax.grid(True)
    plt.ylabel("Percentage of sale")
    plt.xticks(rotation=45)
    plt.legend(handles=[g,a],bbox_to_anchor=(1.05,1),loc="upper left")
    plt.title(name)
    
    rows=["gallery percentage","artist percentage","total observations",
          "most occurring division","observations in this pattern"]
    data = prepareTableData(df)
    tab = plt.table(data,rowLabels=rows,bbox=(0.3,-0.85,1,0.60))
    
    plt.show()
    
def prepareTableData(df):
    means = df.mean()
    gm = round(means["galleryPerc"],2)
    am = round(means["artistPerc"],2)
    std = df.std()
    gsd = round(std["galleryPerc"],2)
    asd = round(std["artistPerc"],2)
    total = df.shape[0]

    gCommon = round(df["galleryPerc"].value_counts().index[0],2)
    aCommon = round(df["artistPerc"].value_counts().index[0],2)
    gCount = df[abs(df["galleryPerc"]-gCommon)<1].shape[0]
    aCount = df[abs(df["artistPerc"]-aCommon)<1].shape[0]
    nCommon = min(gCount,aCount)
    nPerc = round(nCommon*100/total,2)
    
    out = [["mean: "+str(gm),"std: "+str(gsd)],["mean: "+str(am),"std: "+str(asd)],
           [total,""],["gallery: "+str(gCommon)+"%","artist: "+str(aCommon)+"%"],
           [nCommon,str(nPerc)+"% of total"]]
    return out

def prepareTableDataMP2(df):
    means = df.mean()
    gm = round(means["royaltyPerc"],2)
    am = round(means["sellerPerc"],2)
    std = df.std()
    gsd = round(std["royaltyPerc"],2)
    asd = round(std["sellerPerc"],2)
    total = df.shape[0]

    gCommon = round(df["royaltyPerc"].value_counts().index[0],2)
    aCommon = round(df["sellerPerc"].value_counts().index[0],2)
    gCount = df[abs(df["royaltyPerc"]-gCommon)<1].shape[0]
    aCount = df[abs(df["sellerPerc"]-aCommon)<1].shape[0]
    nCommon = min(gCount,aCount)
    nPerc = round(nCommon*100/total,2)
    
    out = [["mean: "+str(gm),"std: "+str(gsd)],["mean: "+str(am),"std: "+str(asd)],
           [total,""],["royalty: "+str(gCommon)+"%","seller: "+str(aCommon)+"%"],
           [nCommon,str(nPerc)+"% of total"]]
    return out
