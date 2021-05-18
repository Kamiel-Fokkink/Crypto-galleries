#file: makeTables.py
#author: Kamiel Fokkink

#This file serves to summarize the fees for each gallery into a table

import pandas as pd
import dataframe_image as dfi

def makeTable():
    header = pd.MultiIndex.from_tuples([("Primary market","Gallery %"),("Primary market",
            "Artist %"),("Secondary market","Gallery %"),("Secondary market","Artist %")])
    out = pd.DataFrame(columns=header)
    out = out.append(pd.DataFrame([[17.5,82.5,2.9,9.7]],index=["SuperRare"],columns=header))
    out = out.append(pd.DataFrame([[10,90,1,10]],index=["Async Art"],columns=header))
    out = out.append(pd.DataFrame([[15,85,12.5,"-"]],index=["Makers Place"],columns=header))
    out = out.append(pd.DataFrame([[15,85,"-","-"]],index=["Foundation"],columns=header))
    
    dfi.export(out,"Table.png")