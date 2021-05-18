#file: query.py
#author: Kamiel Fokkink

#This file contains general functions to query information
#from etherscan. It is used by other mining files

import json
import requests

MyApiKey = "2KZQ7U24SBQZ2FP9T4A6P3X5A74UQRY8D1"

columnNames=["blockNumber","timeStamp","hash","nonce","blockHash",
             "transactionIndex","from","to","value","gas","gasPrice","isError",
             "txreceipt_status","input","contractAddress","cumulativeGasUsed",
             "gasUsed","confirmations"]

internalColumns=["blockNumber","timeStamp","from","to","value","contractAddress",
                 "input","type","gas","gasUsed","isError","errCode"]

def query(module, action, address="", tags="",startblock="",endblock="",
          page="",offset="",txhash=""):
    url = "https://api.etherscan.io/api?"
    url += "module=" + module
    url += "&action=" + action
    if (address!=""): url += "&address=" + address
    if (tags!=""): url += "&tags=" + tags
    if (startblock!=""): url += "&startblock=" + startblock
    if (endblock!=""): url += "&endblock=" + endblock
    if (page!=""): url += "&page=" + page
    if (offset!=""): url += "&offset=" + offset
    if (txhash!=""): url += "&txhash=" + txhash
    url += "&apikey=" + MyApiKey
    #print(url)
    req = requests.get(url)
    dic = json.loads(req.text) #Convert string into nested dictionaries
    return dic

def uniqueMethodIDs(df):
    """Given a dataframe of transactions, this function finds the different
    methodIDs that are present among the transaction. It is used as a helper 
    to determine the kinds of transaction that the contracts handle. Through
    manually looking the transactions up on etherscan, the methods can be found."""
    ids = set()
    i = 0
    for index, row in df.iterrows():
        method = row[13][0:10]
        if (method in ids):
            continue
        else:
            ids.add(method)
            print(str(i) + ": " + row[2])
            i += 1
    return ids