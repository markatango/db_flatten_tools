# MongoDB collection splitter and flattener.
# Splits collection based on a field
# Flattens documents in the collection

# usage: In CLI: 'python dbflatten.py'

# Specify database and collection to be flattened
# and modify the obvious other things like host, username, etc
# or modify program to accept them as arguments to main

from pymongo import MongoClient
from dbflatten import dbFlatten

DATABASE = "dbname"
COLLECTION = "collectionname"
FIELD = "fieldname"

# Globals
client = MongoClient('hostaddress:port', \
                        username='username', 
                        password='password', \
                        authsource='authdb', \
                        authMechanism='SCRAM-SHA-1' \
                        )

col = client[DATABASE][COLLECTION]

# Batches inserts in size 'batchSize' to conserve memory. 
# Probably there's an optimal batchSize number.
def createCollection(value, batchSize = 2000):
  global newdoc, col, client
  
  docs = col.find({ FIELD: value  })
  head_key = value
  counter = 0
  docContainer = []
  
  dbf = dbFlatten()
  
  while docs and docs.alive:
    while counter < batchSize and docs.alive:
        try:      
          doc = docs.next()
          newdoc = dbf.flattenDoc(doc, head_key)
          docContainer.append(newdoc)
          counter += 1
        except Exception as e:
          print("breaking...", e)
          break
    counter = 0
  
    # # Insert the batch in a new collection defined by model
    # I don't know... insert or insertMany?
    client[DATABASE][value].insert(docContainer)
    docContainer = []
    
# ==========================
# 
#  Main loop
#
if __name__ == "__main__":
  client = MongoClient('hostaddress:port', \
                        username='username', 
                        password='password', \
                        authsource='authdb', \
                        authMechanism='SCRAM-SHA-1' \
                        )
  
  #get models from the amya.simulated collection
  values = col.distinct("tags.equipment_model")
  for  v in values:
    print(v)
    createCollection(v);
    # examine a sample flat document
    print(str(client[DATABASE][v].find_one()))
