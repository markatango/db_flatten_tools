# Database splitter and flattener.
# splits based on 'tags.equipment_model'
# flattens arrays and objects

# usage: In CLI: 'python dbflatten.py'

# Specify database and collection to be flattened

from pymongo import MongoClient
from dbflatten import dbFlatten

DATABASE = "amya"
COLLECTION = "simulated"

# Globals
client = MongoClient('amya.io:8002', \
                        username='webuser', 
                        password='webuser', \
                        authsource='amya', \
                        authMechanism='SCRAM-SHA-1' \
                        )

col = client[DATABASE][COLLECTION]

# Batches inserts in size 'batchSize' to conserve memory. 
# Probably there's an optimal batchSize number.
def createCollection(model, batchSize = 20):
  global newdoc, col, client
  
  docs = col.find({ "tags.equipment_model" : model  }).limit(50)
  head_key = model
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
    client[DATABASE][model].insert(docContainer)
    docContainer = []
    
# ==========================
# 
#  Main loop
#
if __name__ == "__main__":
  client = MongoClient('amya.io:8002', \
                        username='webuser', 
                        password='webuser', \
                        authsource='amya', \
                        authMechanism='SCRAM-SHA-1' \
                        )
  
  #get models from the amya.simulated collection
  models = col.distinct("tags.equipment_model")
  for  m in models:
    print(m)
    createCollection(m);
    # examine a sample flat document
    print(str(client[DATABASE][m].find_one()))
