# Flattens structured JSON

class dbFlatten:
  def __init__(self):
    pass
   
  # recursive document flattener
  def _parseDocKeys (self, doc, key):

    if isinstance(doc, list):
      for k,v in enumerate(doc):
        nextkey = key+'_'+ str(k)
        nextdoc = v
        if nextkey == 'timestamp':
          self.newdoc[nextkey] = nextdoc
        elif isinstance(nextdoc, (dict,list)):
          self._parseDocKeys(nextdoc, nextkey)
        else: 
          self.newdoc[nextkey] = nextdoc

    elif isinstance(doc, dict):
      for k,v in doc.items():
        nextkey = key+'_'+ str(k)
        nextdoc = v
        if nextkey == 'timestamp':
          self.newdoc[nextkey] = nextdoc
        elif isinstance(nextdoc, (dict,list)):
          self._parseDocKeys(nextdoc, nextkey)
        else: 
          self.newdoc[nextkey] = nextdoc

    return self.newdoc
    
  def flattenDoc(self, doc, head_key):
    self.newdoc = {}
    return self._parseDocKeys(doc, head_key)
    
#==============================================================
if __name__ == "__main__":
  dbf = dbFlatten()
  doc = {'k1':1, 'k2':[3, 4], 'k5':'5', 'k6':{'k7':7,'k8':8}}
  newdoc = dbf.flattenDoc(doc, "start")
  print(str(newdoc))
  # {'start_k1': 1, 'start_k2_0': 3, 'start_k2_1': 4, 'start_k5': '5', 'start_k6_k7': 7, 'start_k6_k8': 8}