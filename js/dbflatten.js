// Database splitter and flattener.
// splits based on 'tags.equipment_model'
// flattens arrays and objects

// usage: In CLI: 'mongo amya -u webuser -p webuser < dbflatten.js'

// Specify database and collection to be flattened

use amya
col = db.simulated

// Batches inserts in size 'batchSize' to conserve memory. 
// Probably there's an optimal batchSize number.
function createCollection(model, batchSize = 20000){
	
	// recursive document flattener
	var newdoc = {}
	function parseDocKeys(doc, key){
		let keys = Object.keys(doc)
		for (let k in keys){
			let nextkey = key+'_'+keys[k]
			let nextdoc = doc[keys[k]] 
			if (keys[k] == 'timestamp'){
				newdoc[nextkey] = nextdoc
			} else if (nextdoc instanceof Object) {
				parseDocKeys(nextdoc, nextkey)
			} else {
				newdoc[nextkey] = nextdoc
			}
		}
		return
	};
	
	// get all docs for model
	var docs = col.find({"tags.equipment_model": {$eq : model}})  
	
	// Create the collection, batch by batch
    var head_key = model
	var counter = 0
	var docContainer = []
	
	while (docs.hasNext()){
		while ((counter < batchSize) && (docs.hasNext()) ) {
			counter += 1
			doc = docs.next()
			parseDocKeys(doc, head_key)
			docContainer.push(newdoc)
			newdoc = {}
		}
		counter = 0
	}
  // Insert the batch in a new collection defined by model
  // I don't know... insert or insertMany?
  db[model].insert(docContainer)
  docContainer = []
}

// ==========================
// 
//  Main loop
//

//get models from the amya.simulated collection
var models = col.distinct("tags.equipment_model")
printjson(models)
modelIterator = models.entries()
for (let m of modelIterator){
	let model = m[1]
	print(model)
	createCollection(model);
		
	// examine a sample flat document
	printjson(db[model].findOne())
}