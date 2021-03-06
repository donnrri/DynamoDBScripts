/*
This approach uses an asynchronous iteration by async generators
for a review see : https://2ality.com/2019/11/nodejs-streams-async-iteration.html

Be aware generators are composed and transform the stream data iteratively (line by line)
Finally an async functionis called to run a recursive 'write to database' function.

async function* myGenerator(iterable){..} // genrator
async fynction myAsyncFunc(data){...}

Some generic functions were abstracted to the utils.js
Most util functions are curried for ease of use.
*/

var AWS = require('aws-sdk');
const { Readable } = require('stream');
const {
    compose,
    hasKey,
    sleep,
    upperCaseTypeAttribute
} = require ('./utils')

AWS.config.update({region: 'us-east-1'});

require('events').EventEmitter.defaultMaxListeners = 250;

const S3 = new AWS.S3({apiVersion: '2006-03-01'});

const dynamoClient = new AWS.DynamoDB({
    apiVersion: '2012-08-10', 
    maxRetries:15,
    httpOptions: {
        timeout: 300000
      }
});

const tableName = process.env.BUCKET

const BATCH_LIMIT = 24

let params = {
    RequestItems: {[tableName]: []},
    ReturnConsumedCapacity: "TOTAL"
};

let hasUnProcessKey = hasKey('Unprocessed')

let hasUnProcessedItems = (tableName, obj) => hasUnProcessKey(obj) && obj.UnprocessedItems[tableName] && obj.UnprocessedItems[tableName].length > 0


let count = {lines: 0, batches: 0, throttles: 0, retryCount:0};
let pause = 0

function setPause(){

    let {retryCount} = count
    retryCount++

    let delay =  (retryCount*retryCount*100);
    let jitter =  Math.ceil(Math.random()*50);

    if(delay > 3000) // Cap wait time 
    {
        delay = 3000;
        jitter = jitter*3;
    }

    if(count.retryCount > 15) {
        chunks.destroy({message:"DynamoDB batchWriteItem retries exhausted"})
    }else{
         pause = delay + jitter 
    }    
}


const invalidItem = (item) => {
    let vals = Object.values(item)
   
    return vals.filter(o => Object.keys(o).includes("{ nULLValue: true }")).length > 0
}

async function* streamToLinesGenerator(chunkIterable) {
    let previous = '';
    try{
        for await (const chunk of chunkIterable) {
            previous += chunk;
            while (true) {
                const eolIndex = previous.indexOf('\n');
                if (eolIndex < 0) {
                    break;
                }
                // line includes the EOL
                const line = previous.slice(0, eolIndex+1);
                yield line;
                previous = previous.slice(eolIndex+1);
            }
          }
          if (previous.length > 0) {
            yield previous;
          }

    }catch(e){
        console.log(e)
    }
  }

async function* formatLinesGenerator(lineIterable) {
    let lineNumber = 1;
        try{
            for await (var rawItem of lineIterable) {
                let item = JSON.parse(upperCaseTypeAttribute(rawItem))
                console.log( " invalid  ---  " + invalidItem(item))
                if(!invalidItem(item))  {
                    params.RequestItems[tableName].push({"PutRequest": { "Item":item}})
                }else{
                    if(lineNumber > 0) lineNumber--
                }
                if(lineNumber === BATCH_LIMIT ){
                    yield params
                    params.RequestItems[tableName] = []
                    count.batchs++
                    lineNumber = 1

                }
                
                lineNumber++
                count.rows++
            }

        }catch(e){
            console.log(e)
        }
    }

  
async function batchWrieToDB(lineIterable) {

    for await (let params of lineIterable) {
        
        try{
             await sleep(pause)
             await loadData(params)
          
        }catch(e){
            console.log('Error writing to db ' + e.code)
             sleep(90000000)
            if(e.code == "ProvisionedThroughputExceededException" || e.code == "ThrottlingException") {
                setPause()
            }
        }
    }
}


function loadData(params){

    return dynamoClient.batchWriteItem(params).promise()
        .then((resp) => {
            if(hasUnProcessedItems(tableName, resp)){
                setPause()
                params.RequestItems[tableName] = resp.UnprocessedItems[tableName];
                loadData(params)
            }else{
                return resp
            }
            
        })
        .catch(function(e) {
            console.log(e)
            return e
        })
}

var s3ReadStream = S3.getObject({
    Bucket: process.env.BUCKET, 
    Key: process.env.Key
 }).createReadStream();

  const chunks = Readable.from(s3ReadStream);
  compose(batchWrieToDB, formatLinesGenerator ,streamToLinesGenerator)(chunks)

  chunks.on('error', (e) => { console.log('Error', e.message) })
  chunks.on('close', () =>  { console.log(`Stream closed: batches process = ${count.batches} `) })
  chunks.on('finish', () => { console.log(`Stream finished: batches process = ${count.batches} `) })
  chunks.on('end', (e) => console.log(`End of stream. ${e}`))
