
var AWS = require('aws-sdk');
const { Readable } = require('stream');
const {
    asynccompose,
    compose,
    concatStreams,
    curry,
    hasKey,
    sleep,
    upperCaseTypeAttribute
} = require ('./utils')

AWS.config.update({region: process.env.REGION});

require('events').EventEmitter.defaultMaxListeners = 250;

const S3 = new AWS.S3({apiVersion: '2006-03-01'});

const dynamoClient = new AWS.DynamoDB({apiVersion: '2012-08-10', maxRetries:15});

const tableName = process.env.TABLE_NAME

const BATCH_LIMIT = 24

let params = {
    RequestItems: {[tableName]: []},
    ReturnConsumedCapacity: "TOTAL"
};


const MAINFEST_REGEX = new RegExp('manifest', 'g')

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

const hasInvalidKey = hasKey('nullValue')

const invalidItem = (item) => Object.values(item).some( obj => hasInvalidKey(obj))


const getS3BucketList = async(params)  => await S3.listObjects(params).promise()

const getS3BucketFile = async(params) => await S3.getObject(params).promise()

const filterForManifest = ({Key}) => MAINFEST_REGEX.test(Key)

async function getManifestFile(params) {

    try{
        let list = await getS3BucketList(params)
        const {Key}  = list.Contents.filter(filterForManifest)[0]
        let file =  await getS3BucketFile({Bucket:params.Bucket, Key})
        return file.Body.toString('utf-8')
    }catch(e){
        console.log(e)
    }

}


function getFilePaths(manifestStr){
    return JSON.parse(manifestStr).entries
}

function getFileStreams(params){
    return (list) => {
        let promises = list.map(async({Key}) =>{
           params.Key = Key
           return  await getObjectStream(params)
       })

       return Promise.all(promises)
   }
}

let makeFileStreams = getFileStreams(myparams)

const getS3FileList = async(bucketName, regEx, manifestStr) =>{

    let filePaths = getFilePaths(manifestStr)
    
    try{
       return filePaths.map(async ({url}) => {
            // I know this is awful but aws doesnt help here at all
            let removed  = url.replace(regEx, '')
            let first = removed.split('/').shift()
            let Key  = removed.replace(first+ '/', '')
            let params = {
                Bucket:bucketName,
                Key
            }
           return  await S3.getObject(params).createReadStream();
        });

    }catch(e){
        console.log(e)
    }
}

function filterFiles(fn) {
    return (files) => files.filter(fn)
}


const filterMultipartFiles = filterFiles((t)=>{
    return t.Key != null && t.Key.includes("/data/")
} )

const filterDataFile = filterFiles(t => t.Key != null && t.Key.includes("Success").filter( t => t.Key != null && t.Key.includes("manifest")))

function filterExportedDataFiles(files){
    let isMultipart = files.filter(filterMultipartFiles)
    if(isMultipart.length > 0){
        return isMultipart
    }else{
        return files.filter(filterDataFile)
    }
}

async function* streamToLinesGenerator(chunkIterable) {
    let previous = '';
    try{
        for await (const chunk of chunkIterable) {
            previous += chunk;
            while (true) {
                const eolIndex = previous.indexOf('\n');
                if (eolIndex < 0) {
                    chunks.destroy({message: 'End of file'}); 
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


  const Bucket = process.env.BUCKET_NAME
  const Prefix = process.env.PREFIX

asynccompose(concatStreams, makeFileStreams, filterExportedDataFiles, getS3FileList)({Bucket:BUCKET_NAME})
.then(stream => {

    const chunks = Readable.from(stream);
    compose(batchWrieToDB, formatLinesGenerator ,streamToLinesGenerator)(chunks)
    
      chunks.on('error', (e) => { console.log('Error', e.message) })
      chunks.on('close', () =>  { console.log(`Stream closed: batches process = ${count.batches} `) })
      chunks.on('finish', () => { console.log(`Stream finished: batches process = ${count.batches} `) })
      chunks.on('end', (e) => console.log(`End of stream. ${e.message}`))
})
