const { DefaultAzureCredential } = require("@azure/identity");
const { BlobServiceClient } = require("@azure/storage-blob");
const fs = require('fs');
const child_process = require('child_process');


module.exports = async function (context, req) {
    

    //converts downloaded file to readable/writeable stream
    async function streamToBuffer(readableStream) {
        return new Promise((resolve, reject) => {
            
            const chunks = [];
            
            readableStream.on("data", (data) => {
                chunks.push(data instanceof Buffer ? data : Buffer.from(data));
            });
            
            readableStream.on("end", () => {
                resolve(Buffer.concat(chunks));
            });

            readableStream.on("error", reject);
        });
    }

    

    //define container/blob client for landing zone files
    const landing_storage = "https://".concat(context.bindings.name, ".blob.core.windows.net/")

    filenames = []
    const blobServiceClient = new BlobServiceClient(landing_storage, new DefaultAzureCredential());
    const containerClient = blobServiceClient.getContainerClient("land-currency");



    //for each LZ file write to current server
    for await (const blob of containerClient.listBlobsFlat()) {
        if (!blob.name.startsWith('binary_converter/')) {
                continue; // Skip to the next blob
        }
        
        file_name = blob.name.replace('binary_converter/','')

        const tempBlockBlobClient = containerClient.getBlockBlobClient(blob.name);
        const downloadBlockBlobResponse = await tempBlockBlobClient.download(0);

        //convert downloaded file to stream writeable to server
        buffer = await streamToBuffer(downloadBlockBlobResponse.readableStreamBody)

        fs.writeFileSync(file_name, buffer, (err) => {

            if (err){ 
                res = "Here:" + err 
            }
            
            else { 
                res = filenames 
            }

        });

        
        filenames.push(file_name) 
        
    }


    //execute conversion script for each file 
    for (let i = 0; i < filenames.length; i ++) {
        child_process.execFileSync('BPSEco-BNListToCSV.exe', [filenames[i]]);
    }   



    //csv output named the same as input xml
    //specifically, string preceeding ".xml" extension
    files = []
    for (let i = 0; i < filenames.length; i ++) {

        if (filenames[i].includes("Conv") == false){
            files.push(filenames[i].split(".")[0]);
        }

    }  

    
    
    
    //iterate all files on server, every file converted (.csv extension) gets uploaded landing zone
    for (let i = 0; i < files.length; i ++) {
        

        //upload via container and blob client already initialized to landing zone
        try {

                subDirectory = "binary_converter/" 
                convertedFileName = files[i].concat(".csv")
                const convertedBlobClient = containerClient.getBlockBlobClient(subDirectory.concat(convertedFileName));

                

                await convertedBlobClient.uploadFile(convertedFileName, {
                        rangeSize: 4 * 1024 * 1024,  
                        parallelism: 20,  
                        onProgress: (ev) => console.log(ev)
            });

        }


        //continue processing if any upload fails
        catch(err) {
            context.log("File failed upload")
        }

    
    }   







    //delete files uploaded from server, no longer needed
    child_process.execSync('del *.csv del *.xml del *.bin');






    //function completed successfully return response
    context.res = {
        body: "Success"
    };



}