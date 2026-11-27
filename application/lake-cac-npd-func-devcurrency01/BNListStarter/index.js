const df = require("durable-functions");

module.exports = async function (context, req) {

    //creates an instance of DurableOrchestrationClient Class
    const client = df.getClient(context);

    //instance is set to and executes our orchestrator function, "BNListOrchestrator"
    const instanceId = await client.startNew(req.params.functionName, undefined, req.body);


    context.log(`Started orchestration with ID = '${instanceId}'.`);


    //returns HTTP response message status on "BNListOrchestrator"
    return client.createCheckStatusResponse(context.bindingData.req, instanceId);
    
};