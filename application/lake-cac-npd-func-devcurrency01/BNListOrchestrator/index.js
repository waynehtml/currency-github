const df = require("durable-functions");

module.exports = df.orchestrator(function* (context) {

    const outputs = [];


    //returns a task data structure describing status of "BNListActivity" script
    //https://learn.microsoft.com/en-us/python/api/azure-functions-durable/azure.durable_functions.durableorchestrationcontext?view=azure-python#azure-durable-functions-durableorchestrationcontext-call-activity
    outputs.push(yield context.df.callActivity("BNListActivity", context.bindings["context"]["input"]["storage"]));
    
    
    
    return outputs;

});