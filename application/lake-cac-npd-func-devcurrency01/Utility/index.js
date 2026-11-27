const path = require("path");

module.exports = async function (context, req) {
    const funcName = context.bindingData.func;  // ← from route param
    const args = req.body;

    context.log('funcName:', funcName);
    context.log('body:', args);

    if (!funcName || !args) {
        context.res = {
            status: 400,
            body: { error: "Missing function name or args" }
        };
        return;
    }

    try {
        const fnPath = path.join(__dirname, "func", funcName);
        const fn = require(fnPath);

        const result = await fn(args);  // assumes each utility is a function accepting one JSON object
        context.res = {
            status: 200,
            body: { value: result }
        };
    } catch (err) {
        context.log.error(err);
        context.res = {
            status: 500,
            body: { error: `Function '${funcName}' failed or not found.`, details: err.message }
        };
    }
};
