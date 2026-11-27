const { parse, format } = require("date-fns");

module.exports = function ({ str, regex, iformat, oformat }) {
    console.log(`extract_date: ${str}, ${regex}, ${iformat}, ${oformat}`);

    const pattern = new RegExp(regex);
    const match = pattern.exec(str);

    if (match && match[1]) {
        const dateStr = match[1];
        try {
            const parsedDate = parse(dateStr, iformat, new Date());
            if (isNaN(parsedDate)) {
                throw new Error(`Invalid date format: ${dateStr}`);
            }
            return format(parsedDate, oformat);
        } catch (err) {
            throw new Error(`Date parsing failed: ${err.message}`);
        }
    } else {
        throw new Error(`Date not found in: ${str}`);
    }
};
