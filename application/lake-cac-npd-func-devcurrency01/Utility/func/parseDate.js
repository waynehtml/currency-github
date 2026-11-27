const chrono = require('chrono-node');
const { format } = require('date-fns');

module.exports = function ({ base_date, statement } = {}) {
    console.log(`parseDate: base_date=${base_date}, statement="${statement}"`);

    try {
        // Use current date as default base date if not provided
        const baseDate = base_date ? new Date(base_date) : new Date();
        
        // Validate base_date if provided
        if (base_date && isNaN(baseDate)) {
            throw new Error(`Invalid base date format: ${base_date}`);
        }
        
        // Parse the date statement using chrono
        const results = chrono.parse(statement, baseDate);
        
        if (results.length === 0) {
            throw new Error(`Could not parse date from statement: "${statement}"`);
        }
        
        // Get the first (most likely) parsed date
        const parsedDate = results[0].start.date();
        
        // Format to yyyy-MM-dd
        return format(parsedDate, 'yyyy-MM-dd');
        
    } catch (err) {
        throw new Error(`Date parsing failed: ${err.message}`);
    }
};