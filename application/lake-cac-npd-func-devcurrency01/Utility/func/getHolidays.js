const Holidays = require('date-holidays');
const { format } = require('date-fns');

module.exports = function ({ year = new Date().getFullYear(), include_holiday_name = 0 }) {
    console.log(`getHolidays: year=${year}, include_holiday_name=${include_holiday_name}`);

    try {
        // Create holidays instance for Canada with Ontario province
        const holidays = new Holidays('CA', 'ON');
        
        // Get holidays for the specified year
        const holidayList = holidays.getHolidays(year);
        
        // Filter to only include official/public holidays (exclude observances)
        const officialHolidays = holidayList.filter(holiday => 
            holiday.type === 'public' || holiday.type === 'bank'
        );
        
        // Sort holidays by date
        officialHolidays.sort((a, b) => new Date(a.date) - new Date(b.date));
        
        // Format the results based on include_holiday_name parameter
        if (include_holiday_name === 1) {
            // Return array of objects with holiday name and date
            return officialHolidays.map(holiday => ({
                name: holiday.name,
                date: format(new Date(holiday.date), 'yyyy-MM-dd')
            }));
        } else {
            // Return array of dates only
            return officialHolidays.map(holiday => format(new Date(holiday.date), 'yyyy-MM-dd'));
        }
        
    } catch (err) {
        throw new Error(`Failed to get holidays: ${err.message}`);
    }
};