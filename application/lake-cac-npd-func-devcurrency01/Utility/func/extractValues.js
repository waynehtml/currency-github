module.exports = function ({array, key}) {
  console.log(array);
  if (!Array.isArray(array)) {
    throw new Error("First argument must be an array");
  }
  return array.map(obj => obj[key]);
};
