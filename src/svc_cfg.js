var fs = require('fs');

//Reads in file contain JSON, converts it to an object, and passes it to
//callback as a parameter.
exports.configure = function (filename) {
    var cfg_string = fs.readFileSync(filename);
    return JSON.parse(cfg_string);
}
