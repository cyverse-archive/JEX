const spawn = require('child_process').spawn;
const sys = require('sys');
const json = require('json2');

exports.filter = function(msg_obj) {
    return true;
}

exports.execute = function (msg_object, config) {
    var obj_json = JSON.stringify(msg_object);
    sys.puts(obj_json);
    log(obj_json);
};
