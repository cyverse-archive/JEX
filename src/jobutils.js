var fs = require('fs');

//The path.exists() function included in node.js isn't synchronous, which 
//is really annoying. This is an alternative that's rather hackish.
exports.file_exists = function(path) {
	var path_exists = true;
    try {
        var test_path = fs.realpathSync(path);
    } catch (err) {
        path_exists = false;
    }
    return path_exists;
};

//The fs.mkdir() function in node.js doesn't have a way to specify the -p option,
//so I hacked together this simplistic implementation.
exports.mkdir_p = function (dirpath, mode) {
    var dirnames = dirpath.split('/');
    var dir_index = 0;

    var dir_list = [];
    for (var i = 0; i < dirnames.length; i++) {
        dir_list.push(dirnames[i]);

        var this_path = dir_list.join('/');
        if (!this.file_exists(this_path)) {
            fs.mkdirSync(this_path, mode);
        }
    }
};

//Couldn't find a built-in function that does this, so I rolled my own.
exports.add_trailing_slash = function (a_path) {
    var last_char = a_path[a_path.length-1];
    if (last_char !== "/") {
        a_path = a_path + "/";
    }
    return a_path;
};

//Returns the config element from the object created from the JSON
//passed in as a payload in an AMQP message.
exports.get_job_config = function(msg_obj) {
    var job_config = null;
    
    if (msg_obj.config !== undefined) {
        job_config = msg_obj.config;
    } else {
        job_config = null;
    }
    
    return job_config;
};

exports.trim = function (trim_string) {
    return trim_string
        .replace(/^\s+/g, "")
        .replace(/\s+$/g, "");
};