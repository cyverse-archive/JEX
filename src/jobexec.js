const
    http = require('http'),
    cfg = require('svc_cfg'),
    fs = require('fs'),
    sys = require('sys'),
    path = require('path'),
    jobutils = require('jobutils');

//This is from Javascript: The Good Parts by Douglas Crockford.
//Add a create() method to Object so we can get prototypical
//inheritance in a consistent manner.
if (typeof Object.create !== 'function') {
    Object.create = function (obj) {
        var NewClosure = function () {};
        NewClosure.prototype = obj;
        return new NewClosure();
    }
}

/*
 * Creates a global logging function from a WritableStream.
 * 
 * params:
 *    stream - A node.js WritableStream.
 * returns:
 *    nothing, but it results in a log() method being added 
 *    to the global namespace.
 */
var get_new_console_logger = function() {
    return function(msg) {
        var now = new Date();
        process.stdout.write("[" + now.toString() + "] " + msg + "\n");
    };
};

console.log = get_new_console_logger();

var print_usage = function() {
    sys.puts("node jobexec.js <configuration-file> <pid-file>");
}

const ARG_LENGTH = 4; //Max allowed length of argv.
const POS_PID = 3;    //
const POS_CONF = 2;   //Index in argv of the config file path.

//Make sure argv is the right length.
if (process.argv.length < ARG_LENGTH) {
    print_usage();
    process.exit(1);
}

//Make sure the configuration file exists.
if (!jobutils.file_exists(process.argv[POS_CONF])) {
    sys.puts("Configuration file " + process.argv[POS_CONF] + " doesn't exist.");
    process.exit(1);
}

//Read in the config file and create the config object that gets passed into main.
//The config file contains JSON.
var config_file = "";
config_file = fs.realpathSync(process.argv[POS_CONF]);
config = cfg.configure(config_file);

//Make sure the CONDOR_CONFIG is set in the execution environment
//and that the condor_* commands are on the PATH. Otherwise
//condor_submit_dag will fail.
process.env['STORK_CONFIG'] = config.stork_config;
process.env['CONDOR_CONFIG'] = config.condor_config;
process.env['PATH'] = process.env['PATH'] + ":" + config.condor_bin_dir + ":" + config.stork_bin + ":" + config.stork_sbin;

this.outputfs = null;

var get_log_stream = function () {
    return this.outputfs;
};

var setup_logging = function () {
    var self = this;
    self.outputfs = fs.createWriteStream(config.logfile, {'flags' : 'a'});

    console.log = function(msg) {
        var now = new Date();
        self.outputfs.write("[" + now.toString() + "] " + msg + "\n");
    };
};

setup_logging();


//The json2 module raises an error when it tries to parse a non-JSON string,
//so hiding that fact makes the main callback a little cleaner.
var parse_payload = function(payload) {
    var payload_obj = null;
    
    try {
        var payload_obj = JSON.parse(payload);
    } catch (err) {
        console.log("Couldn't parse the payload as JSON.");
    }
    
    return payload_obj;
};

//Returns the step element from the object created from the JSON
//passed in as a payload in an AMQP message.
var get_job_steps = function(msg_obj) {
    var job_steps = null;
    
    if (msg_obj.steps !== undefined) {
        job_steps = msg_obj.steps;
    } else {
        console.log("Message was missing the config variable.");
    }
    
    return job_steps;
};

//Returns the value of the type field from an object.
var get_msg_type = function(msg_obj) {
    var retval = null;

    if (msg_obj.hasOwnProperty('request_type')) {
        retval = msg_obj.request_type;
    }

    return retval;
};

//Looks at the mapping of message types to modules
//and returns the module for the passed in type.
var get_type_module = function(msg_type) {
    var retval = null;

    if (config.type_map.hasOwnProperty(msg_type)) {
        retval = config.type_map[msg_type];
    }
    console.log(config.type_map.toString());

    return retval;
};

/*
 *  Adds a listener to the AMQP connection that contains the main 
 *  application logic.
 */
this.httpserver = null;

var run = function () {
    //Can't just listen for the 'open' event, since AMQP has a multi-step
    //connection handshake.
    this.httpserver = http.createServer(function (req, res) {
        console.log("Received a request.");
        
        var payload = "";
        
        req.on('data', function (chunk) {
            payload += chunk;
        });
        
        req.on('end', function () {
            var msg_obj = parse_payload(String(payload));
            console.log("Received message: " + String(payload));

            var send_response = function (http_status, http_msg) {
                res.writeHead(http_status, {
                    'Content-Length' : http_msg.length,
                    'Content-Type' : 'text/plain'
                });

                res.end(http_msg);
            };
            
            var exit_function = function (status) {
                var http_status = 200;
                var http_msg = "Handled request for analysis " + msg_obj.uuid;
                
                if (status !== 0) {
                    http_status = 500;
                    http_msg = "ERROR: Submission didn't return a status of 0.\n"
                }

                send_response(http_status, http_msg);
            };

            //Extracts the config object from msg_obj and calls the execute() method
            //of cmd. This should result in a job being run.
            var run_command = function (msg_obj, cmd) {
                job_steps = get_job_steps(msg_obj);

                if (job_steps !== null) {
                    try {
                        console.log("Calling cmd.execute().");
                        cmd.execute(msg_obj, config, exit_function);
                        console.log("Done calling cmd.execute().");
                    } catch (err) {
                        console.log("ERROR: " + err);
                        console.log(err.stack);
                        console.log("Recovering and continuing...");
                    }
                } else {
                    console.log("JSON didn't contain a steps list.");
                }
            };

            //Imports the module and calls run_command().
            var import_type_module = function (msg_obj, type_module) {
                console.log("Module to be imported: " + type_module);
                var cmd = require(type_module);
                console.log("Imported command module " + type_module);

                if (cmd.filter(msg_obj)) {
                    run_command(msg_obj, cmd);
                } else {
                    console.log("Received a message that we don't care about.");
                }
            };

            if (msg_obj !== null) {
                var msg_type = get_msg_type(msg_obj);

                if (msg_type !== null) {
                    console.log("Message was of type: " + msg_type);
                    var type_module = get_type_module(msg_type);

                    if (type_module !== null) {
                        import_type_module(msg_obj, type_module);
                    } else {
                        http_status = 400;
                        http_msg = "Could not find a module for a message of type " + msg_type;
                        console.log(http_msg);
                        send_response(http_status, http_msg);
                        
                    }
                } else {
                    http_status = 400;
                    http_msg = "Message was JSON but didn't contain a 'type' field.";
                    console.log(http_msg);
                    send_response(http_status, http_msg);
                }
            } else {
                http_status = 400;
                http_msg = "Message was not JSON";
                console.log(http_msg);
                send_response(http_status, http_msg);
            }
        });
    }).listen(parseInt(config.http_port), config.http_hostname);
    console.log("Started up HTTP server on " + config.http_hostname + ":" + config.http_port);
};

var restart_log = function () {
    var log_stream = get_log_stream();
    log_stream.end();
    setup_logging();

    console.log("Logging restarted");
}

process.on('SIGUSR1', function () {
    restart_log();
});

process.on('SIGTERM', function () {
    console.log("SIGTERM signal received, stopping.");
    process.exit(1);
});

process.on('SIGINT', function () {
    console.log("SIGINT signal received, stopping.");
    process.exit(1);
});

process.on('SIGQUIT', function () {
    console.log("SIGQUIT signal received, stopping.");
    process.exit(1);
});

//Handles the uncaughtException event by reparsing the config and re-calling
//main. This might be overkill. Does not generate a new PID. Might need
//some extra logic to handle connection issues.
process.on('uncaughtException', function(err) {
    console.log("ERROR: Uncaught Exception:  " + err); 
    if (err instanceof Error) {
        console.log(err.stack);
    }
});

function write_pid() {
    var pid_file = process.argv[POS_PID];
    console.log("Writing PID " + process.pid + " to " + pid_file);
    fs.writeFileSync(pid_file, process.pid + "");
};

write_pid();
run();


