const
    fs = require('fs'),
    spawn = require('child_process').spawn,
    path = require('path'),
    sys = require('sys'),
    osm = require('osm'),
    events = require('events'),
    jobutils = require('jobutils');

/*
 * Base object for Condor jobs.
 *
 * Defines the nfs_dir, and submit_filename
 * attributes.
 *
 * Defines the get_submit_description(), get_submit_path(),
 * and write_submit_description() methods.
 * 
 * The get_submit_description() method is intended to be
 * overridden by sub-objects.
 */
var base_job = function () {
    var BaseJob = function () {
        events.EventEmitter.call(this);
    }
    sys.inherits(BaseJob, events.EventEmitter);
    
    var basejob = new BaseJob();
    
    basejob.nfs_dir = "";
    basejob.submit_filename = "";

    basejob.get_submit_description = function () {
        return "";
    };
    
    basejob.get_submit_path = function () {
        var self = this;
        return path.join(self.nfs_dir, "logs", self.submit_filename);
    };
    
    basejob.write_submit_description = function () {
        var submission = this.get_submit_description();
        var submit_path = this.get_submit_path();

        jobutils.mkdir_p(path.dirname(submit_path), 0777);
        jobutils.mkdir_p(path.join(path.dirname(submit_path)), "logs", 0777);

        console.log(submit_path);
        console.log("Submission contents: " + submission);
        fs.writeFileSync(submit_path, submission);
    };
    
    basejob.get = function (key) {
        return this[key];
    };
    
    basejob.set = function(key, value) {
        this[key] = value;
        return this;
    }
    
    return basejob;
};


/*
 * Creates a condor_job object.
 *
 * working_dir is the full path to the job's working directory,
 * which is where the job submission files get written to.
 * This is in /tmp/condor in the current system.
 *
 * nfs_dir is the full path to the job's directory on the NFS
 * share, which contains the files that the job operates on.
 * It's also where the results files get written to.
 * This is in /condor01/scratch in the current system.
 */
exports.create_condor_job = function(nfs_dir, job_id) {
    var err_filename = "iplantDag.err.txt";
    var log_filename = "iplantDag.log.txt";

    var condorjob = Object.create(base_job())
        .set('type', "condor")
        .set('name', "")
        .set('ipc-uuid', "")
        .set('username', "")
        .set('id', job_id)
        .set('nfs_dir', nfs_dir)
        .set('local_base_dir', "/tmp/")
        .set('executable', "")
        .set('args', "")
        .set('stdin_file', "")
        .set('stdout_file', "")
        .set('env', [])
        .set('parents', [])
        .set('children', [])
        .set('requirements', "")
        .set('timeout', "")
        .set('input_files', [])
        .set('output_regex', "");

    condorjob.set('err_filename', "iplantJob" + condorjob.get('id') + ".err.txt")
        .set('log_filename', "iplantDag" + condorjob.get('id') + ".log.txt");

    condorjob.set('submit_filename', "ipc-" + condorjob.get('id') + ".cmd");

    //Returns the contents of the submit file for the Condor job.
    condorjob.get_submit_description = function () {
        var nfs_dir = this.get('nfs_dir'),
            local_base_dir = this.get('local_base_dir'),
            err_filename = this.get('err_filename'),
            log_filename = this.get('log_filename'),
            executable = this.get('executable'),
            requirements = this.get('requirements');
        
        var submit = "";
        submit += "universe = vanilla\n";
        submit += "initialdir = " + nfs_dir + "\n";
        submit += "remote_initialdir = " + nfs_dir + "\n";
        submit += "requirements = " + requirements + "\n";
        
        var env = this.get('env');
        var env_string = "";
        for (var i = 0; i < env.length; i++) {
            var env_setting = env[i];
            env_string += env_setting + " ";
        }
        submit += 'environment = "' + env_string +'"\n';

        submit += "executable = " + executable + "\n";
        
        var stdin_file = this.get('stdin_file');
        if (stdin_file != "") {
            submit += "input = " + path.join(nfs_dir, "logs", stdin_file) + "\n";
        }

        var stdout_file = this.get('stdout_file');
        if (stdout_file != "") {
            submit += "output = " + path.join(nfs_dir, "logs", stdout_file) + "\n";
        } else {
            submit += "output = " + path.join(nfs_dir, "logs", err_filename) + "\n";
        }

        submit += "error = " + path.join(nfs_dir, "logs", err_filename) + "\n";
        submit += "log = " + path.join(local_base_dir, "logs", log_filename) + "\n";

        var args = this.get('args');
        if (args != "") {
            submit += "arguments = " + args + "\n";
        }
        
        var ipc_uuid = this.get('ipc-uuid');
        if (ipc_uuid !== "") {
            submit += "+IpcUuid = \"" + ipc_uuid + "\"\n";
        }
        
        submit += '+IpcJobId = "' + this.get('id') + "\"\n";
        
        var ipc_user = new Buffer(this.get('username')).toString('base64');
        if (ipc_user !== "") {
            submit += '+IpcUsername = "' + ipc_user + '"\n';
        }
        
        submit += "transfer_executable = False\n";
        
        var input_files = this.get('input_files');
        
        submit += "transfer_output_files = \n";
        submit += "when_to_transfer_output = ON_EXIT\n";
        submit += "notification = NEVER\n";
        submit += "queue\n";

        return submit;
    };
    
    /*
     * Performs the actual submission this job.
     *
     * Writes out the submission file and passes the path to 
     * it to'condor_submit'.
     */
    condorjob.submit = function () {
        this.write_submit_description();

        condor = spawn("condor_submit", [this.get_submit_path()]);
        console.log("Spawned condor_submit with submission file " + this.get_submit_path());

        condor.stdout.on('data', function(data) {
            console.log(data);
        });

        condor.stderr.on('data', function(data) {
            console.log(data);
        });

        condor.on('exit', function (code) {
            console.log("exited with: " + code);    
        });
    };

    return condorjob;
}


//Create a new stork job object.
exports.create_stork_job = function (nfs_dir, job_id, src, dest, verify, logging) {
    var storkjob = Object.create(base_job())
        .set('type', "stork")
        .set('src', src)
        .set('dest', dest)
        .set('parents', [])
        .set('children', [])
        .set('requirements', "")
        .set('verify', false)
        .set('logging', false)
        .set('local_base_dir', "/tmp/")
        .set('nfs_dir', nfs_dir)
        .set('id', job_id);
    
    if (verify !== undefined) {
        storkjob.set('verify', verify);
    }

    if (logging !== undefined) {
        storkjob.set('logging', logging);
    }

    var stork_prefix = "ipc-stork-" + storkjob.id;
    storkjob.set('submit_filename', stork_prefix + ".cmd");

    //Returns the contents of the submit file for the Stork job.
    storkjob.get_submit_description = function() {
        var submit = "";
        submit += "[\n";
        submit += '  dap_type = "transfer";\n';
        submit += '  src_url = "' + this.get('src') + '"\;\n';
        submit += '  dest_url = "' + this.get('dest') + '"\;\n';

        if (this.get('logging')) {
            var nfs_dir = this.get('nfs_dir');
            var local_base_dir = this.get('local_base_dir');

            submit += '  log = "' + path.join(local_base_dir, stork_prefix + ".log.txt") + '";\n';
            submit += '  output = "' + path.join(nfs_dir, stork_prefix + ".out.txt") + '";\n';
            submit += '  err = "' + path.join(nfs_dir, stork_prefix + ".err.txt") + '";\n';
        }

        submit += '  set_permission = "666";\n';

        if (this.get('verify')) {
            submit += '  verify_filesize = true;\n';
        } else {
            submit += '  verify_filesize = false;\n';
        }

        submit += "]\n";
        return submit;
    }
    return storkjob;
};

//Creates a new dag object.
exports.create_dag = function(nfs_dir, jobs) {
    var dag = Object.create(base_job())
        .set('nfs_dir', nfs_dir)
        .set('type', "dag")
        .set('edges', [])
        .set('jobs', jobs)
        .set('submit_filename', "iplantDag.dag");
    
    var create_edge = function (parent, child) {
        return {
            "parent" : parent,
            "child" : child,
            get_parent : function() {
                return this.parent;
            },
            get_child : function() {
                return this.child;
            }
        };
    };
    
    var add_edge = function (parent, child) {
        dag.edges.push(create_edge(parent, child));
    };
    
    dag.generate_edges = function () {
        for (var j = 0; j < this.jobs.length; j++) {
            var job = this.jobs[j];
            
            for(var c = 0; c < job.children.length; c++) {
                var child = job.children[c];
                add_edge(job, child);
            }
            
            for(var p = 0; p < job.parents.length; p++) {
                var parent = job.parents[p];
                add_edge(parent, job);
            }
        }
    };

    dag.get_job_defs = function (jobs) {
        var retval = "";
        var prefix = "";
        
        for (var i = 0; i < jobs.length; i++) {
            job = jobs[i];
            
            if (job.get('type') == "condor") {
                prefix = "JOB ";
            } else {
                prefix = "DATA ";
            }
            retval += prefix + job.get('id') + " " + job.get_submit_path() + "\n";
        }
        
        return retval;
    };
    
    dag.get_post_scripts = function (jobs) {
        var retval = "";
        var prefix = "SCRIPT POST ";
        
        for (var i = 0; i < jobs.length; i++) {
            job = jobs[i];
            retval += prefix + job.get('id') + " /usr/local/bin/handle_error.sh  " + job.get('working_dir') + " " + job.get('remote_dir') + " $RETURN\n";
        };
        
        return retval;
    };

    //Creates the contents of the dag file.
    dag.get_submit_description = function () {
        var contents = "";
        
        //Tells condor which jobs are included in the dag and where their submission
        //files are located.
        contents += this.get_job_defs(this.jobs);
        contents += "\n";

        //Write out each edge in the dag to the dag contents.
        for (edge_idx in this.edges) {
            edge = this.edges[edge_idx];
            contents += "PARENT " + edge.parent.id + " CHILD " + edge.child.id + "\n";
        }

        //Write out the SCRIPT POSTs that handle transferring files for failed jobs.
        contents += this.get_post_scripts(this.jobs);
        contents += "\n";

        return contents;
    };

    dag.submit = function(finished_callback) {
        var self = this;
        
        //Write out all of the job submission files.
        var job_types = [
            this.get('jobs')
        ];

        for (var i = 0; i < job_types.length; i++) {
            var job_list = job_types[i];

            for (var job_idx = 0; job_idx < job_list.length; job_idx++) {
                var this_job = job_list[job_idx];
                this_job.write_submit_description();
            }
        }

        this.write_submit_description();
        console.log("Wrote dag file " + this.get_submit_path());
        dag = spawn("condor_submit_dag", ["-f", this.get_submit_path()]);
        //dag = spawn("ls", ["-al", this.get_submit_path()]);
        console.log("spawned command: condor_submit_dag -f" + this.get_submit_path());
        
        var dag_output = "";
        
        dag.stdout.on('data', function(data) {
            console.log(data);
            dag_output += data.toString();
        });

        dag.stderr.on('data', function(data) {
            console.log(data);
            dag_output += data.toString();
        });

        dag.on('exit', function (code) {
            console.log("exited with: " + code);
            var dag_id = null;
            
            if (code === 0) {
                var extract_dag_id = /\d+ job\(s\) submitted to cluster (\d+)\./;
                var results = extract_dag_id.exec(dag_output);

                if ((results !== null) && (results.length >= 2)) {
                    dag_id = results[results.length-1];
                }
            }
            
            finished_callback(code, dag_id);
            self.emit('exit', code);
        });
    };

    //dag.set_edges();
    dag.generate_edges();
    return dag; 
}

