const
    condor = require('condorjob'),
    sys = require('sys'),
    path = require('path'),
    url = require('url'),
    osm = require('osm'),
    argescape = require('argescape'),
    _ = require('underscore'),
    jobutils = require('jobutils');

var get_irods_base = function (irods_url) {
    var url_parts = url.parse(irods_url);
    return url_parts.pathname;
};

var create_irods_url = function (base_url, new_path) {
    return jobutils.add_trailing_slash(base_url) + new_path;
};

var scrub_irods_url = function (irods_url) {
    var irods_proto = "irods://";

    if (irods_url.indexOf("@") !== -1) {
        var at_index = irods_url.indexOf("@");
        var proto_index = irods_url.indexOf(irods_proto) + irods_proto.length;
        var auth_slice = irods_url.slice(proto_index, at_index + 1);
        irods_url = irods_url.replace(auth_slice, "");
    }

    var url_parts = url.parse(irods_url);
    var proto = url_parts.protocol,
        new_path = url_parts.pathname;

    return new_path;
};

var fill = function (value, fill_char, len) {
    value = String(value);
    fill_char = String(fill_char);
    var fill_len = len - value.length;
    return (fill_len > 0) ? new Array(fill_len + 1).join(fill_char) + value : value;
};

var build_base_append_string = function () {
    var now = new Date();
    var retval
        = '-' + now.getUTCFullYear() + '-'
        + fill(now.getUTCMonth() + 1, 0, 2) + '-'
        + fill(now.getUTCDate(), 0, 2) + '-'
        + fill(now.getUTCHours(), 0, 2) + '-'
        + fill(now.getUTCMinutes(), 0, 2) + '-'
        + fill(now.getUTCSeconds(), 0, 2) + '.'
        + fill(now.getUTCMilliseconds(), 0, 3);
    return retval;
};

var contains_timestamp = function (analysis_name) {
    return analysis_name.match(/-\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.\d{3}$/);
}

var get_append_string = function (nfs_dir, username, analysis_name) {
    var iterations = 0;
    var base_append_string = build_base_append_string()
    var append_string = contains_timestamp(analysis_name) ? '' : base_append_string;
    var potential_path = path.join(nfs_dir, username, analysis_name + append_string);

    while (path.existsSync(potential_path)) {
        iterations++;
        append_string = base_append_string + "-" + iterations;
        potential_path = path.join(nfs_dir, username, analysis_name + append_string);
    }
    return append_string;
};

var assemble = function (msg_obj, app_config, no_at_username, dir_string) {
    var analysis_uuid = msg_obj.uuid,
        analysis_name = msg_obj.name.replace("/@/g", "_").replace(/[\s]+/g, '_'),
        analysis_workspace = msg_obj.workspace_id,
        analysis_steps = msg_obj.steps,
        io_maps = msg_obj.mappings,
        filetool_path = app_config.filetool_path,
        all_inputs = [],
        all_outputs = [];

    var working_dir = jobutils.add_trailing_slash(path.join(app_config.nfs_dir, no_at_username, analysis_name + dir_string));
    var local_dir = jobutils.add_trailing_slash(path.join(app_config.local_dir_base, no_at_username, analysis_name + dir_string));

    //The irods_dest_url is the path to the directory in iRODS.
    var irods_dir = path.join(get_irods_base(app_config.irods_url), no_at_username, "analyses", analysis_name + dir_string);
    var irods_dest_url = create_irods_url(app_config.irods_url, irods_dir + "/");

    //Create the local directory.
    jobutils.mkdir_p(local_dir, 0777);
    jobutils.mkdir_p(path.join(local_dir, "logs"), 0777);

    //Each step can have multiple input jobs. We want each job to have its own unique
    //ID, so we track the total number of input jobs.
    var total_input_jobs = 0;

    var build_input_jobs = function (child_job, input_job_defs) {
        var input_jobs = [];

        for (var i = 0; i < input_job_defs.length; i++) {
            var job_def = input_job_defs[i],
                job_id = "input-" + total_input_jobs;

            if (job_def.multiplicity !== undefined) {
                if (job_def.multiplicity === "collection") {
                    ipc_cmd = "getAll";
                }
            }

            //Get the path to the file from the iRODS URL.
            var src_url = scrub_irods_url(job_def.value);

            //The destination is the Condor working directory.
            var dest_path = working_dir;

            //Make sure that the file path is tracked as an input_file for the
            //child_job.
            child_job.input_files.push(path.basename(src_url));

            //Create a Condor job that runs iget to retrieve the file.
            var new_job = condor.create_condor_job(working_dir, job_id)
                .set("env", [app_config.icommands_path])
                .set("executable", filetool_path)
                .set('args', "-get " + " -source " + src_url)
                .set('remote_dir', irods_dir)
                .set('working_dir', working_dir)
                .set('local_base_dir', local_dir)
                .set('ipc-uuid', analysis_uuid)
                .set('username', no_at_username)
                .set('err_filename', "input-error-" + total_input_jobs)
                .set('log_filename', "input-log-" + total_input_jobs)
                .set('stdout_file', "input-stdout-" + total_input_jobs);

            //The input jobs are the parents of the main job.
            new_job.children.push(child_job);

            //Add the input job to the list of all input jobs
            //for this Condor job.
            input_jobs.push(new_job);
            total_input_jobs++;
        }

        return input_jobs;
    };

    //Each step can have multiple output jobs. We want each job to have its own unique
    //ID, so we track the total number of input jobs.
    var total_output_jobs = 0;

    var build_output_jobs = function (parent_job, output_job_defs) {
        var output_jobs = [];

        for (var i = 0; i < output_job_defs.length; i++) {
            var job_def = output_job_defs[i],
                job_id = "output-" + total_output_jobs;

            //The src_path is the path to the file in the working directory.
            var src_path = path.join(working_dir, path.basename(url.parse(job_def.value).pathname));

            //Create a Condor job that runs iget to retrieve the file.
            var new_job = condor.create_condor_job(working_dir, job_id)
                .set("env", [app_config.icommands_path])
                .set("executable", filetool_path)
                .set('args', "-source " + src_path + " -destination " + irods_dir)
                .set('local_base_dir', local_dir)
                .set('remote_dir', irods_dir)
                .set('working_dir', working_dir)
                .set('ipc-uuid', analysis_uuid)
                .set('username', no_at_username)
                .set('err_filename', "output-error-" + total_output_jobs)
                .set('log_filename', "output-log-" + total_output_jobs)
                .set('stdout_file', "output-stdout-" + total_output_jobs);

            //The output jobs are the children of the main job.
            new_job.parents.push(parent_job);

            //Add the output job to the list of all output jobs for this Condor job.
            output_jobs.push(new_job);
            total_output_jobs++;
        }

        return output_jobs;
    };

    //Takes the params element from the JSON,
    //sorts the elements in it, and generates the command-line
    //for the job.
    //Takes the params element from the JSON,
    //sorts the elements in it, and generates the command-line
    //for the job.
    var build_args = function (params) {
        var param_sort = function(a, b) {
            return parseInt(a.order) - parseInt(b.order);
        };

        var make_args = function (param) {
            return _.map([param.name, param.value], jobutils.trim);
        };

        //We're adding this because inputs and outputs with a negative
        //order value should not be added to the command-line for the
        //the job.
        var negative_order = function (param) {
            return parseInt(param.order) < 0;
        };

        var parse_out_bad_params = function (parameter) {
            console.log(JSON.stringify(parameter));
            param_name = parameter[0];
            param_value = parameter[1];

            if (param_name.length > 0 && param_value.length === 0) {
                if (param_name.indexOf("-") === 0) {
                    delimiter = " ";

                    if (param_name.indexOf("=") !== -1) {
                        var substring = param_name.substring(0, param_name.indexOf("="));

                        if (substring.indexOf(" ") === -1) {
                            delimiter = "=";
                        }
                    }

                    param_parts = param_name.split(delimiter);
                    param_name = param_parts[0];
                    param_value = param_parts.slice(1).join(delimiter);
                }

            } else if (param_name.length === 0 && param_value.length > 0) {
                if (param_value.indexOf("-") === 0) {
                    delimiter = " ";

                    if (param_value.indexOf("=") !== -1) {
                        var substring = param_value.substring(0, param_value.indexOf("="));

                        if (substring.indexOf(" ") === -1) {
                            delimiter = "=";
                        }
                    }

                    param_parts = param_value.split(delimiter);
                    param_name = param_parts[0];
                    param_value = param_parts.slice(1).join(delimiter);
                }
            }
            return [param_name, param_value];
        };

        var tuple_escaper = function (param_tuple) {
            return _.map(param_tuple, argescape.escape);
        };

        /*
         *  Joins parameters together with a "" if the first
         *  part ends with an '='.
         */
        var join_params = function (parameter) {
            var delimiter = " ",
                param_name = parameter[0],
                param_value = parameter[1];

            if (param_name.match(/\=$/)) {
                delimiter = "";
            }

            return [[param_name, param_value].join(delimiter)];
        };

        //Filter out the negative order params.
        params = _.reject(params, negative_order);
        params.sort(param_sort);

        var trimmed_params = _.map(params, make_args);
        var parsed_params = _.map(trimmed_params, parse_out_bad_params);
        var escaped_params = _.map(parsed_params, tuple_escaper);
        var joined_params = _.map(escaped_params, join_params);
        var flattened_params = _.flatten(joined_params);
        var arg_string = flattened_params.join(" ");

        return argescape.wrap_in_double_quotes(arg_string);
    };

	var exclude_arg = function (working_dir, input_defs, output_defs) {
		var excluded_paths = [];

        _.each(input_defs, function (def) {
            if (!def.retain) {
                excl_path = path.join(working_dir, path.basename(scrub_irods_url(def.value)))

                if (def.multiplicity === "collection") {
                    excl_path = jobutils.add_trailing_slash(excl_path)
                }

				excluded_paths.push(excl_path)
            }
        });

        //We don't get URLs for outputs, so they have to be treated differently.
        _.each(output_defs, function (def) {
            if (!def.retain) {
                excl_path = path.join(working_dir, def.name) //Might need to be def.property.

                if (def.multiplicity === "collection") {
                    excl_path = jobutils.add_trailing_slash(excl_path)
                }

                excluded_paths.push(excl_path)
            }
        });

        if (excluded_paths.length > 0) {
            return " -exclude " + excluded_paths.join();
        } else {
            return "";
        }
	};

    var build_steps = function (steps, mapping) {
        var retval = [];
        var prev_job = null;
        var last_output_job = null;

        //Create a mkdir job that creates the destination directory in iRODS.
        var imkdir_job = condor.create_condor_job(working_dir, "imkdir")
            .set("env", [app_config.icommands_path])
            .set("executable", filetool_path)
            .set('args', "-mkdir " + "-destination " + irods_dir)
            .set('local_base_dir', local_dir)
            .set('remote_dir', irods_dir)
            .set('working_dir', working_dir)
            .set('ipc-uuid', analysis_uuid)
            .set('username', no_at_username)
            .set('err_filename', "imkdir-error")
            .set('log_filename', "imkdir-log")
            .set('stdout_file', "imkdir-stdout");

        //The imkdir_job needs to run once and before the output jobs. Placing it first in the
        //DAG will satisfy those requirements and ensure that the directory gets created even
        //for DAGs that fail.
        prev_job = imkdir_job;
        retval.push(imkdir_job);

        for (var i = 0; i < steps.length; i++) {
            var step = steps[i];

            if (step.type === "condor") {
                var comp = step.component;
                var executable = path.join(comp.location, comp.name),
                    config = step.config;

                all_inputs = all_inputs.concat(config.input)
                all_outputs = all_outputs.concat(config.output)

                var arguments = build_args(config.params);

                var new_job_id = "condor-" + i;

                var new_condor_job = condor.create_condor_job(working_dir, new_job_id)
                    .set("executable", executable)
                    .set('args', arguments)
                    .set('local_base_dir', local_dir)
                    .set('remote_dir', irods_dir)
                    .set('working_dir', working_dir)
                    .set('ipc-uuid', analysis_uuid)
                    .set('username', no_at_username)
                    .set('err_filename', "condor-error-" + i)
                    .set('log_filename', "condor-log-" + i)
                    .set('stdout_file', "condor-stdout-" + i);

                //Create the input jobs
                if (config.input !== undefined) {
                    var input_jobs = build_input_jobs(new_condor_job, config.input);

                    //The imkdir job must be the first job.
                    if (input_jobs.length > 0) {
                        input_jobs[0].parents.push(imkdir_job);
                    }
                }

                //Right now the jobs are chained together.
                //TODO: use the mapping to determine if two jobs can run in parallel.
                if (prev_job !== null) {
                    new_condor_job.parents.push(prev_job);
                }

                prev_job = new_condor_job;

                //Push the Condor jobs onto the master list of jobs.
                //The ordering on the master list shouldn't matter, the DAG
                //generation code looks at the 'children' and 'parent' attributes
                //of each job to determine which edges exist.
                retval.push(new_condor_job);

                if (config.input !== undefined) {
                    retval = retval.concat(input_jobs);
                }
            }
        }

        //This is the job that shotguns the files into iRODS.
        var last_job_id = "output-last";

        var irods_shotgun_job = condor.create_condor_job(working_dir, last_job_id)
            .set("env", [app_config.icommands_path])
            .set("executable", filetool_path)
            .set('args', "-source " + working_dir + " -destination " + irods_dir + exclude_arg(working_dir, all_inputs, all_outputs))
            .set('local_base_dir', local_dir)
            .set('remote_dir', irods_dir)
            .set('working_dir', working_dir)
            .set('ipc-uuid', analysis_uuid)
            .set('username', no_at_username)
            .set('err_filename', "output-error-" + "last")
            .set('log_filename', "output-log-" + "last")
            .set('stdout_file', "output-stdout-" + "last");

        //The shotgun job needs to be the last job in the DAG.
        if (last_output_job !== null) {
            last_output_job.children.push(irods_shotgun_job);
        } else {
            irods_shotgun_job.parents.push(prev_job);
        }
        retval.push(irods_shotgun_job);

        return retval;
    };

    var jobs = build_steps(analysis_steps, io_maps);
    var dag = condor.create_dag(working_dir, jobs);
    return dag;
};

exports.filter = function (msg_obj) {
    var retval = false;
    if (msg_obj.hasOwnProperty('request_type')) {
        if (msg_obj.request_type === "submit") {
            retval = true;
        }
    }
    return retval;
};

var create_outputs_from_steps = function (steps) {
    var output_list = [];

    for (var s = 0; s < steps.length; s++) {
        var step = steps[s];

        if (step.config.hasOwnProperty("outputs")) {
            var outputs = step.config.outputs;

            for (var out = 0; out < outputs.length; out++) {
                var output = outputs[out];

                output_list.push(output);
            }
        }
    }

    return output_list;
};

var create_osm_object = function (msg_obj, dag, status_code, config, dag_id, dir_string) {
    var now = new Date();
    var new_obj = {};
    var status = "Running";

    if (status_code !== 0) {
        status = "Failed"
    }

    new_obj.uuid = msg_obj.uuid;
    new_obj.name = msg_obj.name;
    new_obj.user = msg_obj.username;
    new_obj.description = msg_obj.description;
    new_obj.analysis_id = msg_obj.analysis_id;
    new_obj.analysis_name = msg_obj.analysis_name;
    new_obj.workspace_id = msg_obj.workspace_id;
    new_obj.dag_id = dag_id;
    new_obj.submission_date = now.toString();
    new_obj.status = status
    new_obj.jobs = {};

    if (msg_obj.hasOwnProperty("email")) {
        new_obj.email = msg_obj.email;
    } else {
        new_obj.email = msg_obj.username;
    }

    if (msg_obj.hasOwnProperty("notify")) {
        new_obj.notify = msg_obj.notify;
    }

    new_obj.output_manifest = create_outputs_from_steps(msg_obj.steps);

    var out_user = msg_obj.username.replace("@", "_").replace(/[\s]+/g, '_'),
        out_name = msg_obj.name.replace("@", "_").replace(/[\s]+/g, '_');

    new_obj.output_dir = jobutils.add_trailing_slash(config.irods_url) + path.join(
        out_user,
        "analyses",
        out_name + dir_string
    );

    for (var i = 0; i < dag.jobs.length; i++) {
        var this_job = dag.jobs[i];
        var this_jid = this_job.id;
        new_obj.jobs[this_jid] = {};
        new_obj.jobs[this_jid].type = this_job.get('type');
        new_obj.jobs[this_jid].submission_date = now.toString();
        new_obj.jobs[this_jid].executable = this_job.get('executable');
        new_obj.jobs[this_jid].args = this_job.get('args');
        new_obj.jobs[this_jid].status = status;

        if (this_job.hasOwnProperty('src')) {
            new_obj.jobs[this_jid].src = this_job.get('src');
        }

        if (this_job.hasOwnProperty('dest')) {
            new_obj.jobs[this_jid].dest = this_job.get('dest');
        }
    }

    return new_obj;
};

var submit_to_osm = function (msg_obj, dag, config, dir_string, callback) {
    return function (status_code, dag_id) {
        var self = this;

        var obj = create_osm_object(msg_obj, dag, status_code, config, dag_id, dir_string);

        var osm_submitter = osm.create_osm_submitter(
            config.osm_baseurl,
            config.osm_service
        );

        self.callback = callback;
        self.osm_submitter = osm_submitter;
        self.config = config;

        osm_submitter.on('error', function (status_code, data_string) {
            console.log("Got an error code of " + status_code + "from the OSM with a message of:\n" + data_string);
        });

        osm_submitter.on('addedCallback', function (uuid, callback) {
            console.log("Finished adding callback to UUID " + uuid);
            osm_submitter.update(obj, uuid);
        });

        osm_submitter.on('submittedBlank', function (uuid) {
            console.log("Submitted blank object with UUID of " + uuid);

            var notification_callback = {
                "callbacks" : [{
                    "type" : "on_update",
                    "callback" : self.config.notification_url
                }]
            };

            osm_submitter.add_callback(uuid, notification_callback);
        });

        osm_submitter.submit_blank();
    }
};

exports.execute = function (msg_obj, app_config, exit_function) {
    var log_osm_submission = function (obj) {
        console.log("Submitted object to OSM and got this response: " + obj);
    };

    console.log(sys.inspect(process.env));
    var no_at_username = msg_obj.username.replace("/@/g", "_").replace(/[\s]+/g, '_');
    var dir_string = get_append_string(app_config.nfs_dir, no_at_username, msg_obj.name);

    var actual_dag = assemble(msg_obj, app_config, no_at_username, dir_string);
    actual_dag.on('exit', exit_function);

    actual_dag.submit(
            submit_to_osm(
                    msg_obj,
                    actual_dag,
                    app_config,
                    dir_string,
                    log_osm_submission
            )
    );

    return actual_dag
};
