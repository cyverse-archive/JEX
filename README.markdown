JEX
===
Backend service that accepts JSON and submits DAGs to Condor with condor_submit_dag.


Configuration
-------------
The JEX is intended to be run as a user that can submit jobs to a Condor cluster. The condor_submit_dag executable needs to be on the PATH for the user that JEX runs as.


Input
-----
The JEX has a single endpoint, "/", which takes JSON in the following format (keep in mind this is simplistic):

    {
        "execution_target" : "condor",
        "analysis_id" : "ac37ced41d82346f68d1b27b56830526c",
        "name" : "jex_example",
        "analysis_name" : "Word Count",
        "username" : "auser",
        "request_type" : "submit",
        "uuid" : "j3b297bae-f264-4c3e-b5a5-4e049d524754",
        "email" : "an.email@example.org",
        "workspace_id" : "5",
        "notify" : true,
        "output_dir" : "/path/to/irods/output-dir",
        "create_output_subdir" : true,
        "description" : "",
        "analysis_description" : "Counts the number of words, characters, and bytes in a file",
        "steps" : [
            {
                "name" : "step_1",
                "type" : "condor",
                "config" : {
                    "input" : [
                        {
                            "name" : "input",
                            "property" : "input",
                            "type" : "File",
                            "value" : "/path/to/irods/input",
                            "id" : "wcInput",
                            "multiplicity" : "single",
                            "retain" : false
                        }
                    ],
                    "params" : [
                        {
                            "name" : "",
                            "order" : 0,
                            "value" : "input",
                            "id" : "wcInput"
                        },
                        {
                            "name" : "",
                            "value" : "wc_out.txt",
                            "order" : 1,
                            "id" : "wcOutput"
                        }
                    ],
                    "output" : [
                        {
                            "name" : "wc_out.txt",
                            "property" : "wc_out.txt",
                            "type" : "File",
                            "multiplicity" : "single",
                            "retain" : true
                        },
                        {
                            "name" : "logs",
                            "property" : "logs",
                            "type" : "File",
                            "multiplicity" : "collection",
                            "retain" : true
                        }
                    ]
                },
                "component" : {
                    "name" : "wc_wrapper.sh",
                    "type" : "executable",
                    "description" : "Word Count",
                    "location" : "/usr/local3/bin/wc_tool-1.00"
                }
            }
        ]
    }
    
An example curl will look like this:

    curl -H "Content-Type:application/json" -d 'Insert overly complicated JSON here' http://127.0.0.1:3000/

The result will look something like this:

    Analysis submitted.
    DAG ID: 1611
    OSM ID: 18CCA2F8-C08F-4E03-A4BC-9F25BE48FE5A

An error will result in a 500 HTTP error code and a stack-trace wrapped in JSON:

    {
        "message" : "Error message",
        "stack-trace" : "stacktrace here"
    }

Redirecting stdout and stderr
-----------------------------

Each step in the analysis can independently redirect stdout and stderr to files within the working directory. To do this, add the "stderr" and/or "stdout" fields to the step object. For example:

    {
        "name" : "step_1",
        "type" : "condor",
        "stdout" : "this_is_a_stdout_redirection",
        "stderr" : "this_is_a_stderr_redirection",
        "config" : {
            ...Removed for irrelevancy on this topic...
        },
        "component" : {
            ...Removed for irrelevancy on this topic...
        }
    }

The stdout and stderr fields should contain paths relative to the current working directory. Invalid paths will either result in stderr/stdout being lost or in an analysis execution failure. Since we don't have access to the execution nodes when jobs are submitted, the JEX cannot confirm that the paths listed in stderr/stdout are valid.


Stopping a running analysis
---------------------------

To stop an executing analysis, do a HTTP DELETE against the /stop/:uuid endpoint. Substitute an analysis uuid for the :uuid in the path. Here's an example with curl: 

    curl -X DELETE http://services-2.iplantcollaborative.org:31330/stop/07248d40-c707-11e1-9b21-0800200c9a66 

You should get JSON in the body of a 200 HTTP response formatted as follows: 

    { 
        "action" : "stop", 
        "status" : "success", 
        "condor-id" : "<A Condor identifier>" 
    } 

On an error, you should get a 500 HTTP response with a JSON body formatted as follows when the condor_rm command returns a non-zero status: 

    { 
        "action" : "stop", 
        "status" : "failure", 
        "error_code" : "ERR_FAILED_NON_ZERO", 
        "sub_id" : "<The Condor submission id>", 
        "err" : "<stderr from the condor_rm command>", 
        "out" : "<stdout from the condor_rm command>" 
    } 

Or, if the UUID can't be found in the OSM: 

   { 
       "action" : "stop", 
       "status" : "failure", 
       "error_code" : "ERR_MISSING_CONDOR_ID", 
       "uuid" : "<the uuid passed in>" 
   } 

Or for more general errors: 

    { 
        "action" : "stop", 
        "status" : "failure", 
        "error_code" : "ERR_UNCHECKED_EXCEPTION", 
        "message" : "<error specific message>" 
    }
    
An explanation of the individual fields in the input JSON, how they interact, and what is added by the JEX is forthcoming.
