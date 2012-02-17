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
                            "value" : "irods://irodsuser.irodszone:irodspassword@sampleirods.notarealurl.org:1247/path/to/irods/input",
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
                            "id" : "wcOutput"\
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
    
An explanation of the individual fields in the input JSON, how they interact, and what is added by the JEX is forthcoming.
