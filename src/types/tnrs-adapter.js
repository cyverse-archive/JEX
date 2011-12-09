const http = require('http');
const json = require('json2');
const querystring = require('querystring');
const url = require('url');
const path = require('path');
const fs = require('fs');

const TNRS_HOST = 'arjuna.iplantc.org';
const TNRS_SEARCH_STRING = '/taxamatch-webservice-read-only/api/taxamatch.php?cmd=tnrs_taxamatch&source=tropicos&str=';
const TNRS_SEARCH = '/taxamatch-webservice-read-only/api/taxamatch.php';
const TNRS_SEARCH_PARAMS = 'cmd=tnrs_taxamatch&source=tropicos&str=';
const TNRS_TIMEOUT = 300000;
const TNRS_LINE_LIMIT = 10;

const HOSTNAME = "localhost:55555";
const PORT = 55555;
const CSV_BASE_DIR = "/tmp/";

var main = function () {
    //I'm treating the closure as an object, kinda. That's
    //why main looks like a HUUUUGE method. I'm using the closure
    //to prevent global namespace pollution.
    
    //The json2 module raises an error when it tries to parse a non-JSON string,
     //so hiding that fact makes the main callback a little cleaner.
     var parse_payload = function(payload) {
         var payload_obj = null;

         try {
             var payload_obj = JSON.parse(payload);
         } catch (err) {
             console.log(err);
             //blah
         }

         return payload_obj;
     };

     //Make sure the JSON object is formatted the way we expect.
     var json_is_valid = function (json_obj) {
         var check_null = function (dobj) {
             var retval = true;
             if (dobj === null) {
                 console.log("JSON was null.");
                 retval = false;
             }
             return retval;
         };

         var check_for_config = function (dobj) {
             var retval = true;
             if (dobj.config === undefined) {
                 console.log("JSON was missing the config.");
                 retval = false;
             }
             return retval;
         };

         var check_for_contents = function(dobj) {
             var retval = true;
             retval = check_for_config(dobj);

             if (retval) {
                 if (dobj.config.sourceContents === undefined) {
                     console.log("JSON missing sourceContents");
                     retval = false;
                 }
             }
             return retval;
         }

         var retval = true;
         retval = check_null(json_obj);
         if (retval) {
             retval = check_for_contents(json_obj);
         }
         return retval;
     };
     
     //Takes the JSON string that was returned by the TNRS service,
     //and generates a new JSON string that we pass back to the caller.
     var fix_json = function(tnrs_string) {
         //Passing null values is valid in JSON, but
         //is probably not smart to pass to a Java app.
         //We're converting nulls to empty strings.
         var fix_null = function (variable) {
             retval = variable;
             if (variable === null) {
                 retval = "";
             }
             return retval;
         };
         
         var tnrs_data = JSON.parse(tnrs_string);
         var new_obj = {};
         new_obj.items = []; 

         //This is where the list flattening happens.
         //The data value is a [[]]. We need to turn it
         //into a [].
         for (var i = 0; i < tnrs_data.data.length; i++) {
             //List of names for a match.
             var this_name_list = tnrs_data.data[i];

             //For each name in the list of matches...
             for (var j = 0; j < this_name_list.length; j++) {
                 var this_obj  = this_name_list[j];
                 var selected = false;

                 //Looks like TNRS returns entries sorted from
                 //highest to lowest by score. The first entry
                 //is therefore always the selected one. 
                 if (j === 0) {
                     selected = true;
                 }

                 //Translate the object returned by TNRS into an object the UI
                 //can accept. Translate nulls to "" and wrap turn integers into
                 //strings. Except for group, it's special. Also, do NOT transform
                 //booleans into strings.
                 new_obj.items.push({
                     "group"            :  i,
                     "nameSubmitted"    :  fix_null(this_obj.ScientificName_submitted),
                     "url"              :  fix_null(this_obj.NameSourceUrl),
                     "nameScientific"   :  fix_null(this_obj.Lowest_scientificName_matched),
                     "scientificScore"  :  fix_null(this_obj.Lowest_sciName_matched_score).toString(),
                     "authorAttributed" :  fix_null(this_obj.Canonical_author),
                     "family"           :  fix_null(this_obj.Accepted_family),
                     "genus"            :  fix_null(this_obj.Genus_matched),
                     "genusScore"       :  fix_null(this_obj.Genus_match_score),
                     "epithet"          :  fix_null(this_obj.SpecificEpithet_matched),
                     "epithetScore"     :  fix_null(this_obj.SpecificEpithet_matched_score).toString(),
                     "author"           :  fix_null(this_obj.Author_matched),
                     "authorScore"      :  fix_null(this_obj.Author_matched_score),
                     "annotation"       :  "",
                     "unmatched"        :  fix_null(this_obj.Unmatched),
                     "overall"          :  fix_null(this_obj.Overall_match_score).toString(),
                     "selected"         :  selected,
                 });
             }
         }
         return new_obj;
     }; //end fix_json().
     
     //Takes in an object and creates a csv formatted string.
     var create_csv_string = function(results) {
         var items = results.items;
         var retval = "";
         var header = [];
         
         if (items.length > 0) {
             for (var header_key in items[0]) {
                 if ((header_key !== "selected") && (header_key !== "group")) {
                     header.push(header_key);
                 }
             };
             var header_string = header.join(',') + "\n";
             console.log(header_string);
             retval += header_string;
             
             for (var i = 0; i < items.length; i++) {
                 item = items[i];
                 var row = [];
                 for (var header_key_idx = 0; header_key_idx < header.length; header_key_idx++) {
                     var header_key = header[header_key_idx];
                     row.push(item[header_key]);
                 }
                 var row_string = row.join(',') + "\n";
                 console.log(row_string);
                 retval += row_string;
             }
         }
         
         return retval;
     };
     
     var generate_uuid = function () {
         //yes, I got this from a StackOverflow response.
         function S4() {
            return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
         }
         function guid() {
            return (S4()+S4()+"-"+S4()+"-"+S4()+"-"+S4()+"-"+S4()+S4()+S4());
         }
         return guid();
     }
     
     //TODO: Annihilate this evilness.
     var write_csv_file = function (csv_contents, http_response) {
         var uuid = generate_uuid();
         fs.mkdirSync(path.join(CSV_BASE_DIR, uuid), 0744);
         var tnrs_csv_filename = path.join(uuid, "TNRS-results.csv");
         var csv_path = path.join(CSV_BASE_DIR, tnrs_csv_filename);
         var write_stream = fs.createWriteStream(csv_path, {'flags' : 'w'});
         write_stream.write(csv_contents);
         write_stream.end();
         
         var new_url = "http://" + HOSTNAME + "/getfile/" + tnrs_csv_filename;
         http_response.writeHead(200, {
             'Content-Length' : new_url.length,
             'Content-Type' : 'text/csv'
         });
         http_response.end(new_url);
     };
     
     //TODO: Annihilate this evilness.
     var get_csv_file_by_path = function(url_path, http_response) {
         var dirnames = path.dirname(url_path).split("/");
         var uuid = dirnames[2];
         
         var csv_filename = path.basename(url_path);
         var tnrs_filename = path.join(uuid, csv_filename);
         var csv_path = path.join(CSV_BASE_DIR, tnrs_filename);
         
         path.exists(csv_path, function (csv_exists) {
             if (csv_exists) {
                 var csv_contents = "";
                 var csv_reader = fs.createReadStream(csv_path);
                 csv_reader.on('data', function (chunk) {
                     csv_contents += chunk;
                 });
                 csv_reader.on('end', function () {
                     http_response.writeHead(200, {
                         'Content-Length' : csv_contents.length,
                         'Content-Type' : 'text/csv'
                     });
                     http_response.end(csv_contents);
                 });
             } else {
                 var file_not_found = "Couldn't file " + csv_path;
                 http_response.writeHead(404, {
                     'Content-Length' : file_not_found.length,
                     'Content-Type' : 'text/plain'
                 });
                 http_response.end(file_not_found);
             }
         });
     };
     
     var get_file_contents = function (data_obj, response){
         var response_string = create_csv_string(data_obj);
         response.writeHead(200, {
             'Content-Length' : response_string.length,
             'Content-Type' : 'text/csv'
         });
         response.end(response_string);
     };
     
     var send_blank_response = function (response) {
         var blank_response_string = '{"items":[]}';
         var blank_headers = {
             'Content-Length' : blank_response_string.length,
             'Content-Type' : 'application/json'
         };
         response.writeHead(200, blank_headers);
         response.end(blank_response_string);
     };
     
     var clean_name_string = function (names) {
        //trim leading and trailing whitespace.
        var name_string = names.trim();

        //trim trailing and leading newlines.
        name_string = name_string.replace(/[\s\r\n]+$/,'')
            .replace(/^[\s\r\n]+/,'')
            .replace(/\r\n/g, ',')
            .replace(/\t/g, ',')
            .replace(/\n/g, ',')
            .replace(/\r/g, ',');
            
        var name_list = name_string.split(',');
        
        if (name_list.length > 10) {
            var limited_list = [];
            for (var i = 0; i < 10; i++) {
                limited_list.push(name_list[i]);
            }
            name_string = limited_list.join(',');
        }
        return name_string
     };
     
     var create_tnrs_request = function (search_string, http_response) {
         var tnrs = http.createClient(80, TNRS_HOST);
         var svc_variables = TNRS_SEARCH_PARAMS + querystring.escape(search_string);
         console.log(svc_variables);
         
         var tnrs_request = tnrs.request(
             'POST', 
             TNRS_SEARCH,//search_string, 
             {
                'host' : TNRS_HOST,
                'Content-Type' : 'application/x-www-form-urlencoded',
                'Content-Length' : svc_variables.length
            }
         );
         
         var timeout_handler = function () {
             tnrs_request.connection.end();
             tnrs_request.connection.destroy();
             tnrs_request = null;
         };
         
         tnrs_request.connection.setTimeout(TNRS_TIMEOUT);
         tnrs_request.connection.on('timeout', function() {
             var error_message = "TNRS Request Timed Out";
             http_response.writeHead(408, {
                'Content-Type' : 'text/plain',
                'Content-Length' : error_message.length
             });
             http_response.end(error_message);
             timeout_handler();
         });
         
         tnrs_request.end(svc_variables);
         return tnrs_request;
     };
     
     var handle_tnrs_response = function(tnrs_data, tnrs_response, http_response) {
         console.log("Raw TNRS Results: " + tnrs_data);

         var fixed_data = fix_json(tnrs_data);
         var fixed_data_json= JSON.stringify(fixed_data);

         console.log("Fixed JSON: " + fixed_data_json);
         console.log("\n");

         var tnrs_status = tnrs_response.statusCode;
         var headers = null;
         if (tnrs_status !== 200) {
             headers = {
                 'Content-Length' : fixed_data_json.length,
                 'Content-Type' : 'text/plain'
             };
         } else{
             headers = {
                 'Content-Length' : fixed_data_json.length,
                 'Content-Type' : 'application/json'
             };
         }

         http_response.writeHead(tnrs_status, headers);
         http_response.end(fixed_data_json);
     };
     
     var get_tnrs_results = function (data_obj, http_response) {
        if (json_is_valid(data_obj)) {
            var name_string = clean_name_string(data_obj.config.sourceContents)

            if (name_string.length === 0) {
                send_blank_response(response)
            } else {
                //var tnrs_request = create_tnrs_request(search_string);
                var tnrs_request = create_tnrs_request(name_string, http_response);
                
                tnrs_request.on('response', function(tnrs_response){
                    var tnrs_data = "";
                    
                    tnrs_response.on('data', function(tnrs_chunk){
                        tnrs_data += tnrs_chunk;
                    });

                    tnrs_response.on('end', function (){
                        handle_tnrs_response(tnrs_data, tnrs_response, http_response);
                    });
                });
            }
        } else {
            send_error("Invalid JSON.", http_response);
        }
    };

    var send_error = function (msg, response) {
        response.writeHead(500, {'Content-Type' : 'application/json'});
        response.end('{ "error" : "' + msg + '" }');  
    };
    
    var create_server = function() {
        return http.createServer(function (request, response){
            var send_http_error = function (err_code, err_msg) {
                response.writeHead(err_code, {
                    'Content-Length' : err_msg.length,
                    'Content-Type' : 'text/plain'
                });
                response.end(err_msg);
            };
            
            var getfileurl = function (data_obj) {
                console.log("In getfileurl.");
                if (request.method === "POST") {
                    var csv_string = create_csv_string(data_obj);
                    write_csv_file(csv_string, response);
                } else {
                    send_http_error(405, "Only POST is supported on " + request.url);
                }
            };
            
            var getfile = function (data_obj) {
                console.log("In getfile.");
                if (request.method === "GET") {
                    var url_parts = url.parse(request.url);
                    get_csv_file_by_path(url_parts.pathname, response);
                } else {
                    send_http_error(405, "Only GET is supported on " + request.url);
                }
            };
            
            var getresults = function(data_obj) {
                console.log("In getresults.");
                if (request.method === "POST") {
                    get_tnrs_results(data_obj, response);
                } else {
                    send_http_error(405, "Only POST is supported on " + request.url);
                }
            };
            
            
            //This looks bad, but it really isn't.
            //Remember that we're in a callback that gets executed for each request,
            //so there will be a new copy of request_body for each request. One request
            //won't tromp on another request.
            var request_body = "";

            //Build up the request_body.
            request.on('data', function(chunk) {
                request_body += chunk;
            });

            //When we're done reading in the request body, we need to convert it into
            //a query string that gets used with the TNRS service.
            request.on('end', function() {                
                var data_obj = parse_payload(request_body);
                var response_string = "";
                
                request_url_parts = url.parse(request.url);
                var cmd_path = request_url_parts.pathname.split('/')[1];
                
                switch (cmd_path) {
                    case "":
                        getresults(data_obj);
                        break;
                        
                    case "getfileurl":
                        getfileurl(data_obj);
                        break;
                        
                    case "getfile":
                        getfile(data_obj);
                        break;
                        
                    default:
                        send_http_error(404, request.url + " was not found.");
                }
            });
        });
    };
    
    var execute = function() {
        server = create_server();
        server.listen(PORT);
    };
    
    var server = null;
    
    //Handles the uncaughtException event by reparsing the config and re-calling
    //main. This might be overkill. Does not generate a new PID. Might need
    //some extra logic to handle connection issues.
    process.on('uncaughtException', function(err) {
        console.log("ERROR: Uncaught Exception:  " + err); 
        if (err instanceof Error) {
            console.log(err.stack);
        }
        console.log("RESTARTING SERVICE");
        server = null;
        execute();
    });
    
    execute();
}();
