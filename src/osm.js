const
    url = require('url'),
    http = require('http'),
    events = require('events');
    
var create_osm_submitter = function (osm_base_url, collection) {
    var client = Object.create(new events.EventEmitter, {});

    var osm_parts = url.parse(osm_base_url);
    client.osm_port = osm_parts.port;
    client.osm_hostname = osm_parts.hostname;
    client.osm_path = osm_parts.pathname.replace(/\/$/, "");

    console.log(client.osm_path);

    client.submit = function (obj) {
        var self = this;
                
        var osm = http.createClient(self.osm_port, self.osm_hostname),
            submit_path = self.osm_path + "/" + collection;
            
        osm.addListener('error', function(socketException) {
            console.log(socketException);
        });
            
        console.log("Created client for host " + self.osm_hostname + ":" + self.osm_port);
        var req = osm.request('POST', submit_path, {'host' : self.osm_hostname});
        
        req.end(JSON.stringify(obj));
        
        req.on('response', function (response) {
            var data = "";
            response.on('data', function (chunk) {
                data += chunk;
            });
            
            response.on('end', function () {
                console.log(response.statusCode + " " + data);

                if (response.statusCode === 200) {
                    self.emit('submittedObject', data.replace(/^\s*/, "").replace(/\s*$/, "")); //this should contain the uuid.
                } else {
                    self.emit('error', response.statusCode, data.toString());
                }
            });
        });
    };
    
    client.update = function (obj, doc_uuid) {
        var self = this;
        
        var osm = http.createClient(self.osm_port, self.osm_hostname),
            submit_path = self.osm_path + "/" + collection + "/" + doc_uuid;
            
        osm.addListener('error', function(socketException) {
            console.log(socketException);
        });
            
        console.log("Created client for host " + self.osm_hostname + ":" + self.osm_port);
        console.log("Update submission path: " + submit_path);

        var req = osm.request('POST', submit_path, {'host' : self.osm_hostname});
        
        req.end(JSON.stringify(obj));
        
        req.on('response', function (response) {
            var data = "";
            response.on('data', function (chunk) {
                data += chunk;
            });
            
            response.on('end', function () {
                console.log("Update response: " + response.statusCode + " " + data);

                if (response.statusCode === 200) {
                    self.emit('updated', data.toString());
                } else {
                    self.emit('error', response.statusCode, data.toString());
                }
            });
        });
    };
    
    client.submit_blank = function () {
        var self = this;
        
        var osm = http.createClient(self.osm_port, self.osm_hostname),
            submit_path = self.osm_path + "/" + collection;
        
        osm.addListener('error', function(socketException) {
            console.log(socketException);
        });
            
        console.log("Created client for host " + self.osm_hostname + ":" + self.osm_port);
        var req = osm.request('POST', submit_path, {'host' : self.osm_hostname});
        
        req.end(JSON.stringify({}));
        
        req.on('response', function (response) {
            var data = "";
            response.on('data', function (chunk) {
                data += chunk;
            });
            
            response.on('end', function () {
                console.log(response.statusCode + " " + data);

                if (response.statusCode === 200) {
                    self.emit('submittedBlank', data.replace(/^\s*/, "").replace(/\s*$/, "")); //this should contain the uuid.
                } else {
                    self.emit('error', response.statusCode, data.toString());
                }
            });
        });
    };
    
    client.add_callback = function(uuid, obj_callback) {
        var self = this;

        console.log("Adding a callback to UUID: " + uuid);

        var osm = http.createClient(self.osm_port, self.osm_hostname),
            submit_path = self.osm_path + "/" + collection + "/" + uuid + "/callbacks";
            
        osm.addListener('error', function(socketException) {
            console.log(socketException);
        });
            
        var req = osm.request('POST', submit_path, {'host' : self.osm_hostname});
        
        req.end(JSON.stringify(obj_callback));
        
        req.on('response', function (response) {
            var data = "";
            response.on('data', function (chunk) {
                data += chunk;
            });
            
            response.on('end', function () {
                console.log(response.statusCode + " " + data);

                if (response.statusCode === 200) {
                    self.emit("addedCallback", uuid, obj_callback);
                } else {
                    self.emit('error', response.statusCode, data.toString());
                }
            });
        });
    }
    
    return client;
};

exports.create_osm_submitter = create_osm_submitter;


