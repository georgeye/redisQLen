var http = require('http'),
    url = require('url'),
    redis = require("redis"),
    config = require('./config');

var redis_pro = redis.createClient(config.redis_pro_port, config.redis_pro);
var redis_cons = redis.createClient(config.redis_cons_port, config.redis_cons);

http.createServer(function (req, res) {
   if(req.method == 'GET') {
      handle_query(req, res);
   }
   else {
      res.statusCode = 405; // method not allowed
      res.end();
   }
}).listen(config.port);

function handle_query(req, resp) {
  var  urlParsed = url.parse(req.url, true);
  if(urlParsed.pathname == '/queue_length') {
    var qName = urlParsed.query['queue_name'];
    if(qName == "push_apns_pro" || qName == "push_c2dm_pro") {
      query_queue_length(redis_pro, qName + "_queue", function(err, len) {
        cookResponse(resp, err, len);
      });
     }
    else if(qName == "push_apns_cons" || qName == "push_c2dm_cons") {
      query_queue_length(redis_cons, qName + "_queue", function(err, len) {
        cookResponse(resp, err, len);
      });
     }
     else {
       resp.statusCode = 400;
       resp.end();
     }
  }
  else {
    resp.statusCode = 400;
    resp.end();
  }
}

function cookResponse(resp, err, len) {
  if(err) {
    resp.statusCode = 500;
    resp.write(err);
  }
  else {
    resp.statusCode = 200;
    resp.write("length=" + len);
  }
  resp.end();
}

function query_queue_length(redis_client, name, callback) {
  redis_client.llen(name, function(err, len) {
    if(!err) {
      console.log("len=" + len);
      callback(null, len);
    }
    else {
      callback(err, null);
    }
  });
}

