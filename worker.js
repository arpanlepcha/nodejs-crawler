var  Cluster = require('cluster'),
     fs = require('fs'),
     byline = require('byline'),
     request = require('request'),
     async = require('async'),
     urllib = require('url'),
     MongoClient = require('mongodb').MongoClient;

require('http').globalAgent.maxSockets = 25000;
process.setMaxListeners(0);

(function(){
    
    Crawler = (function(){
        var prototype = Crawler.prototype, constructor = Crawler;
    
        Crawler.displayName = 'Crawler';
        function Crawler(worker,db){
            this.worker = worker;
            this.file = './file'+worker.id;
            this.queue = async.queue(this.fetcher.bind(this),3000);
            this.queue.drain = this.drained.bind(this);
            this.html = db.collection('html');
            this.error = db.collection('error');
            this.buffer = [];
        }
        
        prototype.drained = function(){
            if(this.buffer.length>0){ 
				this.html.insert(this.buffer,function(e,d){ })
			};
            this.worker.kill();
        };
        
        prototype.save = function(content){
            this.buffer.push(content);
            var _this = this;
            if(this.buffer.length > 100){
                this.html.insert(this.buffer.slice(0),function(e,d){
                    _this.buffer = [];
                });
            }
        };
        prototype.start = function(){
            var _this = this;
            var stream = byline(fs.createReadStream(_this.file));
            stream.on('data',function(urls){
                _this.queue.push(urls.toString('utf-8'));
            });
            
            stream.on('end',function(err,res){
               console.log("file read ended"); 
            });
        };
        prototype.fetcher = function(url,callback){
            var _this = this;
            request({url:url, maxRedirects: 2}, function(error, response, body){
                if (!error && response.statusCode === 200) {
                    _this.parse(body,url);
                } else{
                    _this.errSave(error,url);
                }
                callback();
            });
                       
        };
        
        prototype.parse = function(body,url){
            this.save({html:body,url:url});
        };
        
        prototype.errSave = function(err,url){
            //add tries here 
            this.error.insert({err:err,url:url},function(e,r){
                console.log("error occured"); 
            });
        };
		return  Crawler;
    })();
    
    if(Cluster.isWorker){
        var worker = Cluster.worker;
        MongoClient.connect("mongodb://localhost:27017/crawled", function(err, db) {
            var crawler = new Crawler(worker,db);
             crawler.start();
        });
    }
    
})(this);


 
