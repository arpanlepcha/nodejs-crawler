var cluster = require('cluster'),
    http = require('http'),
    cores =  require('os').cpus().length * 4,
    exec = require('child_process').exec,
    fs = require('fs');


(function(){
    cluster.setupMaster({
        exec : "worker.js"
    });

    if(cluster.isMaster){
        split_files(function(){
            for (var i=0;i<cores;i++){
                var worker = cluster.fork();
                worker.on('message', function(msg) {
                    if(msg.event===1)  
                        process.kill(process.pid, 'SIGTERM');    
            
                }); 
            }
            cluster.on('fork',function(worker){
                console.log("worker is online");
            });
    
            cluster.on('exit',function(worker){
                console.log("worker died");
             });

        });
    }
    
    function split_files(callback){
        exec('wc -l urls', function (error, results) {
        var step = Math.ceil(parseInt(results,10)/cores),
        start = 1,
        end = step,
        index = 1;
        
        for(i=1;i<=cores;i++){
            asyncSet(i,start,end,function(){
                if(index===cores) callback();
                 index++;
            });
            start = end;
            end  = start+step;
            start++;
        }
   
        }); 
    }
    
    function asyncSet(i,start,end,callback){
        var command =  "sed -n '"+start+','+end+"p' urls > file"+i;
        exec(command,function(error,results){
            console.log(error); 
            callback();
        });
    }

})();






