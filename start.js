var fs = require('fs');
var http = require('http');
var express = require('express');
var app = express();

var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database("nyc.db");

var router = express.Router();

var SECOND = 1000;
var HOUR = 60 * 60 * SECOND;


router.get("/history", (req, res) => {
    db.all("SELECT * from nyc", (err,rows) => {
        if (err) {
            console.log(err);
            return;
        }
        var _pws = {};

        var s = rows.map(row => {
            var x = JSON.parse(row.json);
            var cond = x.current_observation;
            var loc = x.location;
            var near = loc.nearby_weather_stations;
            var pws = near.pws.station;
            pws.forEach(pw => {
                _pws[pw.id] = pw;
            });

            var temp = cond.temperature_string; 
            var zip = row.zip;
            var time = row.date;
            return {zip, time, temp};
        });

        console.log(_pws);
        console.log(Object.keys(_pws).length);
        res.json(s);
    });
});

router.get("/history/:zip", (req, res) => {
    var zip = req.params.zip;
    var query = req.query;

    console.log("getting history for zip", zip);

    findNearest(zip)
        .then(rows => {
            if (rows.length !== 1){
                throw "where the rows at";
            }
            return rows[0].station;
        })
        .then(getObservations)
        .then(obs => {
            res.json(obs);
        })
        .catch(err => {
            console.log(err);  
        });
});

function getObservations (station)
{
    return new Promise((resolve, reject) => {
        db.all(`SELECT * FROM observations
               WHERE station=$station`, {
                   $station:station
               }, (err, rows) => {
                   if (err) {
                        reject(err);
                   } else {
                        resolve(rows);
                   }
               });
    });
}

function findNearest (zip)
{
    return new Promise((resolve, reject) => {
        db.all(`SELECT * from nearby
                WHERE nearby.zip=$zip
                ORDER BY distance DESC
                LIMIT 1
                `,{
                    $zip: zip,
                }, (err,rows) => {
                    if (err){
                        reject(err);
                    } else {
                        resolve(rows);
                    }
                });

    });
}

app.use("/api", router);

var jobs = [];

fs.readFile("./zips.txt", 'utf8', function (err, data) {
    if (err) throw new Error("wheres my zip file");

    var lines = data.match(/.+/g);

    jobs = lines.map(zip => {
        return {zip, last:0};
    });

    //limited to 10 req/min
    //setInterval(next, 6000);
});



function request (job)
{
	var options = {
		hostname: "api.wunderground.com",
		path: `/api/${env.apikey}/forecast/geolookup/conditions/q/${job.zip}.json`,
		method: "GET"
	};

	return new Promise((resolve, reject) => {
		var req = http.request(options, (res) => {
			var body = "";
			res.setEncoding('utf8');
			res.on('data', (chunk) => {
				body+=chunk;
			});
			res.on('end', () => {
                var j = JSON.parse(body);
                if (j.response.error){
                    reject(j.response.error);
                }else{
                    resolve({job, data:body});		
                }
                
			})
		});

		req.on('error', (e) => {
			reject(e);
		});

		req.end();
	});
}

function save (result) 
{
	var job = result.job;
	var data = result.data;
    console.log(JSON.parse(result.data));

    return new Promise((resolve, reject) => {
        db.run(`insert into nyc VALUES (?,?,?)`, [
                job.last, job.zip, data
        ], err => {
            if (err) {
                reject(err);
            } else {
                resolve(result.job);
            }
        });
    });
}

function crawl (job)
{
    console.log("crawling", job.zip, job.last);    
	return request(job)
		.then(save);
} 

function next ()
{
    var now = Date.now();
    var todo = jobs.filter(job => now - job.last > HOUR);

    if (todo.length === 0) return;

    var job = todo[0];
    job.last = now;
    crawl(job)
	.then(console.log)
	.catch(console.error);
}


var server = app.listen(3001);
