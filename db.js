var http = require('http');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database("nyc.db");

db.exec(`PRAGMA synchronous = OFF;
        PRAGMA journal_mode = MEMORY;`);


var jobs = [];

setInterval(function(){
    var next = jobs.pop();
    
    if (!next) {
        console.log("no job");
        return;    
    }

    console.log("requesting history for station:", next);
    getStationHistory(next)
        .then(res => {
            try {
                return parseHistoryResponse(res)
                    .map(o => Object.assign({}, {station:next}, o));
            } catch(e) {
                throw e;
            }
        })
        .then(bulkInsertObservations)
        .catch(err => {
            console.log(err);
        });
      
},500);

function bulkInsertObservations (observations)
{
    db.exec("BEGIN");

    var insert = db.prepare("INSERT INTO observations VALUES (?,?,?)");

    observations.forEach(obs => {
        insert.run([obs.station, obs.time, obs.temperature], (err) => {
            if (err) {
                throw new Error(err);
            }
        });
    });

    insert.finalize();
    db.exec("COMMIT");
}

function parseHistoryResponse (res)
{
	var j = JSON.parse(res);

	var response = j.response;
	var history = j.history;

	return history.days.map(day => {
		return day.observations.map(toObservation);
	})
	.reduce((a,c) => a.concat(c)); 
}

function toObservation (obs)
{
	var date = obs.date;
	var epoch_s = date.epoch;	
	var epoch_ms = epoch_s * 1000;
	var temperature = obs.temperature;
	return {time: epoch_ms, temperature};
}

getNearby()
    .then(rows => {
        var stations = {};

        rows.forEach(row => {
            stations[row.station] = true;
        });

        return Object.keys(stations);
    })
    .then(stations => {
        jobs.push(...stations);
    });


function getNearby ()
{
    return new Promise((resolve, reject) => {
        db.all("SELECT * FROM nearby", (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows);
            }        
        });
    });
}

function findNearby ()
{
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
            var stations = []; 

            var temp = cond.temperature_string; 
            var zip = row.zip;
            var time = row.date;

            pws.forEach(pw => {
                stations.push({zip: zip, station: pw.id, distance_km: pw.distance_km});
            });

            return stations;
        });

        var stations = s.reduce((a,c) => a.concat(c));

        console.log(stations);
        console.log(stations.length);

        db.exec("BEGIN");

        var insert = db.prepare("INSERT INTO nearby VALUES (?,?,?)");

        stations.forEach(station => {
            insert.run([station.zip, station.station, station.distance_km], (err) => {
                if (err) {
                    console.log(err);
                }
            });
        });

        insert.finalize();
        db.exec("COMMIT");

    });
}


function getStationHistory (station)
{
	var options = {
		hostname: "api.wunderground.com",
		path: `/api/606f3f6977348613/history_2016011220160119/units:english/v:2.0/q/pws:${station}.json`,
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
                resolve(body);		
			})
		});

		req.on('error', (e) => {
			reject(e);
		});

		req.end();
	});
}
