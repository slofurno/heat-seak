var db = require('./db');

var jobs = [];
var dates = [];

setInterval(function(){
    var next = jobs.pop();
    
    if (!next) {
        //console.log("no job");
        return;    
    }

    console.log("requesting history for station:", next);
    getStationHistory(next)
        .then(res => {
            try {
                return parseHistoryResponse(res)
                    .map(o => Object.assign({}, {station:next.station}, o));
            } catch(e) {
                throw e;
            }
        })
        .then(bulkInsertObservations)
        .catch(err => {
            console.log(err);
        });
      
},500);

makeDates(); 
startNearby();

function toWuDate (date)
{
    var dt = date.toLocaleDateString().split("/").map(x => "00" + x);

    var m = dt[0].slice(-2);
    var d = dt[1].slice(-2);
    var y = dt[2].slice(-4);

    return y + m + d
}

function makeDates ()
{
    var WEEK = 1000 * 60 * 60 * 24 * 7;
    var DAY = 1000 * 60 * 60 * 24;

    var date = new Date(2015, 9, 1);
    var now = Date.now();
    var time;

    while ((time = date.getTime()) < now) {
        var next = new Date(time + DAY); 
        //dates.push(toWuDate(date) + toWuDate(next));
        dates.push(toWuDate(date));
        date = next;
    }

    console.log(dates);
}

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

function startNearby ()
{
    return getNearby()
        .then(rows => {
            var stations = {};

            rows.forEach(row => {
                stations[row.station] = true;
            });

            return Object.keys(stations);
        })
        .then(stations => {
            console.log("there are", stations.length, "stations");
            stations.forEach(station => {
                dates.forEach(date => {
                    jobs.push({station, date});
                });
            });
            console.log("# jobs:", jobs.length);
            //jobs.push(...stations);
        });
}


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

function populateNearby ()
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

function saveResponse (job)
{
    console.log("saving raw response for", job.path);
    return new Promise((resolve, reject) => {
        db.run("insert into wu VALUES (?,?)", [
                job.path, job.body
        ], (err) => {
            if (err) {
                reject(err);
            } else {
                resolve(job.body);
            }
        });

    });    
}

function getStationHistory (job)
{
    var station = job.station;
    var date = job.date;
//history_2015122220151229
//history_2016011220160119
//history_2016010520160112
//history_2015122920160105
//history_2015121520151222

    var path = `/api/606f3f6977348613/history_${date}/units:english/v:2.0/q/pws:${station}.json`;

	var options = {
		hostname: "api.wunderground.com",
		path: path,
		method: "GET"
	};

	var makeRequest = function () {
        return new Promise((resolve, reject) => {
            var req = http.request(options, (res) => {
                var body = "";
                res.setEncoding('utf8');
                res.on('data', (chunk) => {
                    body+=chunk;
                });
                res.on('end', () => {
                    resolve({path, body});		
                })
            });

            req.on('error', (e) => {
                reject(e);
            });

            req.end();
        });
    };

    return makeRequest()
        .then(saveResponse);
}
