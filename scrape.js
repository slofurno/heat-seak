var db = require('./db');
var http = require('http');
var observations = require('./observations');

var jobs = [];
var dates = [];
var rawhours = [];

var SECOND = 1000;
var HOUR = 60 * 60 * SECOND;
var DAY = HOUR * 24;

function startWorker ()
{
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
}

makeDates(); 
//startNearby();
startKNYC();
startWorker();

//makeHours();
//startStats();

function startStats ()
{
    if (rawhours.length === 0) return;

    var next = rawhours.pop();

    makeHourStats(next)
        .then(x => {
            startStats();
        })
        .catch(err => {
            console.log(err);   
        });
}

function makeHourStats (time)
{
    return getHour(time)
        .then(hourlyStats)
        .then(x => {
            //console.log(hotones);
        });
}

function Mathmean (n)
{
    var len = n.length;
    var sum = 0;
    for (var i = 0; i < len; i++) {
        sum += n[i];
    }
    return sum/len;
}

var hotones = {};
var knycdifs = [];

function hourlyStats (res)
{

    var hour = res.rows;
    hour.sort((a,b) => a.average - b.average);
    /* 
    var hottest = hour.slice(-10);
    var coldest = hour.slice(0,5);

    hottest.forEach(x => hotones[x.station] = x.station);
    */

    var time = res.time;
    var pp = new Date(time).toLocaleString();

    if (hour.length === 0) return {};
    var hourly = hour.map(x => x.average);
    var station_count = hourly.length;

    var max = Math.max(...hourly);
    var min = Math.min(...hourly);

    var sum = hourly.reduce((a,c) => a + c);
    var mean = sum/station_count;

    var deviations = hourly.map(x => x - mean);
    var ds = deviations.map(x => x * x);
    var ds_sum = ds.reduce((a,c) => a + c);
    var variance = ds_sum/station_count;
    var std = Math.sqrt(variance);

    var toohot = mean + 3*std;
    var hot = hour.filter(x => x.average >= toohot);

    hot.forEach(x => hotones[x.station] = true);

    var knyc = hour.filter(x => x.station === 'KNYC');
    var ok = hour.filter(x => !hotones[x.station]).map(x => x.average);

    if (knyc.length === 1 && ok.length > 0) {
        var okmean = Mathmean(ok);
        var dif = okmean - knyc[0].average;
        knycdifs.push(Math.abs(dif));
        console.log(ok.length, okmean, knyc[0].average, Mathmean(knycdifs));
    }

    return {time:pp,min,max,mean,std,station_count};
}

function getHour (time)
{
    
    var endTime = time + HOUR;

    return new Promise((resolve, reject) => {
        db.all(`SELECT station, AVG(temperature) as average, count() as count 
            FROM observations
            WHERE time>=$time
            AND time<$endTime
            GROUP BY station`, {
               $time: time,
               $endTime: endTime
            }, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({time,rows});
                }
            });
    });
}

function toWuDate (date)
{
    var dt = date.toLocaleDateString().split("/").map(x => "00" + x);

    var m = dt[0].slice(-2);
    var d = dt[1].slice(-2);
    var y = dt[2].slice(-4);

    return y + m + d
}

function makeHours ()
{
    var date = new Date(2015, 9, 4);
    var now = Date.now();
    var time;

    while ((time = date.getTime()) < now) {
        var next = new Date(time + HOUR); 
        rawhours.push(time);
        date = next;
    }
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
                    jobs.push({station:`pws:${station}`, date});
                });
            });
            console.log("# jobs:", jobs.length);
            //jobs.push(...stations);
        });
}

function startKNYC ()
{
    var station = "KNYC";

    dates.forEach(date => {
        jobs.push({station, date});
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
//.replace(/\"snowdepth\": T,/g, "")
    var path = `/api/606f3f6977348613/history_${date}/units:english/v:2.0/q/${station}.json`;

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
                    resolve({path, body:body.replace(/\"snowdepth\": T,/g, "")});		
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

