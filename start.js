var fs = require('fs');
var http = require('http');
var express = require('express');
var app = express();

var observations = require('./observations');

var router = express.Router();

var SECOND = 1000;
var HOUR = 60 * 60 * SECOND;

router.get("/zip/:zip/history", (req, res, next) => {
    var zip = req.params.zip;
    var query = req.query;

    console.log("getting history for zip", zip);

    observations.nearestStation(zip)
        .then(observations.forStation)
        .then(obs => {
            res.json([obs]);
        })
        .catch(next);
});

router.get("/zip/:zip/daily/:time", (req, res, next) => {

    var zip = req.params.zip;
    var time = parseInt(req.params.time);

    console.log("time", time);

    observations.nearestStation(zip)
        .then(station => {
            return observations.getDay(station, time);            
        })
        .then(summarize)
        .then(s => {
            res.json(s);
        })
        .catch(next);
});

router.get("/station/:station/history", (req, res, next) => {
    var station = req.params.station; 

    observations.forStation(station)
        .then(obs => {
            res.json([obs]);
        })
        .catch(next);
});

router.get("/station/:station/daily/:time", (req, res, next) => {
    var time = parseInt(req.params.time);
    var station = req.params.station;

    observations.getDay(station, time)
        .then(summarize)
        .then(s => {
            res.json(s);
        })
        .catch(next);
});



function summarize (res)
{
    var time = res.time;
    var rows = res.rows;
    var sums = [];

    var hour = time + HOUR;
    var length = rows.length;

    for (var i = 0; i < length; i++) {
        var j = i;

        while (rows[i].time < hour && i < length - 1) {
            i++;
        }
        hour += HOUR;

        var hourly = rows.slice(j, i);

        if (hourly.length > 0) {
            var temps = hourly.map(x => x.temperature);
            var min = Math.min(...temps);
            var max = Math.max(...temps);
            var sum = temps.reduce((a, c) => a + c);
            var count = temps.length;
            var mean = sum/count;

            sums.push({
                min,
                max,
                mean,
                count
            });

        } else {
            sums.push({count: 0});
        }
    }

    return sums;
}

app.use("/api", router);

app.use((err, req, res, next) => {

    if (err instanceof Error) {
        console.error(err.stack);
    } else {
        console.error(err);
    }
    res.sendStatus(500);
});

app.use(express.static("static"));
var server = app.listen(3001);
