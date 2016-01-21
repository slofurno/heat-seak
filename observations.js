var db = require('./db');

var SECOND = 1000;
var HOUR = 60 * 60 * SECOND;
var DAY = HOUR * 24;

module.exports = {
    forStation,
    nearestStation,
    getDay
};

function getDay (station, time)
{
    var endTime = time + DAY;
    console.log("s", time, endTime);

    return new Promise((resolve, reject) => {
        db.all(`SELECT * FROM observations
            WHERE station=$station
            AND time>=$time
            AND time<$endTime
            ORDER BY time ASC`, {
               $station: station,
               $time: time,
               $endTime: endTime
            }, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({time, rows});
                }
            });
    });
}

function forStation (station)
{
    return new Promise((resolve, reject) => {
        db.all(`SELECT * FROM observations
               WHERE station=$station
               ORDER BY time ASC`, {
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

function nearestStation (zip)
{
    return new Promise((resolve, reject) => {
        db.all(`SELECT * from nearby
                WHERE nearby.zip=$zip
                ORDER BY distance ASC
                LIMIT 1
                `,{
                    $zip: zip,
                }, (err,rows) => {
                    if (err){
                        reject(err);
                    } else if (rows.length === 0) {
                        reject("where the rows at");   
                    } else {
                        resolve(rows[0].station);
                    }
                });

    });

}
