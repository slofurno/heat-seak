CREATE TABLE nyc (date number, zip text, json text);
CREATE TABLE pws (id text, json text);
CREATE TABLE nearby (zip varchar(10), station varchar(20), distance number);
CREATE TABLE observations (station varchar(12), time integer, temperature number);
CREATE INDEX observation_station on observations (station, time);
CREATE TABLE wu (url text, response text);
