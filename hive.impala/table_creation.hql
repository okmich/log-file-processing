create database if not exists app_log;

use app_log;

create external table log (
	host string, 
	clientAuthId string, 
	userId string, 
	method string, 
	resource string, 
	protocol string, 
	responsecode string, 
	bytes string, 
	tz string, 
	ts string,
	ts_year smallint,
	ts_month tinyint,
	ts_day tinyint,
	ts_hour tinyint,
	ts_minute tinyint,
	ts_sec tinyint,
	ts_dayOfWeek tinyint
)
stored as parquet
location '/user/cloudera/output/logs/nasa_processed_logs';

create view v_html_access_log as 
	select * from log where 0 <> instr(resource, '.html');
