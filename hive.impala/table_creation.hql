create database if not exists app_log;

create external table log (
	host string, 
	clientAuthId string, 
	userId string, 
	ts string, 
	tz string, 
	method string, 
	resource string, 
	protocol string, 
	responsecode string, 
	bytes string
)
stored as parquet
location '/user/cloudera/output/logs/nasa_output';