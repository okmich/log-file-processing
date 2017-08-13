create database log_stream;

use log_stream;

create external table log_event (
	id string,
	ip string,
	ts bigint,
	tz string,
	cik int,
	ascno string,
	doc string,
	code int,
	size int,
	idxd boolean,
	refd boolean,
	agnt boolean,
	find string,
	crawler int,
	browser string
) 
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping" = ":key, main:ip, main:ts#b, main:tz, main:cik#b, main:ascno, main:doc, main:code#b, main:size#b, main:idxd#b, main:refd#b, main:agnt#b, main:find, main:crawler#b, main:browser")
tblproperties ("hbase.table.name" = "log_stream:log_event");

