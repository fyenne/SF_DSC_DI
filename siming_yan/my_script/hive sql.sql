--hive sql

describe ods_udbfs.dsc_bill;



select 
data_update_time,
inc_day,
bill_start_date  
from ods_udbfs.dsc_bill where data_update_time like '%2020%' order by inc_day desc
