select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_BOSE' as data_source
from ods_cn_BOSE.location

union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_apple_sz' as data_source
from ods_cn_apple_sz.location

union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_apple_sh' as data_source
from ods_cn_apple_sh.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_costacoffee' as data_source
from ods_cn_costacoffee.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_diadora' as data_source
from ods_cn_diadora.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_ferrero' as data_source
from ods_cn_ferrero.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_fuji' as data_source
from ods_cn_fuji.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_hd' as data_source
from ods_cn_hd.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_hp_ljb' as data_source
from ods_cn_hp_ljb.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_hpi' as data_source
from ods_cn_hpi.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_hualiancosta' as data_source
from ods_cn_hualiancosta.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_jiq' as data_source
from ods_cn_jiq.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_kone' as data_source
from ods_cn_kone.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_michelin' as data_source
from ods_cn_michelin.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_razer' as data_source
from ods_cn_razer.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_squibb' as data_source
from ods_cn_squibb.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_vzug' as data_source
from ods_cn_vzug.location


union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_zebra' as data_source
from ods_cn_zebra.location



union all
select  
location as location_id,
null as location_code,
location_type as location_type,
null as location_length, null as location_width, null as location_height, 
null as location_cube,
locating_zone,
null as location_aisle,
null as location_bay,
null as location_level,
null as location_position,
location_class, 
null as sku_dedication,
null as pull_zone,
null as max_qty,
location_sts as location_status,
picking_seq as pick_sequence_number,
warehouse as warehouse_id,
null as warehouse_code,
null as warehouse_name,
date_time_stamp as last_modified_date,
'scale_fas' as data_source
from ods_scale.location









