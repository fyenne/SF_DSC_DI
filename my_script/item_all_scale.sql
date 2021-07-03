union all
select
null as wms_company_id,
'bose' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_bose' as data_source
from ods_cn_BOSE.item as itm
left join ods_cn_BOSE.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'apple_sz' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_apple_sz' as data_source
from ods_cn_apple_sz.item as itm
left join ods_cn_apple_sz.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'apple_sh' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'apple_sh' as data_source
from ods_cn_apple_sh.item as itm
left join ods_cn_apple_sh.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'costacoffee' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'costacoffee' as data_source
from ods_cn_costacoffee.item as itm
left join ods_cn_costacoffee.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'diadora' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_diadora' as data_source
from ods_cn_diadora.item as itm
left join ods_cn_diadora.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'ferrero' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_ferrero' as data_source
from ods_cn_ferrero.item as itm
left join ods_cn_ferrero.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'fuji' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_fuji' as data_source
from ods_cn_fuji.item as itm
left join ods_cn_fuji.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'hd' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_hd' as data_source
from ods_cn_hd.item as itm
left join ods_cn_hd.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'hp_ljb' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_hp_ljb' as data_source
from ods_cn_hp_ljb.item as itm
left join ods_cn_hp_ljb.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 





union all
select
null as wms_company_id,
'hpi' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_hpi' as data_source
from ods_cn_hpi.item as itm
left join ods_cn_hpi.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'hualiancosta' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_hualiancosta' as data_source
from ods_cn_hualiancosta.item as itm
left join ods_cn_hualiancosta.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 





union all
select
null as wms_company_id,
'jiq' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_jiq' as data_source
from ods_cn_jiq.item as itm
left join ods_cn_jiq.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'kone' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_kone' as data_source
from ods_cn_kone.item as itm
left join ods_cn_kone.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 



union all
select
null as wms_company_id,
'michelin' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_michelin' as data_source
from ods_cn_michelin.item as itm
left join ods_cn_michelin.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'razer' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_razer' as data_source
from ods_cn_razer.item as itm
left join ods_cn_razer.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'squibb' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_squibb' as data_source
from ods_cn_squibb.item as itm
left join ods_cn_squibb.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'vzug' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_vzug' as data_source
from ods_cn_vzug.item as itm
left join ods_cn_vzug.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 




union all
select
null as wms_company_id,
'zebra' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_zebra' as data_source
from ods_cn_zebra.item as itm
left join ods_cn_zebra.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 


union all
select
null as wms_company_id,
'fas' as wms_company_name,
itm.item as sku_code,
itm.`description` as sku_name,
null as sku_desc,
itm.`active` as sku_status,
null as product_line,
itm.item_category1 as product_categoty ,
itm.item_category2 as product_group,
itm.item_category3 as product_sub_group,
null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
IUOM.QUANTITY_UM as quantity_um,
IUOM.LENGTH as length,
IUOM.WIDTH as width,
IUOM.HEIGHT as height,
IUOM.WEIGHT as weight,
IUOM.USER_DEF8 as volumn,
weight_um as weight_um,
null as volume_um,
null as std_pack_height,
null as std_pack_length,
null as std_pack_width,
null as std_pack_volume,
null as std_pack_weight,
null as std_pack_qty,
null as std_case_height,
null as std_case_length,
null as std_case_width,
null as std_case_volume,
null as std_case_weight,
null as std_case_qty,
null as manuafacture_date,
null as expiration_date,
null as putaway_type,
null as allocation_type,
null as load_time,
null as create_date,
itm.date_time_stamp as last_modified_date,
'scale_fas' as data_source
from ods_scale.item as itm
left join ods_scale.item_unit_of_measure as iuom
ON ITM.ITEM = IUOM.ITEM
WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]' 

