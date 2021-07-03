
set mapred.job.queue.name=default;
set hive.exec.dynamic.partition.mode=nonstrict;

---scale_inventory_full

---scale_inventory_full
 
INSERT OVERWRITE TABLE dsc.dsc_dwd.dwd_wh_dsc_inventory_dtl_di partition (inc_day= '$[time(yyyyMMdd,-1d)]')

select
null as wms_company_id,
'bose' as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_bose' as data_source 
from dsc.ods_cn_bose.location_inventory as LI 
inner JOIN dsc.ods_cn_bose.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER JOIN dsc.ods_cn_bose.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 

where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_apple_sz' as data_source 
from dsc.ods_cn_apple_sz.location_inventory as LI 
inner join dsc.ods_cn_apple_sz.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_apple_sz.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_apple_sh' as data_source 
from dsc.ods_cn_apple_sh.location_inventory as LI 
inner join dsc.ods_cn_apple_sh.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_apple_sh.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   


union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_costacoffee' as data_source 
from dsc.ods_cn_costacoffee.location_inventory as LI 
inner join dsc.ods_cn_costacoffee.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_costacoffee.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_diadora' as data_source 
from dsc.ods_cn_diadora.location_inventory as LI 
inner join dsc.ods_cn_diadora.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_diadora.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_ferrero' as data_source 
from dsc.ods_cn_ferrero.location_inventory as LI 
inner join dsc.ods_cn_ferrero.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_ferrero.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   


union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_fuji' as data_source 
from dsc.ods_cn_fuji.location_inventory as LI 
inner join dsc.ods_cn_fuji.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_fuji.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   


union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_hd' as data_source 
from dsc.ods_cn_hd.location_inventory as LI 
inner join dsc.ods_cn_hd.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_hd.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   


union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_hp_ljb' as data_source 
from dsc.ods_cn_hp_ljb.location_inventory as LI 
inner join dsc.ods_cn_hp_ljb.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_hp_ljb.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_hpi' as data_source 
from dsc.ods_cn_hpi.location_inventory as LI 
inner join dsc.ods_cn_hpi.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_hpi.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_hualiancosta' as data_source 
from dsc.ods_cn_hualiancosta.location_inventory as LI 
inner join dsc.ods_cn_hualiancosta.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_hualiancosta.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

-- union all
-- select
-- null as wms_company_id,
-- LI.company as wms_company_name,
-- null as wms_warehouse_id,
-- LI.warehouse as wms_warehouse_name,
-- IT.item as sku_code,
-- IT.item as sku_name,
-- IT.description as sku_desc,
-- li.inventory_sts as product_status,
-- LI.lot as batch_number,
-- null as serial_number,
-- null as sku_barcode,
-- LI.inventory_sts as inventory_status,
-- LI.location as location,
-- lo.LOCATING_ZONE as location_zone,
-- lo.LOCATION_CLASS as location_class,
-- lo.LOCATION_TYPE as location_type,
-- lo.LOCATION_STS as location_status,
-- LI.LOGISTICS_UNIT as container_id,
-- it.ITEM_CATEGORY3 as product_group,
-- it.ITEM_STYLE as product_type,
-- null as product_line,
-- LI.ALLOCATED_QTY as allocated_qty,
-- li.ON_HAND_QTY as actual_qty,
-- null as picked_qty,
-- LI.IN_TRANSIT_QTY  as filled_qty,
-- null as order_qty,
-- LI.QUANTITY_UM as quantity_um,
-- LI.AGING_DATE,
-- datediff(LI.AGING_DATE,current_date) as shelf_days,
-- li.RECEIVED_DATE as recived_date,
-- null as staging_location,
-- LI.INVENTORY_STS as lock_codes,
-- null as sort_location,
-- null as carton_equiv,
-- null as plt_equiv,
-- null as load_time,
-- LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
-- LI.date_time_stamp as last_modified_date,
-- 'scale_jiq' as data_source 
-- from dsc.ods_cn_jiq.location_inventory as LI 
-- inner join dsc.ods_cn_jiq.item as IT 
-- on LI.item = IT.item 
-- ---and Li.warehouse = IT.warehouse 
-- and li.company = IT.company
-- INNER join dsc.ods_cn_jiq.location as LO 
-- on LI.location = LO.location 
-- and LI.warehouse = LO.warehouse 
-- where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
-- and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
-- and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_kone' as data_source 
from dsc.ods_cn_kone.location_inventory as LI 
inner join dsc.ods_cn_kone.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_kone.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   


union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_michelin' as data_source 
from dsc.ods_cn_michelin.location_inventory as LI 
inner join dsc.ods_cn_michelin.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_michelin.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_razer' as data_source 
from dsc.ods_cn_razer.location_inventory as LI 
inner join dsc.ods_cn_razer.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_razer.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_squibb' as data_source 
from dsc.ods_cn_squibb.location_inventory as LI 
inner join dsc.ods_cn_squibb.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_squibb.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_vzug' as data_source 
from dsc.ods_cn_vzug.location_inventory as LI 
inner JOIN dsc.ods_cn_vzug.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER JOIN dsc.ods_cn_vzug.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   

union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_zebra' as data_source 
from dsc.ods_cn_zebra.location_inventory as LI 
inner join dsc.ods_cn_zebra.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER join dsc.ods_cn_zebra.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   




union all
select
null as wms_company_id,
LI.company as wms_company_name,
null as wms_warehouse_id,
LI.warehouse as wms_warehouse_name,
IT.item as sku_code,
IT.item as sku_name,
IT.description as sku_desc,
li.inventory_sts as product_status,
LI.lot as batch_number,
null as serial_number,
null as sku_barcode,
LI.inventory_sts as inventory_status,
LI.location as location,
lo.LOCATING_ZONE as location_zone,
lo.LOCATION_CLASS as location_class,
lo.LOCATION_TYPE as location_type,
lo.LOCATION_STS as location_status,
LI.LOGISTICS_UNIT as container_id,
it.ITEM_CATEGORY3 as product_group,
it.ITEM_STYLE as product_type,
null as product_line,
LI.ALLOCATED_QTY as allocated_qty,
li.ON_HAND_QTY as actual_qty,
null as picked_qty,
LI.IN_TRANSIT_QTY  as filled_qty,
null as order_qty,
LI.QUANTITY_UM as quantity_um,
datediff(LI.AGING_DATE, current_date) as shelf_days,
li.RECEIVED_DATE as recived_date,
null as staging_location,
LI.INVENTORY_STS as lock_codes,
null as sort_location,
null as carton_equiv,
null as plt_equiv,
null as load_time,
LI.RECEIVED_DATE as create_date, --RECEIVED_DATE AS CREAT_DATE
LI.date_time_stamp as last_modified_date,
'scale_fas' as data_source 
from dsc.ods_scale.location_inventory as LI 
inner JOIN dsc.ods_scale.item as IT 
on LI.item = IT.item 
---and Li.warehouse = IT.warehouse 
and li.company = IT.company
INNER JOIN dsc.ods_scale.location as LO 
on LI.location = LO.location 
and LI.warehouse = LO.warehouse 
where LI.inc_day= '$[time(yyyyMMdd,-1d)]' 
and IT.inc_day = '$[time(yyyyMMdd,-1d)]' 
and LO.inc_day = '$[time(yyyyMMdd,-1d)]'   


----# wo ye bu zhidao