----
---- bdp sql . for all scale data base, this doc is designed for looking up col names in bdp and 
---- select what we want to build up data base. 
---- siming yan, 2021/6/29



set mapred.job.queue.name=dsc;
set hive.execution.engine=tez;
--- go with ods_scale.*   ## the FAS project and so on.


"""
************************
---inb
dwd_wh_dsc_inbound_header_dtl_di
************************
"""
---crystal
select 
    tc_company_id as wms_company_id,
    '' as wms_company_name,
    destination_facility_alias_id as wms_warehouse_id,
    asn_id as internal_id_id,
    tc_asn_id as asn_id,
    case asn_status 
        when '10' then 'Open' 
        when '20' then 'InTransit'
        when '30' then 'Receiving Started'
        when '40' then 'Receiving Verified'
        when '50' then 'Refused'
        when '60' then 'Cancelled'
        when '70' then 'Pending Cancelled'
    end as wms_asn_status,
    '' as std_asn_status,
    tc_asn_id_u as customer_asn,
    created_dttm as create_time,
    receipt_dttm as receipt_time,
    verified_dttm as closed_date,
    last_updated_dttm as last_modified_date,
    'WMOS' as data_source
from ods_wmos.asn;



-----shiyang
select 
null as wms_company_id,
companycode as wms_company_name,
null as wms_warehouse_id,
warehousecode as wms_warehouse_name,
id as internal_id,
code as asn_id, 
null as wms_asn_status,
null as std_asn_status,
null as customer_asn,
created as create_time,
actualarrivedate as receipt_time,-- comment fuji项目 字段名actualarrivedatetime
closedat as closed_date,
lastupdated as  last_modified_date,
null as data_source
from ods_ttx_%xxx%.ods_ttx_%xxx%_receipt_header



-----siming
 

set mapred.job.queue.name=dsc;
set hive.execution.engine=tez;
select 
NULL as wms_company_id,
'%xxx%' as wms_company_name,
NULL as wms_warehouse_id,
warehouse as wms_warehouse_name, 
internal_receipt_num as internal_id,
receipt_id as asn_id,

-- case leading_sts 
--         when '100' then 'Open' 
--         when '300' then 'InTransit'
--         when '900' then 'Receiving Started' ??
-- or trailing_sts 100, 200, 300, 301, 900
-- end as wms_asn_status,
NULL as wms_asn_status,
NULL as std_asn_status,
NULL as customer_asn,
CREATION_DATE_TIME_STAMP as create_time,
RECEIPT_DATE as receipt_time,
CLOSE_DATE as closed_date,
DATE_TIME_STAMP as last_modified_date,
'SCALE_%xxx%' as data_source
from ods_cn_bose.receipt_header;

---leading_sts ,trailing_sts


"""
************************
---inb2
dwd_wh_dsc_inbound_header_dtl_di
************************
"""
---crystal
select
    details.TC_COMPANY_ID as wms_company_id, 
    '' as wms_company_name,
    asn.DESTINATION_FACILITY_ALIAS_ID as wms_warehouse_id, 
    '' as wms_warehouse_name,
    '' as internal_id,
    details.asn_id as asn_id, 
    details.asn_detail_id as asn_line_id,
	case asn_detail_status 
        when '4' then 'Accepted' 
        when '8' then 'Rejected'
        when '12' then 'Requires Review'
        when '16' then 'Receiving Started'
        when '20' then 'Receiving Complete'
        when '24' then 'Not Received'
        when '28' then 'Refused'
    end as asn_status,
    details.sku_id as sku_code, 
    details.sku_name as sku_name, 
    item.description as sku_desc,
    item.item_bar_code as sku_barcode,
    details.BATCH_NBR as batch_number,
    '' as serial_number,
	'' as vendor_sku_barcode,
	'' as product_category,
	'' as product_status,
	'' as original_qty,	
	details.RECEIVED_QTY/nvl(item.STD_BUNDL_QTY,1) as receive_qty,
	nvl(details.RECEIVED_QTY,0)*item.UNIT_VOLUME/nvl(item.STD_BUNDL_QTY,1) as receive_volume,
	item.UNIT_WEIGHT as receive_weight,
	'' as quantity_um,
	'' as volume_um,
	'' as weight_um,
	'' as std_receive_volume,
	'' as std_receive_weight,
	details.expire_date as expiry_date,
	details.CREATED_DTTM as create_time, 
	asn.last_receipt_dttm as receipt_time,
	asn.verified_dttm  as closed_date,
    details.LAST_UPDATED_DTTM as last_modified_date,
    'wmos' as data_source
from ods_wmos.asn_detail details
left join ods_wmos.asn asn on asn.asn_id = details.asn_id AND details.TC_COMPANY_ID = asn.TC_COMPANY_ID
left join ods_wmos.item_cbo item on details.sku_id = item.item_id and details.TC_COMPANY_ID = item.COMPANY_ID
where details.inc_day = '20210615' and asn.inc_day='20210615' and item.inc_day = '20210615'

----shiynag
select 
null as wms_company_id,
RH.companycode as wms_company_name,
null as wms_warehouse_id,
RH.warehousecode as wms_warehouse_name,
RH.id as internal_id,
RH.code as asn_id, 
RD.id as asn_line_id,
rd.itemCode as sku_code,
rd.itemName as sku_name,
null as sku_desc,
itemBarcode.barcode as sku_Barcode,
RD.batch as batch_number,
SN.serialNumber as serial_number,
null as vendor_sku_barcode,
IT.itemcategory1 as product_categoty,
null as product_status,
RD.totalQty as original_qty,
RD.totalQty - RD.openQty as receive_qty,
RH.totalVolume as receive_volume,
RH.totalWeight as receive_weight,
null as quantity_um,
null as volume_um,
null as weight_um,
null as std_receive_volume,
null as std_receive_weight,
RD.expirationDate as expiry_date,
RH.created as create_time,
RH.closedAt as closed_date,
RH.actualarrivedate as receipt_time,-- comment %xxx%项目 字段名actualarrivedatetime
RH.lastupdated as last_modified_date,
null as data_source
from ods_ttx_%xxx%.ods_ttx_%xxx%_receipt_header RH
inner join ods_ttx_%xxx%.ods_ttx_%xxx%_receipt_detail RD on RH.id = RD.receiptId
inner join ods_ttx_%xxx%.ods_ttx_%xxx%_item IT on RD.itemCode = IT.code and rd.companyCode = IT.companyCode
left join ods_ttx_%xxx%.ods_ttx_%xxx%_serial_number SN on RD.id = SN.receiptId and RD.companyCode = SN.companyCode and RD.warehouseCode = SN.warehouseCode and RD.itemCode = SN.itemCode
left join ods_ttx_%xxx%.ods_ttx_%xxx%_item_barcode as itemBarcode on IT.code = itemBarcode.itemCode and IT.companyCode = itemBarcode.companyCode
limit 10


----siming

select 
null as wms_company_id,
company as wms_company_name,
null as wms_warehouse_id,
RH.WAREHOUSE as wms_warehouse_name,
RH.INTERNAL_RECEIPT_NUM as internal_id,
RH.RECEIPT_ID as asn_id,
null as asn_line_id,
rd.item as sku_code,
rd.item as sku,
rd.ITEM_DESC as sku_desc,
null as sku_Barcode,
null as batch_number,
null as serial_number,
null as vendor_sku_barcode,
rd.item_category1 as product_categoty,
rc.status as product_status,
rd.original_total_quantity as original_qty,
RD.TOTAL_QTY - RD.OPEN_QTY as Quantity_Received,
RH.TOTAL_VOLUME as receive_volume,
RH.TOTAL_WEIGHT as receive_weight,
rd.quantity_um as quantity_um,
null as volume_um,
rd.weight_um as weight_um,
null as std_receive_volume,
null as std_receive_weight,
rd.expiration_date_time as expiry_date,
RH.CREATION_DATE_TIME_STAMP as create_time,
rh.close_date as closed_date,
RH.receipt_date as receipt_time,-- comment %xxx%项目 字段名actualarrivedatetime
RH.date_time_stamp as last_modified_date,
RH.CREATION_DATE_TIME_STAMP as Created_Date
from 
ods_cn_bose.receipt_header RH 
inner join ods_cn_bose.receipt_detail RD 
on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
inner join ods_cn_bose.item as itm
on RD.item = itm.item 
and rd.COMPANY = itm.company
left join ods_cn_bose.receipt_container RC 
on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
and RD.company = RC.company





"""
************************
---inb3
dwd_wh_dsc_inbound_header_dtl_di
************************
"""
-------------------------------------------
----dim_dsc_storage_location_info

----shiyang
set mapred.job.queue.name=dsc;
set hive.execution.engine=tez;

select 
null as wms_company_id,
rc.company as wms_company_name,
null as wms_warehouse_id,
RH.warehouse as wms_warehouse_name,
RH.INTERNAL_RECEIPT_NUM as internal_id,
RH.RECEIPT_ID as asn_id, 
null as asn_line_id,

rd.item as sku_code,
rd.item as sku,
rd.ITEM_DESC as sku_desc,
null as sku_Barcode,
null as batch_number,
null as serial_number,
null as vendor_sku_barcode,

wms_company_id
wms_company_name
wms_warehouse_id
wms_warehouse_name
internal_id
asn_id
asn_line_id
sku_code
sku_name
sku_desc
sku_barcode
batch_number
serial_number
vendor_sku_barcode
order_id
order_line
lpn_staus
wave_number
pick_no
PickNo_create_date
required_ship_date
inventory_status
lpn number
lpn type
lpn size
from_location
ship_to_location
lpn_shipped_qty
lpn_shipped_volume
lpn_shipped_weight
quantity_um
volume_um
weight_um
std_lpn_shipped_volume
std_lpn_shipped_weight
closed_date
create_date
last_modified_date
data_source





"""
************************
oubound1
dwd_wh_dsc_outbound_header_dtl_di

************************
"""
-------------------------------------------
----dim_dsc_storage_location_info

----shiyang
set mapred.job.queue.name=dsc;
set hive.execution.engine=tez;

select
null as wms_company_id	,
SH.companycode as wms_company_name,
null as	wms_warehouse_id	,
SH.warehousecode as	wms_warehouse_name	,
SH.shipmentid as shipment_id	,
null as	delivery_type,
SH.shipto	as	customer_code	,
SH.shiptoname	as	customer_name,
SH.erp_Order	as	purchase_order_no	,
SH.sourceOrderCode	as	customer_delivery_no	,
null	as	carrier_id	,
SH.carriercode	as	carrier_name	,
SH.code	as	order_id	,
CR.status	as	order_status	,
null	as	order_type	,
SH.requesteddeliverydate	as	required_ship_date	,
SH.scheduledshipdate	as	schedule_date	,
SH.actualshipdatetime	as	start_ship_date	,
SH.actualdeliverydate	as	stop_ship_date	,
SH.carrierService	as	ship_via	,
SH.ship_To	as	ship_to_customer_name	,
SH.shiptocity	as	ship_to_city	,
SH.shipToState	as	ship_to_state	,
SH.shiptopostalcode	as	ship_to_zip	,
SH.shiptocountry	as	ship_to_country	,
SH.shipToAddress1 as	ship_to_adress	,
null as	closed_date	,
SH.created	as	create_date	,
SH.lastUpdated	as	last_modified_date	,
null	as	data_source	
from ods_ttx_%xxx%.ods_ttx_%xxx%_receipt_header SH
inner join ods_ttx_%xxx%.carrier CR on SH.carrierCode = CR.code
limit 100;



---siming

select
null as wms_company_id,
'%xxx%' as wms_company_name,
null as	wms_warehouse_id,
SH.WAREHOUSE as	wms_warehouse_name,
SH.shipment_id as shipment_id,
sH.CARRIER_TYPE as delivery_type,
SH.ship_to as customer_code,
sd.customer as customer_name,
SH.erpOrderCode	as	purchase_order_no,
SH.customer_po	as	customer_delivery_no,
null as	carrier_id,
SH.CARRIER as carrier_name,
sc.status	as	order_status,
sd.ORDER_TYPE as order_type,
SH.REQUESTED_DELIVERY_DATE	as	required_ship_date,
SH.SCHEDULED_SHIP_DATE	as	schedule_date,
SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
null as	ship_via,
SH.shIP_TO_NAME as	ship_to_customer_name,
SH.SHIP_TO_CITY as ship_to_city	,
SH.ship_to_state as	ship_to_state	,
SH.ship_to_postal_code	as	ship_to_zip	,
SH.ship_to_country	as	ship_to_country	,
SH.ship_to_adress1 as ship_to_adress,
SH.ACTUAL_DELIVERY_DATE_TIME as	closed_date	,
SH.CREATION_DATE_TIME_STAMP	as	create_date	,
SH.DATE_TIME_STAMP	as	last_modified_date	,
'%xxx%' as data_source	
FROM
shipment_header SH inner join shipment_detail sd on SH.SHIPMENT_ID = sd.SHIPMENT_ID
inner join item on sd.ITEM = item.item 
left join SHIPPING_CONTAINER sch on SH.INTERNAL_SHIPMENT_NUM = sch.INTERNAL_SHIPMENT_NUM
 


 ----
SH.warehouse as 'Warehouse',
(SELECT TOP 1 SAR.FROM_WORK_ZONE FROM SHIPMENT_ALLOC_REQUEST SAR WHERE SAR.INTERNAL_SHIPMENT_LINE_NUM=SD.INTERNAL_SHIPMENT_LINE_NUM) AS WhNo,
SH.company as 'Company',
SH.SHIPMENT_ID as 'Shipment ID',
SH.ERP_ORDER as 'Customer Order Number',
SH.ORDER_TYPE as 'Order Type',
SH.REQUESTED_DELIVERY_TYPE as 'Requested Delivery Type',
SH.CARRIER_TYPE as 'Carrier Type',
SH.CUSTOMER_CATEGORY1 AS Shpt, 
Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time) AS Truck_Date,
SH.WAREHOUSE,
SH.ship_To as 'Ship To Customer Number',
SH.SHIP_TO_ADDRESS1 as 'Ship to Address 1',
SH.SHIP_TO_CITY as 'Ship to City',
SH.ship_To_State as 'Ship to State',
sd.item as 'SKU',
item.ITEM_STYLE as 'SKU Style',
item.ITEM_COLOR as 'SKU Color',
item.ITEM_SIZE as 'SKU Size Description',
item.DESCRIPTION as 'SKU Description',
sd.STATUS1 as 'Inventory Status',
sch.INTERNAL_CONTAINER_NUM as 'Carton Number',
sd.REQUESTED_QTY as 'Original Order Qty',
sd.TOTAL_QTY as 'Total Quantity',
sd.TOTAL_WEIGHT as 'Total_weight',
sd.TOTAL_VOLUME as 'Total_volume',
sch.CONTAINER_TYPE as 'Carton Type',
sch.Volume as 'Carton Size'
FROM
shipment_header SH inner join shipment_detail sd on SH.SHIPMENT_ID = sd.SHIPMENT_ID
inner join item on sd.ITEM = item.item 
left join SHIPPING_CONTAINER sch on SH.INTERNAL_SHIPMENT_NUM = sch.INTERNAL_SHIPMENT_NUM
 




"""
************************
item
dim_dsc_item_info
************************
"""
-------------------------------------------
----dim_dsc_item_info

---- crystal

select 
ic.company_id as wms_company_id,
'' as wms_company_name,
ic.item_id as sku_code,
ic.item_name as sku_name,
ic.description as sku_desc,
ic.status_code as sku_status,
iw.prod_line as product_line,
iw.prod_catgry as product_categoty,
'' as product_group,
iw.prod_sub_grp as product_sub_group,
ic.prod_type as product_type,
iw.sale_grp as sale_group,
'' as store_department,
'' as conversion_qty,
'' as quantity_um,
ic.unit_length as length,
ic.unit_width as width,
ic.unit_height as height,
ic.unit_weight as weight,
ic.unit_volume as volume,
'' as weight_um,
'' as volume_um,
'' as std_pack_height,
'' as std_pack_length,
'' as std_pack_width,
'' as std_pack_volume,
'' as std_pack_weight,
ic.std_bundl_qty as std_pack_qty,
'' as std_case_height,
'' as std_case_length,
'' as std_case_width,
'' as std_case_volume,
'' as std_case_weight,
'' as std_case_qty,
'' as manuafacture_date,
'' as expiration_date,
'' as putaway_type,
'' as allocation_type,
'' as load_time,
ic.created_dttm as create_date,
ic.last_updated_dttm as last_modified_date,
ic.inc_day,
'wmos' as data_source
from ods_wmos.item_cbo ic 
left join ods_wmos.item_wms iw on iw.item_id = ic.item_id

----shiyang
select
null as wms_company_id,
companycode as wms_company_name,
code as sku_code,
name as sku_name,
null as sku_desc,
status as sku_status,
null as product_line,
itemcategory1 as product_categoty ,null as product_group,null as product_sub_group,null as product_type,
null as sale_group,
null as store_department,
null as conversion_qty,
unitdesc as quantity_um,
unitLength as length,
unitWidth as width,
unitHeight as height,
unitWeight as weight,
unitVolume as volume,
null as weight_um,
null as volume_um,
csheight as std_pack_height,cslength as std_pack_length,cswidth as std_pack_width,csvolume as std_pack_volume,csweight as std_pack_weight,csQty as std_pack_qty,
plHeight as std_case_height,
plLength as std_case_length,
plWidth as std_case_width,
plVolume as std_case_volume,
plWeight as std_case_weight,
plQty as std_case_qty,
trackmanufacturedate as manuafacture_date,trackexpirationdate as expiration_date,
locatingRule as putaway_type,
allocationRule as allocation_type,
null as load_time,
created as create_date,lastupdated as last_modified_date,inc_day,
null as data_source
from ods_ttx_%xxx%.ods_ttx_%xxx%_item


---siming 


select
null as wms_company_id,
'%***%' as wms_company_name,
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
'scale_%xxx%' as data_source
from ods_cn_%xxx%.item as itm
left join ods_cn_%xxx%.item_unit_of_measure as iuom
 

"""
************************
location
dim_dsc_storage_location_info
************************
"""
-------------------------------------------
----dim_dsc_storage_location_info

----shiyang
select id as location_id ,code as location_code,locationtype as location_type,
'' as location_length,
'' as location_width,
'' as location_height,
'' as location_cube,
zonecode as location_zone,
aisle as location_aisle,
'' as location_bay,
'' as location_level,
'' as location_position,
locationclass as location_class,
'' as sku_dedication,
'' as pull_zone,
maxcapacity as max_qty,
'' as location_status,
tracklpn as pick_sequence_number,
warehousecode as warehouse_code,
'' as warehouse_name,
lastupdated as last_modified_date,
'' as data_source
from ods_ttx_%xxx%.ods_ttx_%xxx%_location limit 100

---crystal
select 
        locn_id as location_id,
        locn_brcd as location_type,
        '' as location_type,
        len as location_length,
        width as location_width,
        ht as location_height,
        (len*width*ht) as location_cube,
        putwy_zone as location_zone,
        aisle as location_aisle,
        bay as location_bay,
        lvl as location_level,
        posn as location_position,
        locn_class as location_class,
        sku_dedctn_type as sku_dedication,
        pull_zone as pull_zone,
        '' as max_qty,
        '' as location_status,
        '' as pick_sequence_number,
        whse as warehouse_id,
        '' as warehouse_code,
        '' as warehouse_name,
        last_updated_dttm as last_modified_date,
        'WMOS' as data_source
from ods_mda.locn_hdr limit 10;

-- siming
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
'scale_%xxx%' as data_source
from ods_cn_%xxx%.location
limit 5

---
where inc_day='20210629' and DATE_FORMAT(lastupdated, 'yyyy-MM-dd')='2019-05-01'
----


