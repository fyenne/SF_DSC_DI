

--inbound line


set mapred.job.queue.name=default;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT OVERWRITE TABLE dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di 
partition (inc_day, data_source)

   select null as wms_company_id,
    RH.company_name as wms_company_name,
    null as wms_warehouse_id,
    RH.WAREHOUSE as wms_warehouse_name,
    RH.INTERNAL_RECEIPT_NUM as internal_id,
    RH.RECEIPT_ID as asn_id,
    null as asn_line_id,
    null as asn_status,
    rd.item as sku_code,
    rd.item as sku_name,
    rd.ITEM_DESC as sku_desc,
    null as sku_barcode,
    null as batch_number,
    null as serial_number,
    null as vendor_sku_barcode,
    rd.item_category1 as product_category,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    RH.receipt_date as receipt_time,
    rh.close_date as closed_date,
    RH.date_time_stamp as last_modified_date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    data_source
from 
    dsc_dwd.dwd_wh_scale_receipt_header_mid_di RH 
    inner join ods_cn_bose.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_bose.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
