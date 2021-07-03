---fact_inbound_line_scale_all

-- WMOS Inbound Line
set mapred.job.queue.name=default;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
env dsc;
-- scale

INSERT OVERWRITE 
TABLE dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di 
Partition(inc_day, data_source)
SELECT 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    asn_line_id,
    asn_status,
    sku_code,
    sku_name,
    sku_desc,
    sku_barcode,
    batch_number,
    serial_number,
    vendor_sku_barcode,
    product_category,
    product_status,
    original_qty,
    receive_qty,
    receive_volume,
    receive_weight,
    quantity_um,
    volume_um,
    weight_um,
    std_receive_volume,
    std_receive_weight,
    `expire_date`,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    src_inc_day,
    inc_day,
    data_source
FROM (
    SELECT 
        wms_company_id,
        wms_company_name,
        wms_warehouse_id,
        wms_warehouse_name,
        internal_id,
        asn_id,
        asn_line_id,
        asn_status,
        sku_code,
        sku_name,
        sku_desc,
        sku_barcode,
        batch_number,
        serial_number,
        vendor_sku_barcode,
        product_category,
        product_status,
        original_qty,
        receive_qty,
        receive_volume,
        receive_weight,
        quantity_um,
        volume_um,
        weight_um,
        std_receive_volume,
        std_receive_weight,
        `expire_date`,
        create_time,
        receipt_time,
        closed_date,
        last_modified_date,
        src_inc_day,
        inc_day,
        data_source,
        row_number() over (partition by wms_company_id, wms_warehouse_id,asn_id, asn_line_id order by src_inc_day desc) as rn
    FROM
    (

    select 
    null as wms_company_id,
    'bose' as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_bose' as data_source
    from 
    ods_cn_bose.receipt_header RH 
    inner join ods_cn_bose.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_bose.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    
    where rd.inc_day = '$[time(yyyyMMdd,-1d)]'


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_apple_sz' as data_source
    from 
    ods_cn_apple_sz.receipt_header RH 
    inner join ods_cn_apple_sz.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_apple_sz.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_apple_sh' as data_source
    from 
    ods_cn_apple_sh.receipt_header RH 
    inner join ods_cn_apple_sh.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_apple_sh.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_costacoffee' as data_source
    from 
    ods_cn_costacoffee.receipt_header RH 
    inner join ods_cn_costacoffee.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_costacoffee.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_diadora' as data_source
    from 
    ods_cn_diadora.receipt_header RH 
    inner join ods_cn_diadora.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_diadora.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_ferrero' as data_source
    from 
    ods_cn_ferrero.receipt_header RH 
    inner join ods_cn_ferrero.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_ferrero.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_fuji' as data_source
    from 
    ods_cn_fuji.receipt_header RH 
    inner join ods_cn_fuji.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_fuji.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_hd' as data_source
    from 
    ods_cn_hd.receipt_header RH 
    inner join ods_cn_hd.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_hd.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_hp_ljb' as data_source
    from 
    ods_cn_hp_ljb.receipt_header RH 
    inner join ods_cn_hp_ljb.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_hp_ljb.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_hpi' as data_source
    from 
    ods_cn_hpi.receipt_header RH 
    inner join ods_cn_hpi.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_hpi.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_hualiancosta' as data_source
    from 
    ods_cn_hualiancosta.receipt_header RH 
    inner join ods_cn_hualiancosta.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_hualiancosta.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 



    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_jiq' as data_source
    from 
    ods_cn_jiq.receipt_header RH 
    inner join ods_cn_jiq.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_jiq.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_kone' as data_source
    from 
    ods_cn_kone.receipt_header RH 
    inner join ods_cn_kone.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_kone.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_michelin' as data_source
    from 
    ods_cn_michelin.receipt_header RH 
    inner join ods_cn_michelin.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_michelin.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_razer' as data_source
    from 
    ods_cn_razer.receipt_header RH 
    inner join ods_cn_razer.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_razer.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_squibb' as data_source
    from 
    ods_cn_squibb.receipt_header RH 
    inner join ods_cn_squibb.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_squibb.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_vzug' as data_source
    from 
    ods_cn_vzug.receipt_header RH 
    inner join ods_cn_vzug.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_vzug.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 
    
    
    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_zebra' as data_source
    from 
    ods_cn_zebra.receipt_header RH 
    inner join ods_cn_zebra.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_cn_zebra.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]'     
    
    union all
    select 
    null as wms_company_name,
    RH.company as wms_company_name,
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
    rd.expiration_date_time as expire_date,
    RH.CREATION_DATE_TIME_STAMP as create_time,
    rh.close_date as closed_date,
    RH.receipt_date as receipt_time,
    RH.date_time_stamp as last_modified_date,
    RH.CREATION_DATE_TIME_STAMP as Created_Date,
    rh.inc_day as src_inc_day,
    date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') 
    as inc_day,
    'scale_fas' as data_source
    from 
    ods_scale.receipt_header RH 
    inner join ods_scale.receipt_detail RD 
    on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
    left join ods_scale.receipt_container RC 
    on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM 
    and RD.company = RC.company
    WHERE rd.inc_day = '$[time(yyyyMMdd,-1d)]' 

    UNION ALL
        SELECT wms_company_id,
        wms_company_name,
        wms_warehouse_id,
        wms_warehouse_name,
        internal_id,
        asn_id,
        asn_line_id,
        asn_status,
        sku_code,
        sku_name,
        sku_desc,
        sku_barcode,
        batch_number,
        serial_number,
        vendor_sku_barcode,
        product_category,
        product_status,
        original_qty,
        receive_qty,
        receive_volume,
        receive_weight,
        quantity_um,
        volume_um,
        weight_um,
        std_receive_volume,
        std_receive_weight,
        `expire_date`,
        create_time,
        receipt_time,
        closed_date,
        last_modified_date,
        src_inc_day,
        inc_day,
        data_source
        from 
        dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di 
        where inc_day in (
            select distinct date_format(RH.CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day
            from ods_cn_apple_sz.receipt_detail RD 
            where RD.inc_day = '$[time(yyyyMMdd,-1d)]'
            

        ) 
    ) il
)  mg
where rn = 1;