set mapred.job.queue.name=default;
--set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
------------------------------
------------------------------
------------------------------


INSERT OVERWRITE TABLE dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di 
partition (inc_day, data_source)

---inbound header
   select 
        NULL as wms_company_id,
        company_name as wms_company_name,
        NULL as wms_warehouse_id,
        warehouse as wms_warehouse_name, 
        internal_receipt_num as internal_id,
        receipt_id as asn_id,
        NULL as wms_asn_status,
        NULL as std_asn_status,
        NULL as customer_asn,
        CREATION_DATE_TIME_STAMP as create_time,
        RECEIPT_DATE as receipt_time,
        CLOSE_DATE as closed_date,
        DATE_TIME_STAMP as last_modified_date,
        data_source,
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day
    from dwd_wh_scale_receipt_header_mid_di
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 
