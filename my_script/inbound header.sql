---inbound header
   select 
        NULL as wms_company_id,
        'bose' as wms_company_name,
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
        'scale_bose' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day
    from ods_cn_bose.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_apple_sz' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day
    from ods_cn_apple_sz.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_apple_sh' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_apple_sh.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_costacoffee' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_costacoffee.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_diadora' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_diadora.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_ferrero' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_ferrero.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_fuji' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_fuji.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]'

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_hd' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_hd.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_hp_ljb' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_hp_ljb.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_hpi' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_hpi.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 
    
    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_hualiancosta' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_hualiancosta.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_jiq' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_jiq.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_kone' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_kone.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_michelin' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_michelin.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 


    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_razer' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_razer.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_squibb' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_squibb.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_vzug' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_vzug.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_zebra' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_zebra.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 
       
    union all
    select 
        NULL as wms_company_id,
        company as wms_company_name,
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
        'scale_fas' as data_source
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_scale.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    
