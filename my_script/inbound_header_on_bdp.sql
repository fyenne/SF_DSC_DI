----inbound header on bdp
 
set mapred.job.queue.name=default;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
----inbound header on bdp
 ----inbound header on bdp##dasd
 

INSERT OVERWRITE TABLE dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di 
partition (inc_day, data_source)
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    src_inc_day,
    inc_day,
    data_source
from (
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,

    src_inc_day,
    inc_day,    
    data_source,
    row_number() over (partition by wms_company_id, wms_warehouse_id, asn_id, data_source order by src_inc_day desc) as rn

from 
(   -- scale

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
        'scale_bose' as data_source,
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
        'scale_apple_sz' as data_source,
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
        'scale_apple_sh' as data_source,
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
        'scale_costacoffee' as data_source,
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
        'scale_diadora' as data_source,
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_diadora.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    data_source,
    src_inc_day,
    inc_day 
    from 
    dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di
    where inc_day in (
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day  
        from ods_cn_bose.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]' 
        ----and data_source = 'scale_bose'
        
        union  
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_apple_sz.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        ----and data_source = 'scale_apple_sz'

        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_apple_sh.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        ----and data_source = 'scale_apple_sh'
        
        union
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day  
        from ods_cn_costacoffee.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]' 
        --and data_source = 'scale_costacoffee'
        
        union  
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_diadora.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_diadora'

    )
) a
) b
where rn = 1;

INSERT OVERWRITE TABLE dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di 
partition (inc_day,data_source)
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    src_inc_day,
    inc_day,
    data_source
 
from (
select  
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    src_inc_day,
    inc_day,    
    data_source,
    row_number() over (partition by wms_company_id, wms_warehouse_id, asn_id, data_source order by src_inc_day desc) as rn

from 
( 
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
        'scale_ferrero' as data_source,
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
        'scale_fuji' as data_source,
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
        'scale_hd' as data_source,
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
        'scale_hp_ljb' as data_source,
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
        'scale_hpi' as data_source,
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_hpi.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 
    union all
    select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    data_source,
    src_inc_day,
    inc_day 
    from 
    dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di
    where inc_day in (
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_ferrero.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_ferrero'
 
        union
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day  
        from ods_cn_fuji.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]' 
        --and data_source = 'scale_fuji'
        
        union  
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_hd.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_hd'

        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_hp_ljb.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_hp_ljb'

        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_hpi.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_hpi'
    )
) a
) b
where rn = 1
;
 

 
INSERT OVERWRITE TABLE dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di partition (inc_day,data_source)
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    src_inc_day,
    inc_day,
    data_source
from (
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,

    src_inc_day,
    inc_day,    
    data_source,
    row_number() over (partition by wms_company_id, wms_warehouse_id, asn_id, data_source order by src_inc_day desc) as rn

from 
(    
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
        'scale_hualiancosta' as data_source,
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
        'scale_jiq' as data_source,
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
        'scale_kone' as data_source,
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
        'scale_michelin' as data_source,
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
        'scale_razer' as data_source,
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_cn_razer.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    data_source,
    src_inc_day,
    inc_day 
    from 
    dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di
    where inc_day in (
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_hualiancosta.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_hualiancosta'

        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_jiq.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_jiq'
        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_kone.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_kone'
        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_michelin.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_michelin'
        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_razer.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_razer'
    )
) a
) b
where rn = 1
;

 
INSERT OVERWRITE TABLE dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di partition (inc_day,data_source)
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    src_inc_day,
    inc_day,
    data_source
from (
select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,

    src_inc_day,
    inc_day,    
    data_source,
    row_number() over (partition by wms_company_id, wms_warehouse_id, asn_id, data_source order by src_inc_day desc) as rn

from 
( 
    
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
        'scale_squibb' as data_source,
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
        'scale_vzug' as data_source,
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
        'scale_zebra' as data_source,
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
        'scale_fas' as data_source,
        inc_day as src_inc_day,
        date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day   
    from ods_scale.receipt_header
    WHERE inc_day = '$[time(yyyyMMdd,-1d)]' 

    union all
    select 
    wms_company_id,
    wms_company_name,
    wms_warehouse_id,
    wms_warehouse_name,
    internal_id,
    asn_id,
    wms_asn_status,
    std_asn_status,
    customer_asn,
    create_time,
    receipt_time,
    closed_date,
    last_modified_date,
    data_source,
    src_inc_day,
    inc_day 
    from 
    dsc_dwd.dwd_wh_dsc_inbound_header_dtl_di
    where inc_day in (
        
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_squibb.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_squibb'
        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_vzug.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_vzug'
        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_cn_zebra.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_zebra'
        union 
        select distinct date_format(CREATION_DATE_TIME_STAMP,'yyyyMMdd') as inc_day 
        from ods_scale.receipt_header
        where inc_day = '$[time(yyyyMMdd,-1d)]'
        --and data_source = 'scale_fas'
    )
) a
) b
where rn = 1
;