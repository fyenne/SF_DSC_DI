set mapred.job.queue.name=default;
--set hive.execution.engine=tez;
 set hive.exec.dynamic.partition.mode=nonstrict;
------------------------------
------------------------------
------------------------------

INSERT OVERWRITE 
TABLE dsc_dwd.dwd_wh_dsc_outbound_header_dtl_di 
partition (inc_day, data_source)
    SELECT 
        null as wms_company_id,
        'bose' as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        SH.inc_day as src_inc_day,
        case when coalesce(SH.ACTUAL_SHIP_DATE_TIME,'') = then '99991231'
        when date_format(SH.ACTUAL_SHIP_DATE_TIME,'yyyyMMdd') ='$[time(yyyyMMdd,-1d)]' 
        then '$[time(yyyyMMdd,-1d)]' 

        date_format(SH.ACTUAL_SHIP_DATE_TIME,
        'yyyyMMdd') as inc_day,
        'scale_bose' as data_source
        FROM
        dsc.ods_cn_bose.shipment_header SH 
        inner join dsc.ods_cn_bose.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_bose.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 
        
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_apple_sz' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_SHIP_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_apple_sz.shipment_header SH 
        inner join dsc.ods_cn_apple_sz.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_apple_sz.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_apple_sh' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_apple_sh.shipment_header SH 
        inner join dsc.ods_cn_apple_sh.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_apple_sh.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_costacoffee' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_costacoffee.shipment_header SH 
        inner join dsc.ods_cn_costacoffee.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_costacoffee.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

                
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_diadora' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_diadora.shipment_header SH 
        inner join dsc.ods_cn_diadora.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_diadora.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_ferrero' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_ferrero.shipment_header SH 
        inner join dsc.ods_cn_ferrero.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_ferrero.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_fuji' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_fuji.shipment_header SH 
        inner join dsc.ods_cn_fuji.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_fuji.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_hd' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_hd.shipment_header SH 
        inner join dsc.ods_cn_hd.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_hd.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_hp_ljb' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_hp_ljb.shipment_header SH 
        inner join dsc.ods_cn_hp_ljb.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_hp_ljb.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_hpi' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_hpi.shipment_header SH 
        inner join dsc.ods_cn_hpi.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_hpi.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_hualiancosta' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_hualiancosta.shipment_header SH 
        inner join dsc.ods_cn_hualiancosta.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_hualiancosta.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_jiq' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_jiq.shipment_header SH 
        inner join dsc.ods_cn_jiq.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_jiq.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 
       
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_kone' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_kone.shipment_header SH 
        inner join dsc.ods_cn_kone.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_kone.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

       
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_michelin' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_michelin.shipment_header SH 
        inner join dsc.ods_cn_michelin.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_michelin.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        
       
        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_razer' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_razer.shipment_header SH 
        inner join dsc.ods_cn_razer.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_razer.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_squibb' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_squibb.shipment_header SH 
        inner join dsc.ods_cn_squibb.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_squibb.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_vzug' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_vzug.shipment_header SH 
        inner join dsc.ods_cn_vzug.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_vzug.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_zebra' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_cn_zebra.shipment_header SH 
        inner join dsc.ods_cn_zebra.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_cn_zebra.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 

        union all
        SELECT 
        null as wms_company_id,
        sh.company as wms_company_name,
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
        sc.status as order_status,
        sd.ORDER_TYPE as order_type,
        SH.REQUESTED_DELIVERY_DATE as required_ship_date,
        SH.SCHEDULED_SHIP_DATE as schedule_date,
        SH.ACTUAL_SHIP_DATE_TIME as start_ship_date,
        SH.ACTUAL_DELIVERY_DATE_TIME as	stop_ship_date,
        null as	ship_via,
        SH.shIP_TO_NAME as ship_to_customer_name,
        SH.SHIP_TO_CITY as ship_to_city,
        SH.ship_to_state as	ship_to_state,
        SH.ship_to_postal_code as ship_to_zip,
        SH.ship_to_country as ship_to_country,
        SH.ship_to_adress1 as ship_to_adress,
        SH.ACTUAL_SHIP_DATE_TIME as closed_date,
        SH.CREATION_DATE_TIME_STAMP	as create_date,
        SH.DATE_TIME_STAMP	as	last_modified_date,
        'scale_fas' as data_source,
        SH.inc_day as src_inc_day,
        date_format(SH.ACTUAL_ship_DATE_TIME,'yyyyMMdd') as inc_day  
        FROM
        dsc.ods_scale.shipment_header SH 
        inner join dsc.ods_scale.shipment_detail sd 
        on SH.SHIPMENT_ID = sd.SHIPMENT_ID
        left join dsc.ods_scale.SHIPPING_CONTAINER sc 
        on SH.INTERNAL_SHIPMENT_NUM = sc.INTERNAL_SHIPMENT_NUM
        where SH.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and sd.inc_day = '$[time(yyyyMMdd,-1d)]' 
        and SC.inc_day = '$[time(yyyyMMdd,-1d)]' 