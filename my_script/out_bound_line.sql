-- out_bound_line

SELECT 
		o.tc_company_id as wms_company_id
		, O.O_FACILITY_ALIAS_ID as wms_warehouse_id
		, '' as  wms_warehouse_name
		, '' as wms_company_name
		, ic.sku_code as sku_code
		, ic.sku_name as sku_name 
		, ic.sku_desc as sku_desc 
		, '' as sku_barcode
		, oli.batch_nbr as batch_number
		, '' as serial_number
		, '' as vendor_sku_barcode
		, oli.order_id as order_id
		, oli.order_line_id as order_line
		, case o.order_status 
        when '110' then 'Released' 
        when '120' then 'Partially DC Allocated'
        when '130' then 'DC Allocated'
        when '150' then 'Packed'
        when '160' then 'Weighed'
        when '165' then 'Staged'
		when '170' then 'Manifested'
        when '180' then 'Loaded'
        when '185' then 'Partially Shipped'
		when '190' then 'Shipped'
		end as order_staus
		,o.order_type as order_type
		,'' as launch_number
		,'' as pick_no
		,'' as PickNo_create_date
		,oli.planned_ship_date as required_ship_date
		,'' as inventory_status
		,oli.orig_order_qty as original_order_qty
		,oli.allocated_qty as allocated_qty
		,oli.shipped_qty as shipped_qty		
		,nvl(oli.shipped_qty,0)*ic.volume/nvl(ic.std_pack_qty,1) as shipped_volume
		,oli.shipped_weight as shipped_weight
		,'' as quantity_um
		,oli.volume_uom_id as volume_um
		,oli.weight_uom_id as weight_um
		,'' as std_shipped_volume
		,'' as std_shipped_weight
		,'' as Priority
		,oli.actual_shipped_dttm as closed_date
		,oli.created_dttm as create_date
		,oli.last_updated_dttm as last_modified_date
		,'wmos' as data_source
	FROM ods_wmos.order_line_item oli
	LEFT JOIN ods_wmos.orders o ON O.ORDER_ID = OLI.ORDER_ID AND O.TC_COMPANY_ID = OLI.TC_COMPANY_ID
	LEFT JOIN dsc_dim.dim_dsc_item_info ic ON ic.sku_code = oli.item_id 
    WHERE oli.inc_day = '20210630' and o.inc_day ='20210630'  limit 1



----------
     SELECT 
        null as wms_company_id,
        null as	wms_warehouse_id,
        'bose' as wms_company_name,
        SH.WAREHOUSE as	wms_warehouse_name,
        SH.
       
        SH.shipment_id as shipment_id,
        sH.CARRIER_TYPE as delivery_type,
        SH.ship_to as customer_code,
        sd.customer as customer_name,
        SH.erpOrderCode	as	purchase_order_no,
        SH.customer_po	as	customer_delivery_no,
        null as	carrier_id,