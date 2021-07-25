ods_cn_bose--scale_bose -- company name false
ods_cn_apple_sz--scale_apple_sz
ods_cn_apple_sh--scale_apple_sh
ods_cn_costacoffee--scale_costacoffee
ods_cn_diadora--scale_diadora --
ods_cn_ferrero--scale_ferrero
ods_cn_fuji--scale_fuji
ods_cn_hd--scale_hd
ods_cn_hp_ljb--scale_hp_ljb
ods_cn_hpi--scale_hpi
ods_cn_hualiancosta--scale_hualiancosta
ods_cn_jiq--scale_jiq
ods_cn_kone--scale_kone
ods_cn_michelin--scale_michelin
ods_cn_razer--scale_razer
ods_cn_squibb--scale_squibb
ods_cn_vzug--scale_vzug
ods_cn_zebra--scale_zebra -- company name false
ods_scale--scale_fas

WHERE itm.inc_day = '$[time(yyyyMMdd,-1d)]' 
and iuom.inc_day = '$[time(yyyyMMdd,-1d)]'




WHERE rh.inc_day = '$[time(yyyyMMdd,-1d)]' 
and rd.inc_day = '$[time(yyyyMMdd,-1d)]'
and rc.inc_day = '$[time(yyyyMMdd,-1d)]'
 


-- union all
-- SELECT 

--,'scale_cn_zebra' as data_source
--from ods_cn_vzug.shipping_container
-- where inc_day = '$[time(yyyyMMdd,-1d)]'

select * from ods_cn_bose.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_apple_sz.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_apple_sh.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_costacoffee.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_diadora.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_ferrero.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_fuji.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_hd.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_hp_ljb.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_hpi.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_hualiancosta.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_jiq.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_kone.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_michelin.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_razer.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_squibb.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_vzug.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_cn_zebra.shipment_header where inc_day = '$[time(yyyyMMdd,-1d)]' union all
select * from ods_scale.shipment_header 
where inc_day = '$[time(yyyyMMdd,-1d)]'



 

internal_shipment_line_num
,internal_shipment_num
,allocation_rule
,status_flow_name
,erp_order
,status1
,erp_order_line_num
,warehouse
,internal_order_num
,status2
,ship_to
,status3
,launch_num
,status4
,status_failed
,status5
,status6
,status7
,order_type
,status8
,customer
,item
,status9
,status10
,mark_for
,item_desc
,customer_item
,carrier
,carrier_service
,freight_terms
,liability_terms
,order_date
,shipment_id
,company
,interfaced_date
,requested_qty
,quantity_um
,pick_loc
,carrier_type
,pick_zone
,secondary_pick_loc
,weight_um
,secondary_pick_zone
,planned_ship_date
,item_length
,item_width
,requested_delivery_date
,requested_delivery_type
,item_height
,item_dimension_um
,item_department
,item_list_price
,item_net_price
,item_color
,item_style
,item_size
,original_item_ordered
,pick_list_id
,customer_po
,invoice
,item_volume
,packing_category
,total_volume
,hazardous_code
,item_division
,nmfc_code
,total_weight
,catalog_id
,manufacture_id
,total_qty
,value
,merchandise_code
,value_add_label_code
,volume_um
,user_def1
,user_def2
,user_def3
,manually_entered
,user_def4
,user_def5
,user_def6
,user_def7
,user_def8
,user_stamp
,process_stamp
,quantity_at_sts1
,lot_controlled
,date_time_stamp
,serial_num_reqd
,quantity_at_sts2
,catch_weight_reqd
,quantity_at_sts3
,quantity_at_sts4
,quantity_at_sts5
,quantity_at_sts6
,quantity_at_sts7
,quantity_at_sts8
,quantity_at_sts9
,priority
,item_weight
,quantity_at_sts10
,lot
,item_class
,mark_for_name
,mark_for_address1
,mark_for_address2
,mark_for_address3
,mark_for_attention_to
,mark_for_city
,mark_for_state
,mark_for_country
,mark_for_postal_code
,mark_for_phone_num
,mark_for_fax_num
,mark_for_email_address
,harmonized_code
,harmonized_desc
,export_desc
,preference_crit
,producer
,net_cost
,country_of_origin
,packing_class
,interface_record_id
,item_category1
,item_category2
,item_category3
,item_category4
,item_category5
,item_category6
,item_category7
,item_category8
,item_category9
,item_category10
,cont_creation_full_qty
,cont_creation_full_qty_um
,cont_creation_innerpack_qty
,allocate_full_loc_qty
,allow_pct_alloc
,minimum_alloc_pct
,maximum_alloc_pct
,immediate_needs_note
,treat_as_loose
,previous_wave_num
,bom_action
,internal_work_order_num
,related_internal_line_num
,quantity_needed_per_item
,store_distribution
,immediate_needs_eligible
,immediate_needs_loc_rule
,logistics_unit
,parent_logistics_unit
,loc_inv_attributes_id
,allocation_rejected_qty
,eccn
,validated_license
license_exp_date
,'scale_bose' as data_source