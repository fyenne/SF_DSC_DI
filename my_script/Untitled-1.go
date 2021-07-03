---
build table in bdp
#---


bg_id		String comment 'OU所属BG的编号'
,bg_code		String comment 'OU所属BG的编码'
,bg_name_cn	String comment 'OU所属BG的中文正式名称'
,bg_name_en	String comment 'OU所属BG的英文正式名称'
,bg_type		String comment 'BG / Proudct (事业部，产品部）'


# ---

cost_center_code		String  comment '成本中心唯一性编码'
,cost_center_name		String  comment '成本中心名称'
,bg_sap_code			String  comment 'BG / 其它产品部'
,overhead				String  comment 'overhead'
,sap_customer			String  comment 'SAP系统中成本中心所属客户'
,sap_location			String  comment 'SAP系统客户地点'
		
#- --- 

ou_code			String comment 'OU的全局唯一可识别编码'		　	　
,ou_name		String comment 'OU名称'　
,bg_id			String comment 'OU所属BG的编号'	　	　
,bg_code		String comment 'OU所属BG的编码'	　	　
,customer_id	String comment 'D365中客户ID'	　	　
,customer_name	String comment '客户名称'	　	　
,warehouse_id	String comment 'OU对应的仓库ID'　	　
,warehouse_code		String comment 'OU对应的仓库Code'	　	　
,warehouse_status	String comment '仓库状态'　	　
,warehouse_type		String comment '仓库类型'　	　
,station_id			String comment 'OU仓库对应的物理仓的ID'　	　
,warehouse_area		String comment 'OU仓库总面积'　	　
,office_area						String comment '办公面积'	
,shared_and_other_areas				String comment '其他公摊面积'
,warehouse_available_cube			String comment '总可用存储容积'
,warehouse_lease_start_date			String comment '租约开始时间'
,warehouse_lease_expiration_date	String comment  '租约结束时间'

# ---
ou_code				String comment 'OU的全局唯一可识别编码'	
,ou_name			String comment 'OU名称'	
,wms_company_id		String comment 'WMS系统中的Company_ID'	
,wms_company_name	String comment 'WMS系统中的Company_Name'	
,wms_warehouse_id	String comment 'WMS系统中的Warehouse_ID'	
,wms_warehouse_name	String comment 'WMS系统中的Warehouse_Name'	
,data_source		String comment 'WMS系统名称'	


# --- ou cost_center_code


cost_center_code		String comment '成本中心唯一性编码'	
,cost_center_name		String comment '成本中心名称'	
,ou_code				String comment 'OU的全局唯一可识别编码'	
,ou_name				String comment 'OU名称'	

