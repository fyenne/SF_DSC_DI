---


set mapred.job.queue.name=root.dsc;
set mapred.job.queue.name=dsc;


--- this is a documnet stores the standard sql from former IT on scale sys.]

-----
---Inventory

select 
SUBSTRING(ITEM.EPC_ITEM_REFERENCE,2,2) PL,
LI.warehouse as 'Warehouse',
LI.COMPANY as 'Company',
LI.INVENTORY_STS as 'Product Status',
SUBSTRING(LI.INVENTORY_STS,1,4) Sloc,
item.ITEM as 'SKU',
item.ITEM_STYLE as 'SKU Style',
item.ITEM_COLOR as 'SKU Color',
item.ITEM_SIZE as 'SKU Size Description',
item.DESCRIPTION as 'SKU Description',
ITEM.ITEM_COLOR INVENTORY_ITEM_ID,
ITEM.ITEM_CATEGORY1 INSPECTION_FLAG,
ITEM.ITEM_CATEGORY2 CONFIG_REQUIRED,
ITEM.ITEM_CATEGORY3 ITEM_GROUP,
ITEM.ITEM_CATEGORY5 SYSTEM_PACK_REQUIRED,
ITEM.ITEM_CATEGORY6 BATTERY_INDICATOR,
ITEM.ITEM_CATEGORY7 ARC_REQUIRED,
ITEM.ITEM_CATEGORY8 ABC_CLASS,
ITEM.ITEM_CATEGORY9 DG_FLAG,
LI.USER_DEF5 RECEIPT_ID,
LI.LOT as 'Batch',
LI.EXPIRATION_DATE as 'Expire Date',
LI.LOCATION as 'Location',
LI.LOGISTICS_UNIT LPN,
LI.PARENT_LOGISTICS_UNIT PALLET_ID,
LI.AGING_DATE,datediff(dd,LI.AGING_DATE,GETDATE()) AS AGING_DAYS,
LI.LOCATION,LB1.VALUE BRAND,
LB2.VALUE COO,
location.LOCATING_ZONE as 'Location Zone',
location.LOCATION_CLASS as 'Location Class',
LI.ON_HAND_QTY as 'Actual QTY',
LI.ALLOCATED_QTY as 'To be Picked',
LI.IN_TRANSIT_QTY as 'To be Filled',
LI.INVENTORY_STS as 'Lock Codes'
FROM LOCATION_INVENTORY LI 
JOIN ITEM ON ITEM.COMPANY = LI.COMPANY AND ITEM.ITEM = LI.ITEM 
full JOIN LOT ON LI.LOT = LOT.LOT AND LI.ITEM = LOT.ITEM AND LI.COMPANY = LOT.COMPANY 
JOIN LOCATION ON LI.LOCATION = LOCATION.LOCATION AND LI.WAREHOUSE = LOCATION.WAREHOUSE 
full Join ITEM_UNIT_OF_MEASURE IUOM  ON LI.ITEM=IUOM.ITEM AND IUOM.QUANTITY_UM='PAT' 
LEFT OUTER JOIN LOT_ATTRIBUTE LB1 ON LOT.OBJECT_ID = LB1.LOT_ID AND LB1.ATTRIBUTE_TEMPLATE_ID = 1  
LEFT OUTER JOIN LOT_ATTRIBUTE LB2 ON LOT.OBJECT_ID = LB2.LOT_ID AND LB2.ATTRIBUTE_TEMPLATE_ID = 2  
where li.LOCATION<>'PECV1' and substring(CONVERT(VARCHAR, LI.DATE_TIME_STAMP), 1, 12)like 'Jun 15 2021%'
----

----activity
 
select
internal_id,
WORK_TYPE,
USER_NAME,
LOCATION,
ITEM,
COMPANY,
QUANTITY,
ACTIVITY_DATE_TIME,
container_id,
direction,
TO_COMPANY,
TO_WAREHOUSE
from TRANSACTION_HISTORY
where 
substring(CONVERT(VARCHAR, ACTIVITY_DATE_TIME), 1, 12) 
like 'Jun 15 2021%'



------


---inbound
select 
RH.WAREHOUSE,
RD.COMPANY,
RH.RECEIPT_TYPE as 'ASN Origin Type',
RH.RECEIPT_ID as 'RECEIPT_ID',
RD.inventory_sts,
RH.ERP_ORDER_NUM as 'Customer Reference',
RD.ERP_ORDER_LINE_NUM as 'PO Line Number',
rd.ITEM_DESC as 'SKU Description',
rd.item as 'SKU',
item.ITEM_STYLE as 'SKU Style',
item.ITEM_COLOR as 'SKU Color',
item.ITEM_SIZE as 'SKU Size Desc',
RD.TOTAL_QTY as 'Original Quantity',
RD.TOTAL_QTY - RD.OPEN_QTY as 'Quantity Received',
RD.QUANTITY_UM,
RH.TOTAL_VOLUME as 'totalVolume',
RH.TOTAL_WEIGHT as 'totalWeight',
RC.CONTAINER_ID as 'LPNs Received',
RH.CREATION_DATE_TIME_STAMP as 'Created Date'
from 
receipt_header RH 
inner join receipt_detail RD on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
inner join item on RD.item = item.item and rd.COMPANY = item.company
left join receipt_container RC on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM and RD.company = RC.company
where substring(CONVERT(VARCHAR, RH .DATE_TIME_STAMP), 1, 12)like 'Jun 15 2021%';


------


---outbound
select 
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
where substring(CONVERT(VARCHAR, SH.DATE_TIME_STAMP), 1, 12)like 'Jun 15 2021%'


------


---item

SELECT
ITEM.ITEM,
DESCRIPTION,
IUOM.QUANTITY_UM,
IUOM.CONVERSION_QTY, 
IUOM.LENGTH,
IUOM.WIDTH,
IUOM.HEIGHT,
IUOM.WEIGHT,
IUOM.USER_DEF8 as 'bose_weight'
FROM item (NOLOCK)
INNER JOIN dbo.ITEM_UNIT_OF_MEASURE (NOLOCK) IUOM
ON ITEM.ITEM = IUOM.ITEM
WHERE IUOM.QUANTITY_UM= 'EA' 
and substring(CONVERT(VARCHAR, item.DATE_TIME_STAMP), 1, 12) 
like 'Jun 15 2021%'










