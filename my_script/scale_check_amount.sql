

----------------------------------------------------------
--- inb
SELECT
sum(tt.ERP_ORDER_LINE_NUM * TOTAL_QTY) as 'orderL*tt_qty',
count(distinct item) as 'item_distinct',
sum(tt.ERP_ORDER_LINE_NUM) as 'tt_qty'
from 
(SELECT RD.DATE_TIME_STAMP,
RH.WAREHOUSE,
       RD.COMPANY,
       RH.RECEIPT_ID,
       RD.ERP_ORDER_LINE_NUM,
       RD.ITEM,
       RD.inventory_sts,
              ITEM.DESCRIPTION,
       RH.RECEIPT_ID_TYPE,
       RC.CONTAINER_ID,
item.ITEM_CATEGORY3 ITEM_GROUP,
item.ITEM_CATEGORY5,
Item. EPC_ITEM_REFERENCE,
       RD.TOTAL_QTY,
       RD.QUANTITY_UM
  FROM RECEIPT_DETAIL RD WITH(NOLOCK)
  JOIN RECEIPT_HEADER RH WITH(NOLOCK)
    ON RD.INTERNAL_RECEIPT_NUM = RH.INTERNAL_RECEIPT_NUM
  JOIN ITEM WITH(NOLOCK)
    ON RD.ITEM = ITEM.ITEM
   AND RD.COMPANY = ITEM.COMPANY
  FULL JOIN RECEIPT_CONTAINER RC
   ON RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM
   AND RD.INTERNAL_RECEIPT_LINE_NUM=RC.INTERNAL_RECEIPT_LINE_NUM
   where substring(CONVERT(VARCHAR, RD.DATE_TIME_STAMP), 1, 12) 
like 'Jun 15 2021%') as tt


----------------------------------------------------------
---sql server select table names
(
Select Name 
FROM SysColumns 
Where id=Object_Id('ITEM')
and name like '%height%'
)


---all column names in a data base
SELECT COLUMN_NAME, TABLE_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME like '%warehouse%'
ORDER BY ORDINAL_POSITION

-------------------------------------------------
---from excel HP 
---inb
select 
sum(TOTAL_QTY) as 'tt_qty',
count(distinct item) as 'item_distinct',
sum(ITEM_WIDTH*ITEM_HEIGHT*ITEM_LENGTH) as 'vol_hpi',
sum(USER_DEF7) as 'vol_bose',
sum(ERP_ORDER_LINE_NUM) as 'tt_odrline'
--count(distinct RECEIPT_ID) as 'id'
from receipt_detail
where 
substring(CONVERT(VARCHAR, RECEIPT_DATE), 1, 12) 
like 'Jun 15 2021%'

and warehouse = 'HPI_WH'

---第二版：
select
sum(tt.ORIGINAL_TOTAL_QUANTITY),count(tt.receipt_id)
from(
select 
RH.RECEIPT_DATE, RH.RECEIPT_ID, RH.TOTAL_QTY, RH.TOTAL_WEIGHT, RH.TOTAL_VOLUME, 
RH.ARRIVED_DATE_TIME, RH.CLOSE_DATE,RD.ITEM,
RD.ORIGINAL_TOTAL_QUANTITY
from
RECEIPT_header as RH
left join receipt_detail as RD
on RD.RECEIPT_ID=RH.RECEIPT_ID
where substring(CONVERT(VARCHAR, RH.RECEIPT_DATE), 1, 12) 
like 'Jun 17 2021%' and 
RH.warehouse='HPI_WH'

) as tt


----------------------------------------------------------
---outb
---oub
select sum(tt.quantity),count(distinct Product_Code),count(distinct dn)
from
(

SELECT SH.SHIPMENT_ID AS DN,SUBSTRING(ITEM.EPC_ITEM_REFERENCE,2,2) PL, SD.ITEM AS Product_Code,SUM(SD.TOTAL_QTY) AS QUANTITY, RIGHT('0000000'+CAST(Convert(varchar,Convert(VARCHAR,SH.INTERNAL_SHIPMENT_NUM)+1) AS nvarchar(50)),8) Ship_id,
(SELECT TOP 1 SAR.FROM_WORK_ZONE FROM SHIPMENT_ALLOC_REQUEST SAR WHERE SAR.INTERNAL_SHIPMENT_LINE_NUM=SD.INTERNAL_SHIPMENT_LINE_NUM) AS WhNo,
SH.CUSTOMER_CATEGORY1 AS Shpt, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time) AS Truck_Date,SH.WAREHOUSE,getdate() as Getdate
FROM  Shipment_HEADER SH WITH(NOLOCK)
  JOIN Shipment_DETAIL SD WITH(NOLOCK)
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
  JOIN ITEM WITH(NOLOCK)
    ON SD.ITEM = ITEM.ITEM
   AND SD.COMPANY = ITEM.COMPANY
 WHERE SH.TRAILING_STS >= '900' 
 and sh.warehouse='HPI_WH'
 and substring(CONVERT(VARCHAR, SH.ACTUAL_SHIP_DATE_time), 1, 12) 
like 'Jun 17 2021%'  
 -- and Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)  >= convert(varchar(10),getdate()-1,120)+' 23:59:00'
GROUP BY SH.WAREHOUSE,SH.SHIPMENT_ID,ITEM.EPC_ITEM_REFERENCE,SD.ITEM,SH.INTERNAL_SHIPMENT_NUM,SD.INTERNAL_SHIPMENT_LINE_NUM,
SH.CUSTOMER_CATEGORY1, SH.ACTUAL_SHIP_DATE_time

) as tt


# outbound 使用了shipment head & shipment detail 两张表，
# 时间戳：SH.ACTUAL_SHIP_DATE_time
 

---bose
---------------
select
sum(tt.ORIGINAL_TOTAL_QUANTITY),count(distinct tt.receipt_id),count(distinct item) as 'item'
from(
select 
RH.RECEIPT_DATE, RH.RECEIPT_ID, RH.TOTAL_QTY, RH.TOTAL_WEIGHT, RH.TOTAL_VOLUME, 
RH.ARRIVED_DATE_TIME, RH.CLOSE_DATE,RD.ITEM,
RD.ORIGINAL_TOTAL_QUANTITY
from
RECEIPT_header as RH
left join receipt_detail as RD
on RD.RECEIPT_ID=RH.RECEIPT_ID
where substring(CONVERT(VARCHAR, RH.CLOSE_DATE), 1, 12) 
like 'Jun 15 2021%' 
and RH.COMPANY='CE'

) as tt



---v3 bose in
select
sum(distinct tt.TOTAL_QTY) as 'sum-qty', 
count(distinct tt.receipt_id) as 'asn', 
count(distinct item) as 'item',
count(distinct tt.CONTAINER_ID) as 'lpn_containerID'
from(
select 
RH.RECEIPT_DATE, RH.RECEIPT_ID, RH.TOTAL_QTY, RH.TOTAL_WEIGHT, RH.TOTAL_VOLUME, 
RH.ARRIVED_DATE_TIME, RH.CLOSE_DATE,RD.ITEM,
RD.ORIGINAL_TOTAL_QUANTITY,
RH.LICENSE_PLATE_ID,
RC.CONTAINER_ID,
RH.COMPANY
from
RECEIPT_header as RH
left join receipt_detail as RD
on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
left join receipt_container as RC
on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM
where substring(CONVERT(VARCHAR, RH.CLOSE_DATE), 1, 12) 
like 'Jun 15 2021%' 
and RH.COMPANY='CE'

) as tt
 
 
 
 
----outb
----0000
select 
sum(erp_order_line_num*TOTAL_QTY) as 'order_lin*qty',
count(distinct item) as 'distct item', 
--sum(total_qty) as 'tt_qty',
sum(ITEM_HEIGHT*ITEM_LENGTH*ITEM_WIDTH) as 'vol_hp',
sum(User_DEF7) as 'vol_bose',
sum(erp_order_line_num) as 'order_line_num'
from Shipment_DETAIL
where 
substring(CONVERT(VARCHAR, ORDER_DATE), 1, 12) 
like 'Jun 15 2021%'
 
select 
sum(total_qty),
sum(tt.erp_order_line_num),
count(distinct item)
from
(
select 
sd.order_date, sd.erp_order_line_num, sd.shipment_id, sd.TOTAL_QTY, 
sd.ITEM,
sh.trailing_sts_date,
sh.actual_ship_date_time,
sh.LAST_STATUS_UPLOADED
from 
Shipment_DETAIL as sd 
  JOIN  Shipment_HEADER SH
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
 WHERE SH.TRAILING_STS >= '900' 
 and substring(CONVERT(VARCHAR, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)), 1, 12) 
like 'Jun 15 2021%'
) as tt


-----
---v3 bose
 

select sum(tt.quantity),count(distinct Product_Code),count(distinct dn)
from
(

SELECT SH.SHIPMENT_ID AS DN,
SUBSTRING(ITEM.EPC_ITEM_REFERENCE,2,2) PL,
SD.ITEM AS Product_Code,SUM(SD.TOTAL_QTY) AS QUANTITY, 
RIGHT('0000000'+CAST(Convert(varchar,Convert(VARCHAR,SH.INTERNAL_SHIPMENT_NUM)+1) AS nvarchar(50)),8) Ship_id,
(SELECT TOP 1 SAR.FROM_WORK_ZONE FROM SHIPMENT_ALLOC_REQUEST SAR WHERE SAR.INTERNAL_SHIPMENT_LINE_NUM=SD.INTERNAL_SHIPMENT_LINE_NUM) AS WhNo,
SH.CUSTOMER_CATEGORY1 AS Shpt, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time) AS Truck_Date,SH.WAREHOUSE,getdate() as Getdate
FROM  Shipment_HEADER SH WITH(NOLOCK)
  JOIN Shipment_DETAIL SD WITH(NOLOCK)
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
  JOIN ITEM WITH(NOLOCK)
    ON SD.ITEM = ITEM.ITEM
   AND SD.COMPANY = ITEM.COMPANY
 WHERE SH.TRAILING_STS >= '900' 
 and SH.COMPANY='CE'
 and substring(CONVERT(VARCHAR, SH.ACTUAL_SHIP_DATE_time), 1, 12) 
like 'Jun 15 2021%'  
 -- and Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)  >= convert(varchar(10),getdate()-1,120)+' 23:59:00'
GROUP BY SH.WAREHOUSE,SH.SHIPMENT_ID,ITEM.EPC_ITEM_REFERENCE,SD.ITEM,SH.INTERNAL_SHIPMENT_NUM,SD.INTERNAL_SHIPMENT_LINE_NUM,
SH.CUSTOMER_CATEGORY1, SH.ACTUAL_SHIP_DATE_time

) as tt


---or

select 
sum(total_qty),
count(distinct tt.SHIPMENT_ID),
count(distinct item)
from
(
select 
sd.order_date, sd.erp_order_line_num, sd.shipment_id, sd.TOTAL_QTY, 
sd.ITEM,
sh.trailing_sts_date,
sh.actual_ship_date_time,
sh.LAST_STATUS_UPLOADED
from 
Shipment_DETAIL as sd 
  JOIN  Shipment_HEADER SH
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
 WHERE SH.TRAILING_STS >= '900' 
 and substring(CONVERT(VARCHAR, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)), 1, 12) 
like 'Jun 15 2021%'
and SH.COMPANY='CE'
) as tt







-- lot_template in apple






--inb apple. 

select 
count(distinct rh.receipt_id) as 'asn',
count(distinct rd.item) as 'item',
sum(distinct rh.TOTAL_QTY) as 'sum-qty', 
count(distinct rc.CONTAINER_ID) as 'lpn_containerID'
from
RECEIPT_header as RH
left join receipt_detail as RD
on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM
left join receipt_container as RC
on RD.INTERNAL_RECEIPT_NUM = RC.INTERNAL_RECEIPT_NUM
where substring(CONVERT(VARCHAR, RH.CLOSE_DATE), 1, 12) 
like 'Jun 15 2021%' 
group by rh.warehouse


select warehouse from receipt_header
where substring(CONVERT(VARCHAR, CLOSE_DATE), 1, 12) 
like 'Jun 17 2021%'
group by warehouse


select USER_DEF5, warehouse from WAREHOUSE





------
---- Oliver qiang
select 
count(distinct rd.receipt_id) as 'asn',  
count(distinct item) as 'item',
sum(rd.ORIGINAL_TOTAL_QUANTITY) as 'qty'
from
RECEIPT_header as RH
left join receipt_detail as RD
on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM

where substring(CONVERT(VARCHAR, RH.CLOSE_DATE), 1, 12) 
like 'Jun 15 2021%' 
group by rh.warehouse
----
select 
count(distinct rd.receipt_id) as 'asn',  
count(distinct item) as 'item',
sum(rd.ORIGINAL_TOTAL_QUANTITY) as 'qty'
from
RECEIPT_header as RH
left join receipt_detail as RD
on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM

where substring(CONVERT(VARCHAR, RH.CLOSE_DATE), 1, 12) 
like 'Jun 16 2021%' 
group by rh.warehouse
-------
select 
count(distinct rd.receipt_id) as 'asn',  
count(distinct item) as 'item',
sum(rd.ORIGINAL_TOTAL_QUANTITY) as 'qty'
from
RECEIPT_header as RH
left join receipt_detail as RD
on RH.INTERNAL_RECEIPT_NUM = RD.INTERNAL_RECEIPT_NUM

where substring(CONVERT(VARCHAR, RH.CLOSE_DATE), 1, 12) 
like 'Jun 17 2021%' 
group by rh.warehouse
---- warehouse inbound


select warehouse from receipt_header
where substring(CONVERT(VARCHAR, CLOSE_DATE), 1, 12) 
like 'Jun 15 2021%'
group by warehouse
select warehouse from receipt_header
where substring(CONVERT(VARCHAR, CLOSE_DATE), 1, 12) 
like 'Jun 16 2021%'
group by warehouse
select warehouse from receipt_header
where substring(CONVERT(VARCHAR, CLOSE_DATE), 1, 12) 
like 'Jun 17 2021%'
group by warehouse

select USER_DEF5, warehouse 
from WAREHOUSE
-----
---oub
select 
count(distinct sh.SHIPMENT_ID) as 'order',
count(distinct sd.ITEM) as 'item',
sum(sd.total_qty) as 'ttqty'
from 
Shipment_DETAIL as sd 
  JOIN  Shipment_HEADER SH
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
 WHERE SH.TRAILING_STS >= '900' 
 and substring(CONVERT(VARCHAR, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)), 1, 12) 
like 'Jun 15 2021%'
group by sh.warehouse

select 
count(distinct sh.SHIPMENT_ID) as 'order',
count(distinct sd.ITEM) as 'item',
sum(sd.total_qty) as 'ttqty'
from 
Shipment_DETAIL as sd 
  JOIN  Shipment_HEADER SH
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
 WHERE SH.TRAILING_STS >= '900' 
 and substring(CONVERT(VARCHAR, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)), 1, 12) 
like 'Jun 16 2021%'
group by sh.warehouse


select 
count(distinct sh.SHIPMENT_ID) as 'order',
count(distinct sd.ITEM) as 'item',
sum(sd.total_qty) as 'ttqty'
from 
Shipment_DETAIL as sd 
  JOIN  Shipment_HEADER SH
    ON SH.INTERNAL_SHIPMENT_NUM = SD.INTERNAL_SHIPMENT_NUM
 WHERE SH.TRAILING_STS >= '900' 
 and substring(CONVERT(VARCHAR, Dateadd(HH, +8, SH.ACTUAL_SHIP_DATE_time)), 1, 12) 
like 'Jun 17 2021%'
group by sh.warehouse



select warehouse from Shipment_HEADER
where substring(CONVERT(VARCHAR, ACTUAL_SHIP_DATE_TIME), 1, 12) 
like 'Jun 15 2021%'
group by warehouse

select warehouse from Shipment_HEADER
where substring(CONVERT(VARCHAR, ACTUAL_SHIP_DATE_TIME), 1, 12) 
like 'Jun 16 2021%'
group by warehouse


select warehouse from Shipment_HEADER
where substring(CONVERT(VARCHAR, ACTUAL_SHIP_DATE_TIME), 1, 12) 
like 'Jun 17 2021%'
group by warehouse


























































































































































































