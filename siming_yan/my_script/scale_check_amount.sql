

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



----------------------------------------------------------
---outb

