---
author: Siming Yan
title: Check SCALE data with OUs
---

<center><span style="color:rgba(255,244,0,1);font-size:22px"><b>_________________________________________________________</b></span>

<center><span style="color:rgba(255,244,0,1);font-size:32px"><b>Check SCALE data with OUs
</b></span>

<center><span style="color:rgba(255,244,0,1);font-size:22px"><b>_________________________________________________________</b></span>



## HPI_WH 

> <span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-214</b></span>

不需要筛选

```sql
# bdp 下载数据
select * 
        from dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di 
        where inc_day between '20210601' and '20210720' 
        and src = 'scale' 
        and wms_warehouse_id = 'HPI_WH'
        
select * 
        from dsc_dwd.dwd_wh_dsc_outbound_line_dtl_di 
        where inc_day between '20210601' and '20210720' 
        and src = 'scale' 
        and wms_warehouse_id = 'HPI_WH'
```



## HPI_SH 

> <span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-035</b></span>

<Albert Huang>

+ 所有66开头的asn_id均为转单号，不计入ou报表的inbound

+ 业务方使用GI时间作为判断，所以BDP中inc_day的时间虽然是6月30号但是GI时间已经到了7月份，未记录在报表中。





## BOSE_SH

><span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-214</b></span>

+ 业务出表逻辑:  
  1. 删除create_date 小于6月的订单 
  2. 某些quantity对不上的订单都来自手工创建订单, 其物料有两个客户料号, 导致quantity数量会乘2, wenbo lu 说这个是可以做成这样的, 因为车厂产品比较特殊. 目前并没有方法去做筛选,比如订单: 2021060801 



## ZEBRA 

> <span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-163</b></span>

<HaiBo Yu>

`inbound`

| asn_id                                                       | reasons                                                      | solutions                                             |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------------------- |
| '45914096', 'ASN58118', 'I202105284033' '46488779', '46789814' | 业务给出时库位为临时库位, 中转库位TMP01                      | 1                                                     |
| 45686511、45914247、46092697、46240746、46301493、46369730、47145247、46109237、46590307、46706112 | 缺失原因不明                                                 | BDP  inb_line中间添加筛选条件product_category is null |
| 46789814,                                                    | 业务给的多一条数据，12qty， 原因是为了传送回去序列号，不涉及实物操作，故比本地多一条数据 | no specific solution                                  |

`outbound`

统计的order数所用字段不一致，业务中用sales_order_num, db对应为中为ERP_number, 计算所用字段为receipt_id





## MICHELIN

> <span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-267</b></span>

筛选手工创建的订单. 

SISC21060501, CTSC21060801, CPP21062602, CPP21062601, CPP3960847, CPP396084701 

+ 订单start with: 

`sisc` `cpp` `LSC`

CTSC21060801 ctsc 这个订单原因不明.



outbound 只需要筛选CPP?

## PERNOD_FAS

> <span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-057</b></span>



ou直接提供DB数据，DB closed_date需要加8小时， 数据需要group by "allocation_zone"

+ outbound 需要筛选掉ou的退货 (shipment id 中含有中文字段的订单)
+ 目前outbound仍然有一天多出27个订单, 情况不明.



## PERNOD_JIQ

> <span style="color:rgba(255,244,0,1);font-size:17px"><b>CN-057</b></span>

