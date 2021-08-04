from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from MergeDataFrameToTable import MergeDFToTable

import numpy as np
import pandas as pd 
from sklearn import cluster


spark = SparkSession.builder.enableHiveSupport().getOrCreate()
"""
outbound_data
"""
#----------------------------
inb_line = spark.sql("""select * 
        from dsc_dws.dws_dsc_wh_ou_daily_kpi_sum 
        where operation_day between '""" + start_date + """' and '""" + end_date + """' 
        and ou_code = 'CN-298' """)

inb_line.show(10,False)


june_hpwh = inb_line.toPandas()
june_hpwh = june_hpwh.dropna(axis = 1, inplace = True, how = 'all')
# june_hpwh.columns = june_hpwh.columns.to_series().str.slice(32).values

#----------------------------
df_shipped_qty = june_hpwh.groupby(['inc_day'])['shipped_qty'].sum()
alg1 = cluster.MiniBatchKMeans(n_clusters = 3, random_state = 707)
hist1 = alg1.fit(df_shipped_qty.to_numpy().reshape(-1,1))
df_shipped_qty = pd.DataFrame(df_shipped_qty).reset_index()

df_shipped_qty['cluster_centers'] = alg1.labels_
cl_1 = pd.concat([pd.DataFrame(hist1.cluster_centers_), pd.Series(np.arange(0,3))], axis = 1)
cl_1.columns = ['cluster_values', 'cluster_centers']

df_shipped_qty = df_shipped_qty.merge(cl_1, on = 'cluster_centers', how = 'inner')

#----------------------------
"""
inbound_data
"""
oub_line = spark.sql("""select * 
        from dsc_dwd.dwd_wh_dsc_inbound_line_dtl_di 
        where inc_day between '20210601' and '20210720' 
        and src = 'scale' 
        and wms_warehouse_id = 'HPI_WH'""")
june_hpwh2 = oub_line.toPandas()

june_hpwh2 = june_hpwh2.dropna(axis = 1, inplace = True, how = 'all')
june_hpwh2.columns = june_hpwh2.columns.to_series().str.slice(31).values


rec_qty = (june_hpwh2.groupby(['inc_day'])['receive_qty'].sum()).to_numpy()
hist2 = alg1.fit(rec_qty.reshape(-1,1))

cl_2 = pd.concat([pd.DataFrame(hist2.cluster_centers_), pd.Series(np.arange(0,3))], axis = 1)
cl_2.columns = ['cluster_values', 'cluster_centers']

df_receive_qty = pd.concat([pd.DataFrame(june_hpwh2.groupby(['inc_day'])['receive_qty'].sum()).reset_index(), 
pd.Series(hist2.labels_)], axis = 1, ignore_index = True)
df_receive_qty.columns = ['inc_day', 'inbound_ttl_qty', 'cluster_centers']

df_receive_qty = df_receive_qty.merge(cl_2, on = 'cluster_centers', how = 'inner')

#----------------------------
"""
hr_data

# select * from 
# dsc_dwd.dwd_hr_dsc_working_hour_dtl_di
# where inc_day between '20210601' and '20210720'
# and ou_code = 'CN-298'
"""
 
# hr data
hr = spark.sql("""select * 
        from dsc_dwd.dwd_hr_dsc_working_hour_dtl_di 
        where inc_day between '20210601' and '20210720' 
        and ou_code = 'CN-298'""")
june_hr = hr.toPandas()

june_hr = june_hr.dropna(axis = 1, inplace = True, how = 'all')
# june_hr.columns =  june_hr.columns.to_series().str.slice(31).values
june_hr['working_date'] = june_hr['working_date'].str.slice(0, -9).values

june_hr_hc = june_hr[june_hr['working_hours'] != 0].groupby(['working_date'])['emp_name'].count()
june_hr_new = june_hr.groupby(['working_date'])['working_hours'].sum()

# weekend adjustment.
# june_hr_new = june_hr_new[june_hr.groupby(['working_date'])['working_hours'].sum() != 0 ]
june_hr_hc = pd.DataFrame(june_hr_hc).reset_index()
june_hr_hc = june_hr_hc[june_hr_hc['working_date']]
june_hr_new = pd.DataFrame(june_hr_new).reset_index()
june_hr_new = june_hr_new[june_hr_new['working_date']]
# alg1 = KMeans(n_clusters = 3)
hist3 = alg1.fit(june_hr_new['working_hours'].to_numpy().reshape(-1,1))

hr_cluster_centers = pd.DataFrame(hist3.cluster_centers_)
hr_cluster_centers['cc_hr'] = [0,1,2]
june_hr_new['cc_hr'] = hist3.labels_

june_hr_ttl = june_hr_new.merge(june_hr_hc, on = 'working_date', how = 'left').fillna(0)
june_hr_ttl['working_date'] = [int(i.replace('-', '')) for i in pd.to_datetime(june_hr_ttl['working_date']).astype(str)]

# values = pd.concat([june_hr_ttl.groupby(['cc_hr'])['working_hours'].std(), 
# june_hr_ttl.groupby(['cc_hr'])['working_hours'].median(),
# june_hr_ttl.groupby(['cc_hr'])['working_hours'].mean()], axis = 1).reset_index()
# values.columns = ['cc_hr','std_cc_hr', 'median_cc_hr', 'mean_cc_hr']
# june_hr_ttl = june_hr_ttl.merge(values, on = 'cc_hr', how = 'left')

june_hr_ttl = june_hr_ttl.merge(hr_cluster_centers, on = 'cc_hr', how = 'left')


"""
concat
"""

full = june_hr_ttl.merge(df_receive_qty, left_on = 'working_date', right_on = 'inc_day', how = 'left').fillna(0)
full = full.merge(df_shipped_qty, left_on = 'working_date', right_on = 'inc_day', how = 'left',suffixes=('_x', '_y')).fillna(0)
full = full.drop(['inc_day_x', 'inc_day_y'], axis = 1)
full = pd.DataFrame(full)
full = full.rename({'emp_name' : 'hc', 'cluster_centers_x': 'inb_cc', 
        'cluster_values_x' : 'inb_cv', 'cluster_centers_y' : 'outb_cc',
        'cluster_values_y' :  'outb_cv', 0 : 'cluster_center_working_hour'}, axis = 1)

value_set = (df_receive_qty['inc_day'].astype(str).append(df_shipped_qty['inc_day'].astype(str)).reset_index())
no_value_set = np.setdiff1d(june_hr_ttl['working_date'].astype(str), value_set['inc_day'])

dates = list(no_value_set.astype(int))
# -----------------------

# np.setdiff1d(mm2['working_date'],dates)
mm_not_null = mm2.loc[mm2['working_date'].isin(np.setdiff1d(mm2['working_date'],dates))]
mm_null = mm2.loc[~mm2['working_date'].isin(np.setdiff1d(mm2['working_date'],dates))]

mm3 = pd.DataFrame(mm_not_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].max()).reset_index()
mm4 = pd.DataFrame(mm_not_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].min()).reset_index()
mm3 = mm3.rename({'working_hours': 'working_hours_max'}, axis = 1)
mm4 = mm4.rename({'working_hours': 'working_hours_min'}, axis = 1)

mm5 = pd.DataFrame(mm_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].max()).reset_index()
mm6 = pd.DataFrame(mm_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].min()).reset_index()
mm5 = mm5.rename({'working_hours': 'working_hours_max'}, axis = 1)
mm6 = mm6.rename({'working_hours': 'working_hours_min'}, axis = 1)


mm_summary = mm_not_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].median().reset_index().merge(
        mm_not_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].mean().reset_index(),
    on =['inb_cc', 'outb_cc'] ,how = 'inner'
    ).merge(
        mm_not_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].quantile(.75).reset_index(),
        on =['inb_cc', 'outb_cc'] ,how = 'inner'
    ).merge(
        mm_not_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].quantile(.6666).reset_index(), 
        on =['inb_cc', 'outb_cc'] ,how = 'inner'
    )

mm_summary.columns = ['inb_cc', 'outb_cc', 'working_hours_median', 'working_hours_mean', 'quantile_75', 'quantile_66']

mm_summary2 = mm_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].median().reset_index().merge(
        mm_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].mean().reset_index(),
    on =['inb_cc', 'outb_cc'] ,how = 'inner'
    ).merge(
        mm_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].quantile(.75).reset_index(),
        on =['inb_cc', 'outb_cc'] ,how = 'inner'
    ).merge(
        mm_null.groupby(['inb_cc', 'outb_cc'])['working_hours'].quantile(.6666).reset_index(), 
        on =['inb_cc', 'outb_cc'] ,how = 'inner'
    )
    
mm_summary2.columns = ['inb_cc', 'outb_cc', 'working_hours_median', 'working_hours_mean', 'quantile_75', 'quantile_66']

mm_not_null = mm_not_null.merge(mm3, how = 'left', on = ['inb_cc','outb_cc'])
mm_not_null = mm_not_null.merge(mm4, how = 'left', on = ['inb_cc','outb_cc'])
mm_not_null = mm_not_null.merge(mm_summary, how = 'left', on = ['inb_cc','outb_cc'])

mm_null = mm_null.merge(mm5, how = 'left', on = ['inb_cc','outb_cc'])
mm_null = mm_null.merge(mm6, how = 'left', on = ['inb_cc','outb_cc'])
mm_null = mm_null.merge(mm_summary2, how = 'left', on = ['inb_cc','outb_cc'])

mm_not_null['dis_to_kernel'] = (mm_not_null['working_hours'] - mm_not_null['cluster_center_working_hour'])
mm_null['dis_to_kernel'] = (mm_null['working_hours'] - mm_null['cluster_center_working_hour'])

mm2 = pd.concat([mm_not_null, mm_null], axis = 0)
mm2 = mm2.rename({'std_cc_hr': 'working_hours_std',  
            'median_cc_hr': 'working_hours_median', 
            'mean_cc_hr': 'working_hours_mean'}, axis = 1)

mm2['year'] = mm2['working_date'].astype(str).str.slice(0,4)
mm2['month'] = mm2['working_date'].astype(str).str.slice(5,6)
mm2['date'] = mm2['working_date'].astype(str).str.slice(6,8)

mm2['inc_day'] = june_hr['inc_day'][1]

df = spark.createDataFrame(mm2)
df.createOrReplaceTempView("mm2")
sql ="""
select
working_date
,working_hours
,cc_hr
,hc
,cluster_center_working_hour
,inbound_ttl_qty
,inb_cc
,inb_cv
,shipped_qty
,outb_cc
,outb_cv
,working_hours_max
,working_hours_min
,working_hours_median
,working_hours_mean
,quantile_75
,quantile_66
,dis_to_kernel
,year
,month
,date
,inc_day
from mm2
"""
df = spark.sql(sql)

merge_data = MergeDFToTable("dsc_dws.dws_inb_outb_working_hour_sum",df, "working_date", "inc_day")
merge_data.merge()