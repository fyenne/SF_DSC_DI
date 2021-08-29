# ou_whse mapping table
import pandas as pd
import numpy as np
import re

"""
read your own ou_whse mapping table. you can download it from bdp.
dsc_dim.dim_dsc_ou_whse_rel
remember to put the downloaded csv and this script under the same path.
change contents when you see ## 
"""
whse_ou = pd.read_csv('./ou_whse0.csv') ## 
re1 = re.compile(r'(?<=\.).+')
whse_ou.columns = [re1.findall(i)[0] for i in list(whse_ou.columns.to_numpy())]

def search_ou_whse(input):
    """
    input can be a company name (apple; app; michelin; mich...) 
    or a code only int (214 for bose ...)
    """
    try:
        input = int(input)
        code = input
        cod='CN-'+str(code)
        cod2 = 'HK-' + str(code)
        return pd.concat([whse_ou[whse_ou['ou_code'] == cod], whse_ou[whse_ou['ou_code'] == cod2]], axis = 0)
    except:
        whse = input
        cod=str(whse).upper() 
        return whse_ou[whse_ou['ou_name'].str.match(cod)]

"""
change this code>>>
"""
print(search_ou_whse('hp')) ##
# print(search_ou_whse('fas')) ## 