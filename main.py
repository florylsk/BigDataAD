import concurrent.futures
import pandas as pd
import numpy as np
import dask.dataframe as dd
from tqdm import tqdm
import multiprocessing as mp
import time
start = time.time()
df_review=pd.read_csv("review_data.csv",dtype={'review_id':'string','user_id':'string','business_id':'string','rating':'float','useful':'float','fun':'float','cool':'float','description':'string','date':'string'})

df_user=pd.read_csv("user_data.csv",dtype={'user_id':'string','name':'string','num_reviews':'int','user_since':'string','useful':'string','fun':'string','cool':'string','expert':'string'
,'friends':'string','followers':'string','average_rating':'string','like_fashion':'string','like_extras':'string','like_profile':'string','like_format':'string'
,'like_list':'string','like_comment':'string','like_simple':'string','like_cool':'string','like_fun':'string','like_texts':'string','like_pics':'string'})

df_business=pd.read_csv("business_data.csv",dtype={'business_id':'string','name':'string','address':'string','city':'string',
'state':'string','zipcode':'string','lat':'float','long':'float','rating':'float','num_reviews':'string','open':'string'
,'attributes':'string','categories':'string','hours':'string'})

df_check=pd.read_csv("check_data.csv")
df_advice=pd.read_json("advice_data_2.json")

print(df_review)
print(df_user)
print(df_business)
print(df_check)
print(df_advice)
end = time.time()
print("Time elapsed:",end - start," seconds")