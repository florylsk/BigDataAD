import concurrent.futures
import pandas as pd
import numpy as np
import dask.dataframe as dd
from tqdm import tqdm
import multiprocessing as mp
import pandas as pd
import matplotlib.pyplot as plt
import time


# df_review=pd.read_csv("review_data.csv",dtype={'review_id':'string','user_id':'string','business_id':'string','rating':'float','useful':'float','fun':'float','cool':'float','description':'string','date':'string'})
# df_user=pd.read_csv("user_data.csv",dtype={'user_id':'string','name':'string','num_reviews':'int','user_since':'string','useful':'int','fun':'int','cool':'int','expert':'string'
# ,'friends':'string','followers':'int','average_rating':'float','like_fashion':'string','like_extras':'string','like_profile':'string','like_format':'string'
# ,'like_list':'string','like_comment':'string','like_simple':'string','like_cool':'string','like_fun':'string','like_texts':'string','like_pics':'string'})
#
# df_business=pd.read_csv("business_data.csv",dtype={'business_id':'string','name':'string','address':'string','city':'string',
# 'state':'string','zipcode':'string','lat':'float','long':'float','rating':'float','num_reviews':'string','open':'string'
# ,'attributes':'string','categories':'string','hours':'string'})
#
# df_check=pd.read_csv("check_data.csv")
# df_advice=pd.read_json("advice_data_2.json")

def main():
    def read_file(filename):
        if "csv" in filename:
            if "review_data" in filename:
                nonlocal df_review
                df_review = pd.read_csv(filename,
                                        dtype={'review_id': 'string', 'user_id': 'string', 'business_id': 'string',
                                               'rating': 'float', 'useful': 'float', 'fun': 'float', 'cool': 'float',
                                               'description': 'string', 'date': 'string'})
            elif "user_data" in filename:
                nonlocal df_user
                df_user = pd.read_csv(filename, dtype={'user_id': 'string', 'name': 'string', 'num_reviews': 'int',
                                                              'user_since': 'string', 'useful': 'int', 'fun': 'int',
                                                              'cool': 'int', 'expert': 'string'
                    , 'friends': 'string', 'followers': 'int', 'average_rating': 'float', 'like_fashion': 'string',
                                                              'like_extras': 'string', 'like_profile': 'string',
                                                              'like_format': 'string'
                    , 'like_list': 'string', 'like_comment': 'string', 'like_simple': 'string', 'like_cool': 'string',
                                                              'like_fun': 'string', 'like_texts': 'string',
                                                              'like_pics': 'string'})
            elif "business_data" in filename:
                nonlocal df_business
                df_business = pd.read_csv(filename,
                                          dtype={'business_id': 'string', 'name': 'string', 'address': 'string',
                                                 'city': 'string',
                                                 'state': 'string', 'zipcode': 'string', 'lat': 'float', 'long': 'float',
                                                 'rating': 'float', 'num_reviews': 'string', 'open': 'string'
                                              , 'attributes': 'string', 'categories': 'string', 'hours': 'string'})
            else:
                nonlocal df_check
                df_check = pd.read_csv(filename)
        else:
            nonlocal df_advice
            df_advice = pd.read_json(filename)

    def concurrent_downloads(filenames):
        # choose the number of threads
        threads = min(MAX_THREADS, len(filenames))
        # map the threads to the main get data function
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            list(tqdm(executor.map(read_file, filenames), total=len(filenames)))

    MAX_THREADS = 25
    df_review = pd.DataFrame
    df_user = pd.DataFrame
    df_business = pd.DataFrame
    df_check = pd.DataFrame
    df_advice = pd.DataFrame
    start = time.time()
    file_names = {'review_data.csv', 'business_data.csv', 'user_data.csv', 'check_data.csv', 'advice_data_2.json'}
    concurrent_downloads(file_names)
    print(df_review)
    print(df_user)
    print(df_business)
    print(df_check)
    print(df_advice)
    end = time.time()
    print("Time elapsed:", end - start, " seconds")

if __name__ == "__main__":
    main()