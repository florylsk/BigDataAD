import concurrent.futures
import pandas as pd
import numpy as np
import dask.dataframe as dd
from tqdm import tqdm
import multiprocessing as mp
import pandas as pd
import matplotlib.pyplot as plt
import time
import json as js
import gc
from collections import Counter
import numpy as np
from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import f1_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import LinearRegression
from sklearn import metrics
from textblob import TextBlob


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
                df_user = pd.read_csv(filename, dtype={'user_id': 'string', 'name': 'string', 'num_reviews': 'int8',
                                                              'user_since': 'string', 'useful': 'int8', 'fun': 'int8',
                                                              'cool': 'int8', 'expert': 'string'
                    , 'friends': 'string', 'followers': 'int16', 'average_rating': 'float', 'like_fashion': 'int8',
                                                              'like_extras': 'int8', 'like_profile': 'int8',
                                                              'like_format': 'int8'
                    , 'like_list': 'string', 'like_comment': 'int16', 'like_simple': 'int8', 'like_cool': 'int8',
                                                              'like_fun': 'int8', 'like_texts': 'int8',
                                                              'like_pics': 'int8'})


            elif "business_data" in filename:
                nonlocal df_business
                df_business = pd.read_csv(filename,
                                          dtype={'business_id': 'string', 'name': 'string', 'address': 'string',
                                                 'city': 'string',
                                                 'state': 'string', 'zipcode': 'string', 'lat': 'float', 'long': 'float',
                                                 'rating': 'float', 'num_reviews': 'int16', 'open': 'int8'
                                              , 'attributes': 'string', 'categories': 'string', 'hours': 'string'})


            else:
                nonlocal df_check
                df_check = pd.read_csv(filename)

        else:
            nonlocal df_advice
            df_advice = pd.read_json(filename)

    def pregunta_1(df):
        tmpdf = df.drop(['business_id_x', 'user_id', 'review_id', 'business_id_y'], axis=1);del df
        df_all_open = tmpdf[tmpdf.open == 1];del tmpdf
        states = []
        for value in df_all_open.state:
            if value not in states:
                states.append(value)
        all_good_categories=[]
        for state in states:
            df_all_open_nc = df_all_open[df_all_open.state == state]
            df_all_open_nc_good_reviews = df_all_open_nc[df_all_open_nc.rating_y >= 2];del df_all_open_nc
            final_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in df_all_open_nc_good_reviews.items()])).fillna("Nan")
            good_categories = []
            for value in final_df["categories"]:
                tmpString = value
                for category in tmpString.split(", "):
                    if category not in good_categories:
                        good_categories.append(category)
            i = 0
            for value in final_df["categories"]:
                tmpString = value
                for category in tmpString.split(", "):
                    if category in good_categories and final_df.iloc[i]["rating_x"] < 4:
                        good_categories.remove(category)
                i += 1
            del final_df
            print("best categories in ",state,":")
            print(good_categories)
            print(len(good_categories))
            all_good_categories.append(good_categories)

        print(all_good_categories)
        plot_categories(all_good_categories)


    def plot_categories(categories):
        all_categories=[]
        for subarray in categories:
            for word in subarray:
                all_categories.append(word)
        tf = Counter(all_categories)
        y = [count for tag, count in tf.most_common(20)]
        x = [tag for tag, count in tf.most_common(20)]

        plt.bar(x, y, color='crimson')
        plt.title("Term frequencies in the good categories")
        plt.ylabel("Frequency (log scale)")
        plt.yscale('log')  # optionally set a log scale for the y-axis
        plt.xticks(rotation=90)
        for i, (tag, count) in enumerate(tf.most_common(20)):
            plt.text(i, count, f' {count} ', rotation=90,
                     ha='center', va='top' if i < 10 else 'bottom', color='white' if i < 10 else 'black')
        plt.xlim(-0.6, len(x) - 0.4)  # optionally set tighter x lims
        plt.tight_layout()  # change the whitespace such that all labels fit nicely
        plt.show()


    def analize_data(dataframe):
        tmpdf = dataframe.drop(['business_id_x', 'user_id', 'review_id', 'business_id_y'], axis=1);del dataframe
        states=tmpdf.state.unique()
        plt.figure(figsize=(16, 6))
        ax = sns.barplot(x='state', y='rating_y',data=tmpdf)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right", fontsize=8)
        plt.title('Distribution of rating per state', fontsize=14, fontweight='bold')
        plt.show()

        plt.figure(figsize=(16, 6))
        _tmpdf=tmpdf.drop_duplicates(subset=['name'])
        ax = sns.countplot(x='state',data=_tmpdf)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right", fontsize=8)
        plt.title('Number of business per state', fontsize=14, fontweight='bold')
        plt.show()

        # X_cat=tmpdf['categories']
        # X_cat=pd.get_dummies(data=X_cat,drop_first=True)
        # # X_cat.replace(to_replace=pd.NA, value=None, inplace=True)
        # y_cat=tmpdf['rating_y']
        # # y_cat.replace(to_replace=pd.NA, value=None, inplace=True)
        # # del tmpdf
        # X_train_, X_test_, y_train_, y_test_ = train_test_split(X_cat, y_cat, test_size=0.3, random_state=1)
        # # training the algorythm
        #
        # lr_eval_ = LinearRegression()
        # lr_eval_.fit(X_train_, y_train_)
        # # making predictions
        # y_pred_ = lr_eval_.predict(X_test_)
        # df_eval_ = pd.DataFrame({'Actual': y_test_, 'Predicted': y_pred_})
        # print(df_eval_)
        # # calculating the model metrics
        #
        # rmse_eval_ = np.sqrt(metrics.mean_squared_error(y_test_, y_pred_))
        # r2_eval_ = metrics.mean_squared_error(y_test_, y_pred_)
        # print('Mean Absolute Error:', metrics.mean_absolute_error(y_test_, y_pred_))
        # print('Mean Squared Error:', r2_eval_)
        # print('Root Mean Squared Error:', rmse_eval_)

    def sentiment_analysis(df):
        tmpdf = df.drop(['business_id_x', 'user_id', 'review_id', 'business_id_y'], axis=1);del df
        #use business "Be Epic Athletics"
        tmpdf=tmpdf[tmpdf.state=='NC']
        business_polarity = pd.DataFrame(columns=['Business','Polarity'])
        for business in tmpdf['name'].unique():
            businessDF=tmpdf[tmpdf.name==business]
            unique_reviews = businessDF['description'].unique()
            total_polarity = 0
            for review in unique_reviews:
                tmpBlob = TextBlob(review)
                total_polarity += tmpBlob.polarity
            relative_polarity = total_polarity / len(unique_reviews)
            business_polarity=business_polarity.append({"Business": business, "Polarity": relative_polarity}, ignore_index=True)

        #Most negative reviews
        business_polarity=business_polarity.sort_values(by='Polarity')
        polarity_values=business_polarity['Polarity'].head(12)
        business_names=business_polarity['Business'].head(12)
        fig, ax = plt.subplots(figsize=(16, 9))
        ax.barh(business_names, polarity_values)
        for s in ['top', 'bottom', 'left', 'right']:
            ax.spines[s].set_visible(False)
        ax.xaxis.set_ticks_position('none')
        ax.yaxis.set_ticks_position('none')
        ax.xaxis.set_tick_params(pad=5)
        ax.yaxis.set_tick_params(pad=10)
        ax.grid(visible=True, color='grey',
                linestyle='-.', linewidth=0.5,
                alpha=0.2)
        ax.invert_yaxis()
        ax.set_title('Top negative reviewed business',
                     loc='left', )
        plt.show()
        #Most positive reviews
        business_polarity = business_polarity.sort_values(by='Polarity', ascending=False)
        polarity_values = business_polarity['Polarity'].head(12)
        business_names = business_polarity['Business'].head(12)
        fig, ax = plt.subplots(figsize=(16, 9))
        ax.barh(business_names, polarity_values)
        for s in ['top', 'bottom', 'left', 'right']:
            ax.spines[s].set_visible(False)
        ax.xaxis.set_ticks_position('none')
        ax.yaxis.set_ticks_position('none')
        ax.xaxis.set_tick_params(pad=5)
        ax.yaxis.set_tick_params(pad=10)
        ax.grid(visible=True, color='grey',
                linestyle='-.', linewidth=0.5,
                alpha=0.2)
        ax.invert_yaxis()
        ax.set_title('Top positive reviewed business',
                     loc='left', )
        plt.show()

    def concurrent_downloads(filenames):
        # choose the number of threads
        threads = min(MAX_THREADS, len(filenames))
        # map the threads to the main get data function
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            list(tqdm(executor.map(read_file, filenames), total=len(filenames)))

    MAX_THREADS = 25
    df_review = None
    df_user = None
    df_business = None
    df_check = None
    df_advice = None
    start = time.time()
    file_names = {'review_data.csv', 'business_data.csv', 'user_data.csv', 'check_data.csv', 'advice_data_2.json'}
    concurrent_downloads(file_names)
    df_review_reduced=df_review.drop(['date', 'useful', 'fun', 'cool'], axis=1)
    del df_review
    df_user_reduced=df_user.drop(
        ["name", 'num_reviews', 'user_since', 'useful', 'fun', 'cool', 'friends', 'average_rating',
         'like_fashion', 'like_extras', 'like_profile', 'like_format'
            , 'like_list', 'like_comment', 'like_simple', 'like_cool', 'like_fun', 'like_texts',
         'like_pics'], axis=1)
    del df_user
    df_business_reduced=df_business.drop(['lat', 'long', 'address', 'city', 'hours'], axis=1);del df_business
    df_advice_reduced=df_advice.drop(['date','description'], axis=1);del df_advice
    df_check_reduced=df_check.drop('date',axis=1);del df_check
    df_review_user = df_review_reduced.merge(df_user_reduced,how="inner",on="user_id");del df_review_reduced;del df_user_reduced
    print("10%")
    df_review_user_business=df_review_user.merge(df_business_reduced,how='inner',on="business_id");del df_business_reduced;del df_review_user
    print("25%")
    df_review_user_business_check = df_review_user_business.merge(df_check_reduced,how="inner",on="business_id");del df_check_reduced;del df_review_user_business
    print("50%")
    df_review_user_business_check_advice = df_review_user_business_check.merge(df_advice_reduced,how="inner",on="user_id");del df_review_user_business_check;del df_advice_reduced
    gc.collect()
    print("100%")
    print("dataframe fully merged")
    #pregunta_1(df_review_user_business_check_advice)
    analize_data(df_review_user_business_check_advice)
    #sentiment_analysis(df_review_user_business_check_advice)
    del df_review_user_business_check_advice
    end = time.time()
    print("Time elapsed:", end - start, " seconds")

if __name__ == "__main__":
    main()

