import numpy as np
import pandas as pd
import os
import requests
from lxml import html, etree

import pyspark
import os
import pyspark.sql.types as typ
import pyspark.sql.functions as F
from pyspark.sql.functions import col, asc, desc, split

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("data preprocessing") \
    .config("spark.executor.memory", '8g') \
    .config('spark.executor.cores', '4') \
    .config('spark.cores.max', '4') \
    .config("spark.driver.memory",'8g') \
    .getOrCreate()

sc = spark.sparkContext

links = spark.read.csv("/project/ds5559/group10_reviews/link.csv", header=True)
imdbIds = [i.imdbId for i in links.select('imdbId').distinct().collect()]
df = pd.DataFrame(None, columns=['imdbId','title','year','genres','category','score','description'])


for id in imdbIds:
    if int(id) >= 1000000:
        url = "https://www.imdb.com/title/tt{}/?ref_=fn_al_tt_1".format(id)
    elif int(id) >= 100000:
        url = "https://www.imdb.com/title/tt0{}/?ref_=fn_al_tt_1".format(id)
    elif int(id) >= 10000:
        url = "https://www.imdb.com/title/tt00{}/?ref_=fn_al_tt_1".format(id)
    elif int(id) >= 1000:
        url = "https://www.imdb.com/title/tt000{}/?ref_=fn_al_tt_1".format(id)
    else:
        url = "https://www.imdb.com/title/tt0000{}/?ref_=fn_al_tt_1".format(id)
    
    response = requests.get(url)
    tree = html.fromstring(response.content)
    
    try:
        title = tree.xpath('//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/h1')[0].text
    except:
        title = None

    try:
        year = tree.xpath('//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/div/ul/li[1]/span')[0].text
    except:
        year = None

    try:
        score = tree.xpath('//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[1]/div[2]/div/div[1]/a/div/div/div[2]/div[1]/span[1]')[0].text
    except:
        score = None

    try:
        category = tree.xpath('//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/div/ul/li[2]/span')[0].text
    except:
        category = None
    
    
    genres = []
    for i in range(4,8):
        for j in range(1,3):
            genres_lst = tree.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section[{}]/div[2]/ul[2]/li[{}]/div/ul/li'.format(i,j))
            if genres_lst != []:
                try:
                    for gen in genres_lst:
                        genres.append(gen.find('a').text)
                except:
                    continue 


    try:
        desc = tree.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section[7]/div[2]/div[1]/div[1]/div')[0].text
    except:
        try:
            desc = tree.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section[6]/div[2]/div[1]/div[1]/div')[0].text
        except: 
            try:
                desc = tree.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section[5]/div[2]/div[1]/div[1]/div')[0].text
            except:
                try:
                    desc = tree.xpath('//*[@id="__next"]/main/div/section[1]/div/section/div/div[1]/section[4]/div[2]/div[1]/div[1]/div')[0].text
                except:
                    desc = None

  
    df = df.append({'imdbId': id,
                    'title':title,
                    'year':year,
                    'genres':genres,
                    'category':category,
                    'score':score,
                    'description':desc},
                   ignore_index=True)
    
    
    
df.to_csv('imdb_scrape.csv',index=False)