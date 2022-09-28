import pandas as pd
import sqlite3


class db:
    def __init__(self,name):
        self.name = name
        self.conn = sqlite3.connect(f'data/{name}.db')

    def insert_data_sql(self,data,collum):
        vdata = data.rename(columns={collum: 'n_citacores'})
        vdata = vdata[['id', 'track_name', 'n_citacores', 'size_bytes', 'prime_genre']]
        vdata.to_sql(name=self.name, con=self.conn)
    def sql_close(self):
        self.conn.close()

    def create_json(self,data,collum):
        vdata = data.rename(columns = {collum:'n_citacores'})
        vdata = vdata[['id','track_name','n_citacores','size_bytes','prime_genre']]
        vdata.to_json(f'data/{self.name}.json',orient='records')

    def create_csv(self,data,collum):
        vdata = data.rename(columns={collum: 'n_citacores'})
        vdata = vdata[['id', 'track_name', 'n_citacores', 'size_bytes', 'prime_genre']]
        vdata.to_csv(f'data/{self.name}.csv')

    def save_all_files(self,data,collum):
        self.insert_data_sql(data,collum)
        self.sql_close()
        self.create_csv(data,collum)
        self.create_json(data,collum)





class AppleStore:
    def __init__(self):
        self.csv = pd.read_csv("AppleStore.csv")
    def find_highest_one(self,val,collum,collum_compare,quant):
        data = self.csv.loc[self.csv[collum]==val].nlargest(quant,collum_compare)
        return data
    def find_highest_two(self,val1,val2,collum,collum_compare,quant):
        data =  self.csv.loc[(self.csv[collum] == val1) | (self.csv[collum] == val2)].nlargest(quant,collum_compare)
        return data


AppleStore = AppleStore()
dbnews = db('news')
dbmusic_book = db('musicbook')

news = AppleStore.find_highest_one("News",'prime_genre','rating_count_tot',1)
music_book = AppleStore.find_highest_two("Music",'Book','prime_genre','rating_count_tot',10)

dbnews.save_all_files(news,'rating_count_tot')
dbmusic_book.save_all_files(music_book,'rating_count_tot')


