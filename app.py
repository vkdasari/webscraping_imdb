import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import logging
import pymongo

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')

link = "https://www.imdb.com/chart/top"

logging.info('Starting Web Scraping from {}'.format(link))

req = requests.get(link)

logging.info('Connection to link {}, is succesful'.format(link))

content = req.content

soup = BeautifulSoup(content, "html.parser")
logging.info('Soup object created')

ratings = soup.find_all("td", class_='ratingColumn imdbRating')

def rating_strip(rating):
    return rating.text.strip()


ratingsList = list(map(lambda i: rating_strip(i), ratings))
logging.info('Ratings extraction completed, {} ratings found'.format(len(ratingsList)))

titleCol = soup.find_all('td', class_='titleColumn')
titleCol = list(map(lambda t:t.text.strip().replace('\n',''),titleCol))

rankingList = list(map(lambda r:re.findall(r'^(\d+)[.]',r)[0], titleCol))
logging.info('Rankings extraction completed, {} records found'.format(len(rankingList)))

yearList = list(map(lambda y:re.findall(r'[(](\d{4})[)]',y)[0],titleCol))
logging.info('Years extraction completed, {} records found'.format(len(yearList)))

titleList = list(map(lambda t: re.findall(r'^\d+[.]\s+(.*)[(]',t)[0],titleCol))
logging.info('Title extraction completed, {} records found'.format(len(titleList)))

top250 = pd.DataFrame()
top250['Rank'] = rankingList
top250['Title'] = titleList
top250['Ýear'] = yearList
top250['Rating'] = ratingsList

logging.info('Scraping completed')
logging.info('+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
logging.info('Data Frame Created')
logging.info('Head of Data Frame:')
logging.info('===================================================================')
#print(top250.head())
#top250.to_csv("imdb.csv", index = None, header=True)
#logging.info('Records extracted to CSV File')



db = 'mongodb://127.0.0.1:27017/'
myClient = pymongo.MongoClient(db)
logging.info('Connecting to MongoDB server at {}'.format(db))

logging.info("List of DBs {}".format(myClient.list_database_names()))
mydb = myClient["imdb"]
logging.info("++++++++++Accessing IMDB Data Base+++++++++++")
myCol = mydb["top250"]
logging.info("++++++++++Accessing top250 collection+++++++++++")
#myCol.insert_many(top250.to_dict('records'))

x = myCol.find({})
logging.info("++++++++++Records+++++++++++")
for i in x:
    print(i)
