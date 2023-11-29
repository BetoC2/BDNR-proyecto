import pymongo
import csv

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['project']
collection = db['flights']
file = 'flight_passengers.csv'

with open(file, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        collection.insert_one(row)

client.close()
