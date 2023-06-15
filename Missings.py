from pymongo import MongoClient

import certifi

connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(connection, tlsCAFile=certifi.where())

# Specify the database and collection name
db = client["PopcornHour"]
collection = db["titleStats"]

test = list(range(2893))

for row in collection.find():
    test.remove(row['titleID'])

print(len(test))
print(test)


