from pymongo import MongoClient

import certifi
from datetime import datetime

# Function to retrieve reviews of a specific movie
def getMovieReviews(titleID, sortBy=None, filterBy=None, user=None):
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleReviews"]

    # Find specific titleID
    pipeline = [
        {
            "$match": {
                "titleID": titleID,
            }
        }
    ]

    if user != None:
        pipeline[0]["$match"]["reviewName"] = user

    pipeline.append({
        "$group": {
            "_id": {
                "titleID": "$titleID",
                "reviewName": "$reviewName",
                "reviewRating": "$reviewRating",
                "reviewDate": "$reviewDate",
                "review": "$review",
            }
        }
    })

    # If requires sorting
    if sortBy:
        pipeline.append({
            "$sort": sortBy
        })

    # If additional filter
    if filterBy:
        pipeline.append({
            "$match": filterBy
        })

    result = collection.aggregate(pipeline)

    return result


# Send in mandatory titleID, 2 optional sortBy and filterBy
titleID = 1
sortBy = {"_id.reviewDate": -1} 
filterBy = {"_id.reviewRating": {"$lte": '3'}}
reviewer = "cwebb2327"

# Call get review function
reviews = getMovieReviews(titleID, sortBy, filterBy)

# Iterate over the result and print review 
for document in reviews:
    print("Name: \t", document["_id"]["reviewName"])
    print("Rating: ", document["_id"]["reviewRating"])

    review_date = document["_id"]["reviewDate"]
    formatted_date = datetime.strftime(review_date, "%d %B %Y")
    
    print("Date: \t", formatted_date)
    print("Review: ", document["_id"]["review"])
    print()