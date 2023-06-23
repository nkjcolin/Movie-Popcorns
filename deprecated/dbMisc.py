from pymongo import MongoClient

import pandas as pd
import certifi

# Function to find movies left out while scraping
def findMissingMovieIDs():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    # Initialise connection with certificate
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleStats"]

    # Specify range of data
    IDs = list(range(21973))

    # Find all documents in database
    for row in collection.find():
        # If document found, delete the titleID
        IDs.remove(row['titleID'])

    # Print missing amount and missing IDs
    print(len(IDs))
    print(IDs)

# Function to remove duplicate reviews when re-scraping
def removeDuplicates():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    # Initialise connection with certificate
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleReviews"]

    # Create aggregate pipline 
    result = collection.aggregate([
        # Find batches at a time since mongoDB limits memory query usage
        {
            "$match": {
                "titleID": { "$gte": 21000, "$lte": 22000 },
            }
        },
        # Group desired details together
        {
            "$group" : { 
                "_id": {
                        "titleID": "$titleID",
                        "reviewName": "$reviewName",
                        "reviewRating": "$reviewRating",
                        "reviewDate": "$reviewDate",
                        "review": "$review",
                },
                # Add unique documents to count
                "count": { "$sum": 1 },
                "ids": { "$addToSet": "$_id" }
            }
        },
        # If there is a document with more than 1 count, means duplicate
        {
            "$match": {
                "_id": { "$ne": None },
                "count": { "$gt": 1 }
            }
        }
    ])

    # Iterate over the result and print the duplicate details and objectID specifically
    for document in result:
        print("Duplicate:", document["_id"])
        print("IDs:", document["ids"])

        # Keep the first objectID and delete the rest
        idsToKeep = document["ids"][0:1]
        idsToDelete = document["ids"][1:]

        # Delete the duplicate documents
        collection.delete_many({"_id": {"$in": idsToDelete}})
        
        # Print deleted documents
        print("Deleted IDs:", idsToDelete)
        print()

# Function to convert "1 January 2000" string field to "2000-01-01" date format and field
def updateReviewDate():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    try:
        # Initialise connection with certificate
        client = MongoClient(connection, tlsCAFile=certifi.where())

        # Specify the database and collection name
        db = client["PopcornHour"]
        collection = db["titleReviews"]

        # Create a loop to automate query instead of sending via batch
        for titleID in range(1, 21973):
            # Update query
            collection.update_one(
            # Find specific titleID to update dates
            {
                "titleID": titleID,
            },
            # Convert string date to numerics for easier updating of DB field type
            [{
                "$set": {
                    # Split "1 January 2000" as day[0], month[1] and year[2]
                    "reviewDate": {
                        # Get day, if is '1', add '0' in front of it. Else, ignore
                        "Day": { 
                            "$cond": {
                                "if": { "$lte": [{ "$strLenCP": { "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 0] } }, 1] },
                                "then": { "$concat": ["0", { "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 0] }] },
                                "else": { "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 0] }
                            }
                        },
                        # Get month, if is 'January', convert to '01' etc..
                        "Month": {
                            "$switch": {
                                "branches": [
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "January"] }, "then": "01" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "February"] }, "then": "02" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "March"] }, "then": "03" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "April"] }, "then": "04" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "May"] }, "then": "05" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "June"] }, "then": "06" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "July"] }, "then": "07" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "August"] }, "then": "08" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "September"] }, "then": "09" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "October"] }, "then": "10" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "November"] }, "then": "11" },
                                    { "case": { "$eq": [{ "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 1] }, "December"] }, "then": "12" }
                                ],
                                "default": ""
                            }
                        },
                        # Get year, final element of the splitted array
                        "Year": { 
                            "$arrayElemAt": [{ "$split": ["$reviewDate", " "] }, 2] 
                        }
                    }
                }
            },
            # Convert date of string type to date type
            {
                "$set": {
                    "reviewDate": {
                        "$dateFromString": {
                            "dateString": {
                                "$concat": [
                                    { "$substr": ["$reviewDate.Day", 0, -1] },
                                    "-",
                                    "$reviewDate.Month",
                                    "-",
                                    { "$substr": ["$reviewDate.Year", 0, -1] }
                                ]
                            },
                            "format": "%d-%m-%Y",
                        }
                    }
                }
            }
            ])

            # Print updated titleIDs for tracking
            print("TitleID: " + str(titleID) + " done.")
            print()

    except Exception as e:
        print(e)

    # Close the MongoDB connection
    client.close()

# Function to convert '1' string field to 1 integer field
def updateRatingInt():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    try: 
        # Initialise connection with certificate
        client = MongoClient(connection, tlsCAFile=certifi.where())

        # Specify the database and collection name
        db = client["PopcornHour"]
        collection = db["titleReviews"]

        # Create a loop to automate query instead of sending via batch
        for titleID in range(2, 21973):
            # Update query
            collection.update_many(
                # Find specific titleID to update dates
                {
                    "titleID": titleID,
                },
                # Convert string date to numerics for easier updating of DB field type
                [{
                    "$set": {
                        "reviewRating": {
                            "$toInt": "$reviewRating"
                        }
                    }
                }]
            )

            # Print updated titleIDs for tracking
            print("TitleID: \t" + str(titleID) + " done.")
            print()

    except Exception as e:
        print(e)

    # Close the MongoDB connection
    client.close()

# Function to insert movie image and video sources into 'titleSrc' table
def insertTitleSrc():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    SRCs = getTitleSrcs()
    titleID = 1

    try: 
        # Initialise connection with certificate
        client = MongoClient(connection, tlsCAFile=certifi.where())

        # for titleID in range(2, 21973):
            # Specify the database and collection name
        db = client["PopcornHour"]
        collection = db["titleSrcs"]

        collection.insert_one({ 
            "_id": titleID,
            "imageSrc": SRCs[titleID][0],
            "videoSrc": SRCs[titleID][1], 
        })
  
        # Print updated titleIDs for tracking
        print("TitleID: \t" + str(titleID) + " done.")
        print()

    except Exception as e:
        print(e)

    # Close the MongoDB connection
    client.close()

# Function to get titleID: imageSrc, videoSrc dictionary
def getTitleSrcs():
    # Open the Excel file
    file = pd.read_excel('titleSrc.xlsx')

    titleSrcs = {}

    # Extract the ID and title details columns
    titleIDs = file[file.columns[0]].values.tolist()
    imageSrcs = file[file.columns[1]].values.tolist()
    videoSrcs = file[file.columns[2]].values.tolist()

    # Assign the lists of image and video sources to the titleIDs dictionary key
    for i in range(len(titleIDs)):
        titleSrcs[titleIDs[i]] = [imageSrcs[i], videoSrcs[i]]

    # Return the dictionary of ID-titleDetail pairs
    return titleSrcs

# Function to count number of reviews per movie
def countReviews():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    # Initialise connection with certificate
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleReviews"]

    lowerBound = 1
    upperBound = 5

    # while upperBound <= 5:
        # Create aggregate pipline 
    result = collection.aggregate([
        {
            "$match": {
                "titleID": {"$gte": lowerBound, "$lte": upperBound},
            }
        },
        {
            "$sort": {
                "reviewDate": -1
            }
        },
        {
            "$group": {
                "_id": "$titleID",
                "reviews": {
                    "$push": {
                        "reviewDate": "$reviewDate",
                    }
                },
                "reviewCount": {
                    "$sum": 1
                },
            }
        }
    ])

    # Iterate over the result and print the reviews by date
    for document in result:
        titleID = document["_id"]
        reviews = document["reviews"]

        print("TitleID:", titleID, "- Review Count:", document["reviewCount"])
        for review in reviews:
            reviewDate = review["reviewDate"]
            print("Review Date:", reviewDate)
        print()

            
        # lowerBound += 3000
        # upperBound += 3000

# Function to keep maximum of 18 reviews and delete oldest reviews
def removeReviews():
    # Set connection
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    # Initialise connection with certificate
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleReviews"]

    lowerBound = 3001
    upperBound = 6000

    while lowerBound <= 24000:
        # Create aggregate pipline 
        result = collection.aggregate([
            {
                "$match": {
                    "titleID": {"$gte": lowerBound, "$lte": upperBound},
                }
            },
            {
                "$sort": {
                    "reviewDate": -1,
                }
            },
            {
                "$group": {
                    "_id": "$titleID",
                    "reviews": {
                        "$push": {
                            "objectID": "$_id",
                            "reviewDate": "$reviewDate",
                        }
                    },
                    "reviewCount": {
                        "$sum": 1
                    },
                }
            },
            {
                "$project": {
                    "titleID": "$_id",
                    "reviewCount": 1,
                    "reviews": {
                        "$filter": {
                            "input": "$reviews",
                            "cond": {"$gt": [{"$indexOfArray": ["$reviews", "$$this"]}, 17]}
                        }
                    }
                }
            },
            {
                "$sort": {
                    "titleID": 1,
                }
            },
        ])

        # Iterate over the result and print the reviews by date
        for document in result:
            titleID = document["titleID"]
            reviewCount = document["reviewCount"]
            reviews = document["reviews"]

            print("TitleID:", titleID, "- Review Count:", reviewCount)
            for review in reviews:
                objectID = review["objectID"]
                collection.delete_one({"_id": objectID})
                print("Delete Object ID: ", objectID)
            print()

        lowerBound += 3000
        upperBound += 3000
            
# # Open the Excel file
# file = pd.read_excel('titleDataset.xlsx')

# # Search for the row with the given movieID
# movieRow = file.loc[file[file.columns[0]] == 13]

# # Extract the ID and title details columns from the movieRow
# casts = movieRow[file.columns[8]].values[0]

# # Remove brackets and single quotes
# casts = casts.strip("[]").replace("'", "")

# # Separate the names
# names = [name.strip() for name in casts.split(',')]

# # Print the separated names
# for name in names:
#     print(name)
# insertTitleSrc()
findMissingMovieIDs()

