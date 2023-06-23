import mysql.connector
from .misc import getVideo

from django.shortcuts import render
from pymongo import MongoClient
import json

mySQLConnection = mysql.connector.connect (
    host='34.31.78.127',
    user='root',
    password='ZbSN6ZdPR_eYeH',
    database='db_proj'
)
mongoConnection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(mongoConnection)
mongoDatabase = client["PopcornHour"]
statsCollection = "titleStats"
reviewsCollection = "titleReviews"
srcsCollection = "titleSrcs"


def index(request):
    segment = "dashboard"

    lowerBound = 1
    upperBound = 150
    availableMovies = []

    # Specify the database and collection name
    stats = mongoDatabase[statsCollection]

    moviesCursor = stats.aggregate([
        # Find by titleID
        {
            "$match": {
                "titleID": {"$gte": lowerBound, "$lte": upperBound}
            }
        },
        # Outer left join with titleSrcs to get matching titleID's data 
        {
            "$lookup": {
                "from": "titleSrcs",
                "localField": "titleID",
                "foreignField": "titleID",
                "as": "joinedData"
            }
        },
        # Allow following data to be displayed 
        {
            "$project": {
                "_id": 0,
                "titleID": 1,
                "description": 1,
                "imageSrc": { "$arrayElemAt": ["$joinedData.imageSrc", 0] }
            }
        },
        # Sort the data by titleIDs
        {
            "$sort": {
                "titleID": 1
            }
        }
    ])

    # Convert fetched data to list
    movies = list(moviesCursor)

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title and runtime from titleInfo table
    query = "SELECT title, runtime FROM titleInfo WHERE titleID >= %s AND titleID <= %s"
    params = (lowerBound, upperBound)

    # Execute query
    cursor.execute(query, params)

    # Fetch all the rows
    rows = cursor.fetchall()

    # Assign the name and runtime for each titleID
    for i, row in enumerate(rows):
        if i < len(movies):
            movies[i]["name"] = row[0]
            movies[i]["runtime"] = row[1]
            availableMovies.append(row[0])

    # For search bar
    availableMoviesJson = json.dumps(availableMovies)

    context = {'segment': segment, 'movies': movies, 'availableMovies': availableMoviesJson}
    return render(request, 'pages/dashboard.html', context)

def sorted_movies(request):
    # Get the sorting criterion from the query parameters (e.g., ?sort=title)
    sort_criterion = request.GET.get('sort', 'title')
    print("Sort Criterion:", sort_criterion)
    # Specify the database and collection name
    collection = mongoDatabase[statsCollection]

    moviesCursor = collection.aggregate([
        {
            "$match": {
                "titleID": {"$gte": 1, "$lte": 15}
            }
        },
        {
            "$project": {
                "_id": 0,
                "titleID": 1,
                "description": 1
            }
        }
    ])

    movies = list(moviesCursor)

    cursor = mySQLConnection.cursor()

    upperBound = 15
    lowerBound = 1

    # Execute a SELECT query
    query = """SELECT title, runtime 
            FROM titleInfo
            WHERE titleID >= %s AND titleID <= %s"""
    params = (lowerBound, upperBound)

    cursor.execute(query, params)

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    for i, row in enumerate(rows):
        movies[i]["name"] = row[0]
        movies[i]["runtime"] = row[1]

    # Sort the movies based on the selected criterion
    if sort_criterion == 'title':
        movies.sort(key=lambda x: x['name'])
    # Add more conditions for other sorting criteria if needed

    context = {'segment': 'dashboard', 'movies': movies}
    return render(request, 'pages/sorted_movies.html', context)

def login(request):
    return render(request, 'pages/login.html')

def register(request):
    return render(request, 'pages/register.html')

def movie(request, titleID):
    segment = "movie"

    # Initialise connection for mongoDB
    stats = mongoDatabase[statsCollection]
    reviews = mongoDatabase[reviewsCollection]

    # Find movie description, rating and votes from titleStats table
    statsCursor = stats.aggregate([
        # Find by titleID
        {
            "$match": {
                "titleID": titleID
            }
        },
        # Outer left join with titleSrcs to get matching titleID's data 
        {
            "$lookup": {
                "from": "titleSrcs",
                "localField": "titleID",
                "foreignField": "titleID",
                "as": "joinedData1"
            }
        },
        # Outer left join with titleReviews to get matching titleID's data 
        {
            "$lookup": {
                "from": "titleReviews",
                "localField": "titleID",
                "foreignField": "titleID",
                "as": "joinedData2"
            }
        },
        # Allow following data to be displayed and get it as null if does not exist
        {
            "$project": {
                "_id": 0,
                "noOfVotes": 1,
                "rating": 1,
                "description": 1,
                "imageSrc": {
                    "$ifNull": [{ "$arrayElemAt": ["$joinedData1.imageSrc", 0] }, ""]
                },
                "videoSrc": {
                    "$ifNull": [{ "$arrayElemAt": ["$joinedData1.videoSrc", 0] }, ""]
                },
            }
        },
    ])

    movieReviewsCursor = reviews.aggregate([
        {
            "$match": {
                "titleID": titleID
            }
        },
        # Allow following data to be displayed and get it as null if does not exist
        {
            "$project": {
                "_id": 0,
                "reviewName": 1,
                "reviewDate": 1,
                "reviewRating": 1,
                "review": 1
            }
        }
    ])

    # Convert fetched data to list
    movieStats = list(statsCursor)
    movieReviews = list(movieReviewsCursor)

    # Convert the dates from mongoDB datetime to formatted date
    for review in movieReviews:
        reviewDate = review["reviewDate"]
        formattedDate = reviewDate.strftime("%d %b %Y")
        review["reviewDate"] = formattedDate

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title, runtime and yearRelease from titleInfo table
    query = "SELECT title, runtime, yearReleased FROM titleInfo WHERE titleID = %s"
    params = (titleID,)

    # Execute query
    cursor.execute(query, params)

    # Fetch the specific row
    row = cursor.fetchone()

    # Close the connection
    cursor.close()

    # Structure to separate movie stats and movie reviews
    movieStats = {
        "titleID": titleID,
        "name": row[0],
        "runtime": row[1],
        "votes": movieStats[0]["noOfVotes"],
        "rating": movieStats[0]["rating"],
        "description": movieStats[0]["description"],
        "imageSrc": movieStats[0]["imageSrc"],
        "videoSrc": getVideo(movieStats[0]["videoSrc"]),
    }

    # Send request to HTML page
    context = {'segment': segment, 'movieStats': movieStats, 'movieReviews': movieReviews}
    return render(request, 'pages/movie.html', context)

def actor(request):
    segment = "actor"
    context = {'segment': segment}

    return render(request, 'pages/actor.html', context)

def profile(request):
    segment = "profile"
    context = {'segment': segment}

    return render(request, 'pages/profile.html', context)

def account(request):
    segment = "account"
    context = {'segment': segment}

    return render(request, 'pages/account.html', context)
