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
    upperBound = 15
    availableMovies = []

    # Specify the database and collection name
    stats = mongoDatabase[statsCollection]

    moviesCursor = stats.aggregate([
        {
            "$match": {
                "titleID": {"$gte": lowerBound, "$lte": upperBound}
            }
        },
        {
            "$lookup": {
                "from": "titleSrcs",
                "localField": "titleID",
                "foreignField": "titleID",
                "as": "joinedData"
            }
        },
        {
            "$project": {
                "_id": 0,
                "titleID": 1,
                "description": 1,
                "imageSrc": { "$arrayElemAt": ["$joinedData.imageSrc", 0] }
            }
        },
        {
            "$sort": {
                "titleID": 1
            }
        }
    ])

    movies = list(moviesCursor)

    cursor = mySQLConnection.cursor()

    # Execute a SELECT query
    query = "SELECT title, runtime FROM titleInfo WHERE titleID >= %s AND titleID <= %s"
    params = (lowerBound, upperBound)

    cursor.execute(query, params)

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    for i, row in enumerate(rows):
        if i < len(movies):
            movies[i]["name"] = row[0]
            movies[i]["runtime"] = row[1]
            availableMovies.append(row[0])

    availableMoviesJson = json.dumps(availableMovies)

    context = {'segment': segment, 'movies': movies, 'availableMovies': availableMoviesJson}
    return render(request, 'pages/dashboard.html', context)

def login(request):
    return render(request, 'pages/login.html')

def register(request):
    return render(request, 'pages/register.html')

def movie(request, titleID):
    segment = "movie"

    # Initialise connection for mongoDB
    collection1 = mongoDatabase[statsCollection]
    collection2 = mongoDatabase[reviewsCollection]

    # Find movie description, rating and votes from titleStats table
    movieStats = collection1.find_one(
        {
            "titleID": titleID
        }, 
        {
            "description": 1,
            "rating": 1,
            "votes": 1,
        }
    )

    # Find movie reviewer, reviewDate, reviewRating and reviewContent from titleReviews table
    reviews = collection2.aggregate([
        {
            "$match": {
                "titleID": titleID
            }
        },
        {
            "$project": {
                "_id": 0,
                "reviewName": 1,
                "reviewDate": 1,
                "reviewRating": 1,
                "review": 1,
            }
        }
    ])

    # Convert fetched data to list
    movieReviews = list(reviews)
    
    # Convert the dates from mongoDB datetime to formatted date
    for review in movieReviews:
        reviewDate = review["reviewDate"]
        formattedDate = reviewDate.strftime("%d %b %Y")
        review["reviewDate"] = formattedDate

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title, runtime and yearRelease from titleInfo table
    query = """SELECT title, runtime, yearReleased 
            FROM titleInfo
            WHERE titleID = %s"""
    params = (titleID,)

    # Execute the query
    cursor.execute(query, params)

    # Fetch the specific row
    rows = cursor.fetchone()

    # Close the connection
    cursor.close()

    # Process the fetched row
    movieStats["name"] = rows[0]
    movieStats["runtime"] = rows[1]
    movieStats["imageUrl"] = imageUrl
    movieStats["videoUrl"] = getVideo()

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
