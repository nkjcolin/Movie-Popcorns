import json
from collections import Counter
from datetime import datetime
from math import sqrt

import mysql.connector
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import Group, User
from django.db import connection
from django.shortcuts import (HttpResponse, HttpResponseRedirect, redirect, render)
from django.utils.html import escapejs
from pymongo import MongoClient

from .forms import EditProfileForm, LoginForm, SignUpForm
from .misc import getVideo
from .models import castMap, genreMap, titleCasts, titleGenres, titleInfo

# MySQL connection settings
mySQLConnection = mysql.connector.connect (
    host='34.31.78.127',
    user='qwesdfxcv',
    password='DM=e7.?bJpY~JgT{',
    database='db_proj'
)

# MongoDB connection settings
mongoConnection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(mongoConnection)
mongoDatabase = client["PopcornHour"]
statsCollection = "titleStats"
reviewsCollection = "titleReviews"
srcsCollection = "titleSrcs"


# Home page that displays movies in descending year released order
def homepage(request):
    # recommended_movies = recommend_movies(request)
    segment = str(request.user.username) + "'s homepage"

    # Define the movies list
    movies = []

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find all movie titles from titleInfo table
    query1 = """
            SELECT title 
            FROM titleInfo 
            """

    # Execute query
    cursor.execute(query1)

    # Fetch all the rows
    allMoviesList = cursor.fetchall()
    
    # Add to movie titles for search bar autocomplete
    movieTitles = [row[0] for row in allMoviesList]

    # Find movie title and runtime from titleInfo table and sort by year and alphabet
    query2 = """
            SELECT ti.titleID, ti.title, ti.runtime
            FROM titleInfo ti
            ORDER BY ti.yearReleased DESC, ti.title ASC
            LIMIT 30
            """

    # Execute query and fetch all the rows
    cursor.execute(query2)
    mysqlList = cursor.fetchall()

    # Get the list of titleIDs from the MySQL result
    titleIDs = [row[0] for row in mysqlList]

    # Specify the database and collection name
    stats = mongoDatabase[statsCollection]

    moviesCursor = stats.aggregate([
        # Find by titleID in the list of titleIDs received from MySQL
        {
            "$match": {
                "titleID": {"$in": titleIDs}
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
                "rating": 1,
                "imageSrc": { "$arrayElemAt": ["$joinedData.imageSrc", 0] }
            }
        }
    ])

    # Convert fetched data to list
    mongoList = list(moviesCursor)

    # For every movie, compile the details together
    for row in mysqlList:
        # Find the corresponding movie in the MongoDB result
        movieData = next(item for item in mongoList if item["titleID"] == row[0])

        # Compiling all details of movie into a dict
        movieDict = {
            "titleID": row[0],
            "name": row[1],
            "runtime": row[2],
            "description": movieData["description"],
            "rating": movieData["rating"],
            "imageSrc": movieData["imageSrc"],
        }

        # Add movie details to movies list for displaying 
        movies.append(movieDict)

    # Close the connection
    cursor.close()

    # For scripts
    availableMoviesJson = escapejs(json.dumps(movieTitles))
    moviesJson = escapejs(json.dumps(movies)) 

    # context = {'segment': segment, 'moviesJson': moviesJson, 'availableMovies': availableMoviesJson, 'recommended_movies': recommended_movies}
    context = {'segment': segment, 'moviesJson': moviesJson, 'availableMovies': availableMoviesJson}
    # context = {'segment': segment, 'moviesJson': moviesJson}
    return render(request, 'pages/homepage.html', context)

# Genre page to display genres to choose from
def genre(request):
    segment = "genre"
    context = {'segment': segment}
    return render(request, 'pages/genre.html', context)

# Function to filter movies by genre selected
def genreSelect(request, genreselection):
    segment = "genreSelect"

    # Define the movies list
    movies = []

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title according to genre choosen
    query = """
            SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
            FROM titleInfo ti, genreMap gm, titleGenres tg
            WHERE ti.titleID = gm.titleID
            AND gm.genreID = tg.genreID
            AND tg.genre = %s
            ORDER BY ti.yearReleased DESC, ti.title ASC
            LIMIT 300
            """

    # Execute query
    cursor.execute(query, (genreselection,))

    # Fetch all the rows
    mysqlList = cursor.fetchall()

    # Specify the database and collection name
    stats = mongoDatabase[statsCollection]

    for row in mysqlList:
        titleID = row[0]
        
        moviesCursor = stats.aggregate([
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
                    "as": "joinedData"
                }
            },
            # Allow following data to be displayed 
            {
                "$project": {
                    "_id": 0,
                    "description": 1,
                    "imageSrc": { "$arrayElemAt": ["$joinedData.imageSrc", 0] }
                }
            }
        ])

        # Convert fetched data to list
        mongoList = list(moviesCursor)

        # Compile all details of movie into a dict then add to movies list
        movieDict = {
            "titleID": row[0],
            "name": row[1],
            "runtime": row[2],
            "yearReleased": row[3],
            "description": mongoList[0]['description'],
            "imageSrc": mongoList[0]['imageSrc'],
        }
        movies.append(movieDict)
    
    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title and runtime from titleInfo table
    query2 = """
            SELECT title 
            FROM titleInfo 
            """

    # Execute query
    cursor.execute(query2)

    # Fetch all the rows
    allMoviesList = cursor.fetchall()

    # Extract movie titles from the rows
    movieTitles = [row[0] for row in allMoviesList]

    # For scripts
    availableMoviesJson = escapejs(json.dumps(movieTitles))
    moviesJson = escapejs(json.dumps(movies)) 

    context = {'segment': segment, 'moviesJson': moviesJson, 'availableMovies': availableMoviesJson}
    return render(request, 'pages/genreSelect.html', context)

# Function to direct either to exact movie page or search for closest results
def movieSearch(request, title):
    # Convert title '_'s to ' 's for queries
    newTitle = title.replace('_', ' ')

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Search for exact title match
    query1 = """
            SELECT titleID
            FROM titleInfo
            WHERE title = %s
            """
    params = (newTitle,)
    cursor.execute(query1, params)
    
    # Get the row to see if exists
    row = cursor.fetchone()

    # If exact title found, go straight to movie page
    if row:
        # Close the cursor
        cursor.close()

        # Send to movie function to get movie details
        return movie(request, row[0])

    # Else, find closest match and view in search page
    else:
         # Search for titles that are title% or %title or %title%
        query2 = """
                SELECT titleID, title, runtime
                FROM titleInfo
                WHERE title LIKE %s OR title LIKE %s OR title LIKE %s
                """
        pattern = '%' + newTitle + '%'
        cursor.execute(query2, (newTitle + '%', '%' + newTitle, pattern))
        
        # Get all the rows of closest match
        mysqlList = cursor.fetchall()

        # Get the list of titleIDs from the MySQL result
        titleIDs = [row[0] for row in mysqlList]

        # Specify the database and collection name
        stats = mongoDatabase[statsCollection]

        moviesCursor = stats.aggregate([
            # Find by titleID in the list of titleIDs received from MySQL
            {
                "$match": {
                    "titleID": {"$in": titleIDs}
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
                    "rating": 1,
                    "description": 1,
                    "imageSrc": { "$arrayElemAt": ["$joinedData.imageSrc", 0] }
                }
            }
        ])

        # Convert fetched data to list
        mongoList = list(moviesCursor)

        # Define the movies list
        movies = []

        # For every movie, compile the details together
        for row in mysqlList:
            # Find the corresponding movie in the MongoDB result
            movieData = next(item for item in mongoList if item["titleID"] == row[0])

            # Compiling all details of movie into a dict
            movieDict = {
                "titleID": row[0],
                "name": row[1],
                "runtime": row[2],
                "rating": movieData["rating"],
                "description": movieData["description"],
                "imageSrc": movieData["imageSrc"],
            }

            # Add movie details to movies list for displaying 
            movies.append(movieDict)

        # Close the cursor
        cursor.close()

        # For scripts
        moviesJson = escapejs(json.dumps(movies)) 

        context = {'segment': 'Search', 'moviesJson': moviesJson, 'searchedTitle': newTitle}
        return render(request, 'pages/movieSearch.html', context)

# Movie page to display selected movie, its details and reviews in descending date order
def movie(request, titleID):
    segment = "movie"

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # If user is logged in, they can review or update review
    if request.user.is_authenticated:
        # Check userMap if userID and titleID exists
        query1 = """
                SELECT *
                FROM userMap
                WHERE userID = %s AND titleID = %s
                """
        params1 = (request.user.id, titleID,)

        # Execute query
        cursor.execute(query1, params1)

        # Fetch the specific row
        row1 = cursor.fetchone()

        # If record exists, user already watched it so update review
        if row1:
            # Set review box to enabled, submitted button shown and watch button hidden
            reviewForm = {
                "reviewBox": "true",
                "reviewLabel": "Write a review for this movie...",
                "submitButton": "block",
                "watchedButton": "none",
            }

            if request.method=='POST':
                reviewName = str(request.user.username)
                reviewRating = request.POST.get('rating')
                review = request.POST.get('review')

                # If submitted rating is not 0, convert the to int value
                if reviewRating:
                    reviewRating = int(reviewRating)
                # If user submits a 0 rating, empty string
                else:
                    reviewRating = 0

                updateReview(request, titleID, reviewRating, review)

        # Else, user have not watched it so insert review
        else:
            # Set review box to disabled, submitted button hidden and watch button shown
            reviewForm = {
                "reviewBox": "false",
                "reviewLabel": "Watch the movie before reviewing...",
                "submitButton": "none",
                "watchedButton": "block",
            }

            if request.method=='POST':
                reviewName = str(request.user.username)
                reviewRating = request.POST.get('rating')
                review = request.POST.get('review')

                # If submitted rating is not 0, convert the to int value
                if reviewRating:
                    reviewRating = int(reviewRating)
                # If user submits a 0 rating, empty string
                else:
                    reviewRating = 0

                insertReview(request, titleID, reviewName, reviewRating, review)

    # If user is not logged in, they are unable to review
    else:
        reviewForm = {
            "reviewBox": "false",
            "reviewLabel": "Log in to review movie...",
            "submitButton": "none",
            "watchedButton": "none",
        }
        
    # Initialize an empty list to store the cast names
    castNames = []
    
    # Check all the castID from castMap of the titleID and get the name from titleCasts
    query2 = """
            SELECT castName
            FROM titleCasts
            WHERE castID IN (
                SELECT castID
                FROM castMap
                WHERE titleID = %s
            )
            ORDER BY castName ASC
            """
    params2 = (titleID,)

    # Execute query
    cursor.execute(query2, params2)

    # Fetch the first row
    row2 = cursor.fetchone()

    # Loop through the rows
    while row2:
        castName = row2[0]
        castNames.append(castName)

        # Fetch the next row
        row2 = cursor.fetchone()


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
        # Find by titleID
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
        },
        # Sort collection by reviewDate
        {
            "$sort": {
                "reviewDate": -1
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
        review["popcornCount"] = range(review["reviewRating"])
        review["popcornCount2"] = range(review["reviewRating"], 10)

    # Find movie title, runtime and yearRelease from titleInfo table
    query = "SELECT title, runtime, yearReleased FROM titleInfo WHERE titleID = %s"
    params = (titleID,)

    # Execute query
    cursor.execute(query, params)

    # Fetch the specific row
    row = cursor.fetchone()

    # Close the connection
    cursor.close()

    # Get latest movie link for displaying (FOR PROJECT PURPOSES AS LINK EXPIRED)
    if movieStats[0]["videoSrc"] == "Video not found":
        videoSRC = ""
    else:
        videoSRC = getVideo(movieStats[0]["videoSrc"])

    # Structure to separate movie stats
    movieStats = {
        "titleID": titleID,
        "name": row[0],
        "runtime": row[1],
        "votes": movieStats[0]["noOfVotes"],
        "rating": movieStats[0]["rating"],
        "casts" : castNames,
        "description": movieStats[0]["description"],
        "imageSrc": movieStats[0]["imageSrc"],
        "videoSrc": videoSRC,
    }

    # Send request to HTML page
    context = {'segment': segment, 'movieStats': movieStats, 'movieReviews': movieReviews, 'reviewForm': reviewForm}
    return render(request, 'pages/movie.html', context)

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
            WHERE titleID >= %s AND titleID <= %s
            """
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

def cast_list(request):
    # Retrieve the list of cast members from the database
    cursor = mySQLConnection.cursor()

    # Execute a SELECT query to fetch the cast members
    query = "SELECT castID, castName FROM titleCasts"
    cursor.execute(query)

    # Fetch all the rows returned by the query
    rows = cursor.fetchall()

    # Create a list of cast members
    cast_members = [{'cast_id': row[0], 'castName': row[1]} for row in rows]

    context = {'segment': 'Casts', 'cast_members': cast_members}
    return render(request, 'pages/cast_list.html', context)

def cast_movies(request, cast_id):
    cast = castMap.objects.get(castID=cast_id)
    title_casts = titleCasts.objects.filter(castID=cast)
    movies = titleInfo.objects.filter(titlecasts__in=title_casts)
    return render(request, 'pages/cast_movies.html', {'movies': movies})

def movie_list_by_cast(request, cast_id):
    # Retrieve movies based on cast ID using raw SQL query
    query = f"""
            SELECT titleInfo.*
            FROM titleInfo
            INNER JOIN titleCasts ON titleInfo.tconst = titleCasts.tconst
            WHERE titleCasts.castID = {cast_id}
            """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    # Create a list of movie objects from the query result
    movies = [{'tconst': row[0], 'primaryTitle': row[1], 'originalTitle': row[2]} for row in rows]

    context = {'segment': 'cast_movies', 'movies': movies}
    return render(request, 'pages/cast_movies.html', context)

# Function to insert new reviews
def insertReview(request, titleID, reviewName, reviewRating, review):
    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Insert into userMap to mark user has watched movie
    query = """
            INSERT INTO userMap(
                userID,
                titleID
            )
            VALUES (%s, %s)
            """
    params = (request.user.id, titleID)

    # Execute query
    cursor.execute(query, params)
    mySQLConnection.commit()

    # Initialise connection for reviewsCollection and statsCollection
    reviews = mongoDatabase[reviewsCollection]
    stats = mongoDatabase[statsCollection]

    # Insert the review into titleReviews
    reviews.insert_one(
    {
        'titleID': titleID,
        'reviewName': reviewName,
        'reviewRating': int(reviewRating),
        'reviewDate': datetime.utcnow(),
        'review': review
    })

    # Find movie rating and votes from titleStats table
    statsCursor = stats.aggregate([
        # Find by titleID
        {
            "$match": {
                "titleID": titleID
            }
        },
        # Allow following data to be displayed and get it as null if does not exist
        {
            "$project": {
                "_id": 0,
                "noOfVotes": 1,
                "rating": 1,
            }
        },
    ])

    # Convert fetched data to list
    movieStats = list(statsCursor)

    # Get the old rating and noOfVotes
    oldNoOfVotes = movieStats[0]["noOfVotes"]
    oldRating = movieStats[0]["rating"]

    # Update by appending the noOfVotes and recalculating the total ratings
    newNoOfVotes = int(oldNoOfVotes) + 1
    newRating = (float(oldRating) * oldNoOfVotes + float(reviewRating)) / newNoOfVotes

    # Update the noOfVotes and in titleStats
    stats.update_one(
        {
            'titleID': titleID,
        },
        {
            '$set': {
                'noOfVotes': int(newNoOfVotes),
                'rating': round(newRating, 1)
            }
        }
    )

    redirect('movie', titleID = titleID)

# Function to update existing reviews
def updateReview(request, titleID, reviewRating, review):
    # Initialise connection for reviewsCollection and statsCollection
    reviews = mongoDatabase[reviewsCollection]
    stats = mongoDatabase[statsCollection]

    # Find old review rating from titleStats table
    reviewCursor = reviews.aggregate([
        # Find by titleID and user name
        {
            "$match": {
                'titleID': titleID,
                'reviewName': request.user.username
            }
        },
        # Allow following data to be displayed
        {
            "$project": {
                "_id": 0,
                "reviewRating": 1
            }
        },
    ])

    # Convert fetched data to list
    reviewStats = list(reviewCursor)

    # Previous rating
    previousRating = reviewStats[0]["reviewRating"]

    # Update the review in titleReviews
    reviews.update_one(
        # Find by titleID and user name
        {
            'titleID': titleID,
            'reviewName': request.user.username
        },
        {
            '$set': {
                'review': review,
                'reviewRating': int(reviewRating),
                'reviewDate': datetime.utcnow()
            }
        }
    )

    # Find movie rating and votes from titleStats table
    statsCursor = stats.aggregate([
        # Find by titleID
        {
            "$match": {
                "titleID": titleID
            }
        },
        # Allow following data to be displayed
        {
            "$project": {
                "_id": 0,
                "noOfVotes": 1,
                "rating": 1,
            }
        },
    ])

    # Convert fetched data to list
    movieStats = list(statsCursor)

    # Get the old rating and noOfVotes
    noOfVotes = movieStats[0]["noOfVotes"]
    oldRating = movieStats[0]["rating"]

    # Update by recalculating the total ratings
    ratingSum = float(oldRating) * noOfVotes                                    # Sum of all previous ratings
    newRatingSum = ratingSum - float(previousRating) + float(reviewRating)      # Subtract old review rating and add new review rating

    # Recalculate the average rating
    newRating = newRatingSum / noOfVotes                                        # Number of votes remain the same

    # Update the noOfVotes and in titleStats
    stats.update_one(
        {
            'titleID': titleID,
        },
        {
            '$set': {
                'rating': round(newRating, 1)
            }
        }
    )

    redirect('movie', titleID = titleID)

def actor(request):
    segment = "actor"
    context = {'segment': segment}

    return render(request, 'pages/actor.html', context)

def account(request):
    if request.user.is_authenticated:
        user = request.user
        if request.method == 'POST':
            form = EditProfileForm(request.POST, instance=user)
            if form.is_valid():
                form.save()
                messages.success(request, 'Profile updated successfully!')
                return redirect('profile')
        else:
            form = EditProfileForm(instance=user)
        return render(request, 'pages/account.html', {'form': form})
    else:
        return HttpResponseRedirect('/login/')

def profile(request):
    if request.user.is_authenticated:
        # Retrieve additional information from the titleReviews collection
        reviews = mongoDatabase[reviewsCollection]
        totalRatings = reviews.aggregate([
            {
                '$match': {
                    'reviewName': request.user.username
                }
            },
            {
                '$group': {
                    '_id': None,
                    'totalRating': {'$sum': '$reviewRating'}
                }
            }
        ])

        totalRatingsDict = next(totalRatings, {}).get('totalRating', 0)

        return render(request, 'pages/profile.html', {'totalRatings': totalRatingsDict})
    else:
        return HttpResponseRedirect('/login/')
  
def logout_view(request):
    if request.user.is_authenticated:
        logout(request)
    return HttpResponseRedirect('/login/')
    
def login_view(request):
    if not request.user.is_authenticated:
        if request.method=='POST':
            fm=LoginForm(request=request,data=request.POST)
            if fm.is_valid():
                uname=fm.cleaned_data['username']
                upass=fm.cleaned_data['password']
                user=authenticate(username=uname,password=upass)
                if user is not None:
                    login(request, user)
                    messages.success(request,'Logged in Successfully!')
                    return HttpResponseRedirect('/homepage/')
        else:
            fm=LoginForm()
        return render(request,'pages/login.html',{'form':fm})
    else:
        return HttpResponseRedirect('/homepage/')
    
def register(request):
    if not request.user.is_authenticated:
        if request.method=='POST':
            fm=SignUpForm(request.POST)
            if fm.is_valid():
                user=fm.save()
                
                # Initialise connection for mySQL
                cursor = mySQLConnection.cursor()

                # Insert username and password into userAccounts table
                query = "INSERT INTO userAccounts (username, password) VALUES (%s, %s)"
                params = (user.username, user.password)

                # Execute query
                cursor.execute(query, params)
                mySQLConnection.commit()

                # Close the cursor
                cursor.close()

                group=Group.objects.get(name='Editor')
                user.groups.add(group)
                messages.success(request,'Account Created Successfully!')
                return redirect('login')
        else:
            if not request.user.is_authenticated:
                fm=SignUpForm()
        return render(request,'pages/register.html',{'form':fm})
    else:
        return HttpResponseRedirect('/dashboard/')

# check if user have any reviews (from nosql titleReviews), else no recommend 
# get titleID from user reviews (from nosql titleReviews) 
# use genreMap to get all the genreID(from sql genreMap.objective) from that specific titleID 
# use titleGenres to find the genre name(from sql titleGenres.objective) from the genreID
# keep tract and count all the number of genres reoccurencc for all the movie the user reviewed 
# sort into descesnding order 
# recommend based on the top genre Eg. Horror 
# calculate avg rating for all the movies 
# find movies that have that genre based on sorted_movies ( list of descending avg overall rating for movies)
# display top 10 movies 

# class genreMap(models.Model):
#     mappingID = models.AutoField(primary_key=True)
#     genreID = models.IntegerField()
#     titleID = models.IntegerField()

#     genre = models.ForeignKey('titleGenres', on_delete=models.CASCADE)
#     title = models.ForeignKey('titleInfo', on_delete=models.CASCADE)

#     class Meta:
#         db_table = 'genreMap'


# class titleGenres(models.Model):
#     genreID = models.AutoField(primary_key=True)
#     genre = models.CharField(max_length=50)

#     class Meta:
#         db_table = 'titleGenres'


# def recommend_movies(request):
#     # Establish connection to MongoDB
#     connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
#     client = MongoClient(connection)

#     # Access the database and collections
#     db = client["PopcornHour"]
#     collection_reviews = db["titleReviews"]
#     collection_srcs = db["titleSrcs"]

#     # Calculate the average rating for each movie, excluding documents with None values
#     average_ratings = collection_reviews.aggregate([
#         {"$match": {"reviewRating": {"$ne": None}}},
#         {"$group": {"_id": "$titleID", "avg_rating": {"$avg": "$reviewRating"}}}
#     ])

#     # Sort movies based on average rating in descending order
#     sorted_movies = sorted(average_ratings, key=lambda x: x['avg_rating'], reverse=True)

#     # Get the top recommended movies
#     top_movies = sorted_movies[:10]  # Adjust the number as per your requirement

#     # Retrieve the movie details based on titleID from the titleInfo collection
#     recommended_movies = []
#     for movie in top_movies:
#         title_id = movie['_id']
#         movie_info = titleInfo.objects.get(titleID=title_id)

#         # Retrieve the imageSrc from the collection_srcs collection based on titleID
#         src_info = collection_srcs.find_one({"titleID": title_id})
#         if src_info:
#             movie_info.imageSrc = src_info.get("imageSrc")

#         recommended_movies.append(movie_info)

#     print("Recommended Movies:")
#     for movieRec in recommended_movies:
#         print(f"TitleID: {movieRec.titleID},Title: {movieRec.title}, Image: {movieRec.imageSrc}")

#     context = {'recommended_movies': recommended_movies}
#     return render(request, 'pages/recommend_movies.html', context)


from django.db.models import Count


# def recommend_movies(request):
#     # Establish connection to MongoDB
#     connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
#     client = MongoClient(connection)

#     # Access the database and collections
#     db = client["PopcornHour"]
#     collection_reviews = db["titleReviews"]
#     collection_srcs = db["titleSrcs"]

#     # Get the user's reviews using the reviewName field
#     # user_reviews = collection_reviews.find({"reviewName": request.user.username})
#     user_reviews = collection_reviews.find({"reviewName": request.user.username})
#     user_reviews_list = list(user_reviews)  # Convert the cursor to a list

#     if not user_reviews_list or not user_reviews_list[0].get("reviewName"):
#         return None  # No reviews found for the specified person or reviewName is blank

#     # print(user_reviews_list)

#     # Extract the titleIDs into a list
#     title_ids = [review.get("titleID") for review in user_reviews_list if "titleID" in review]

#     # print(title_ids)
    
#     top_genre = titleInfo.objects.filter(titleID__in=title_ids).values_list("genre", flat=True)
#     top_genre_name = list(top_genre)

#     # Count the duplicates
#     genre_counts = Counter(top_genre_name)

#     # Count the occurrences of each genre name
#     for genre, count in genre_counts.items():
#         print(f"Genre: {genre}, Count: {count}")
#         print(top_genre_name)

#     # Find the genre with the highest count
#     max_count = 0
#     for genre, count in genre_counts.items():
#         if count > max_count:
#             max_count = count
#             top_genre_name = genre

#     print("Top Genre:", top_genre_name)


#     # Calculate the average rating for each movie, excluding documents with None values
#     average_ratings = collection_reviews.aggregate([
#         {"$match": {"reviewRating": {"$ne": None}}},
#         {"$group": {"_id": "$titleID", "avg_rating": {"$avg": "$reviewRating"}}}
#     ])

#     # # Sort movies based on average rating in descending order (id,avg_rating)
#     sorted_movies = sorted(average_ratings, key=lambda x: x['avg_rating'], reverse=True)

#     # Find movies with the top genre name 
#     recommended_movies = []
#     count = 0  # Counter variable

#     for movie in sorted_movies:
#         title_id = movie['_id']
#         movie_info = titleInfo.objects.get(titleID=title_id)

#         # Check if the movie has the top genre
#         if movie_info.genre == top_genre_name:
#             print(movie_info.genre)
#             # Retrieve the imageSrc from the collection_srcs collection based on titleID
#             src_info = collection_srcs.find_one({"titleID": title_id})
#             if src_info:
#                 movie_info.imageSrc = src_info.get("imageSrc")

#             recommended_movies.append(movie_info)
#             count += 1  # Increment the counter

#         if count >= 2:  # Break the loop when 5 movies have been recommended
#             break

#     print("Recommended Movies:")
#     for movieRec in recommended_movies:
#         print(f"TitleID: {movieRec.titleID}, Title: {movieRec.title}, Image: {movieRec.imageSrc}")

#     return recommended_movies

