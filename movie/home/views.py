import json
import mysql.connector

from collections import Counter
from datetime import datetime
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import Group
from django.db import connection
from django.shortcuts import HttpResponseRedirect, redirect, render
from django.utils.html import escapejs
from pymongo import MongoClient

from .forms import EditProfileForm, LoginForm, SignUpForm
from .misc import getVideo
from .models import genreMap, titleGenres


# MySQL connection settings
mySQLConnection = mysql.connector.connect (
    host='35.184.25.93',
    user='dbuser',
    password='Pa,7(@%0bO4#kEI*',
    database='db_proj'
)

# mySQLConnection = mysql.connector.connect (
#     host='localhost',
#     user='root',
#     password='root',
#     database='db_proj'
# )

# MongoDB connection settings
mongoConnection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(mongoConnection)
mongoDatabase = client["PopcornHour"]
statsCollection = "titleStats"
reviewsCollection = "titleReviews"
srcsCollection = "titleSrcs"


################
# MAIN PROGRAM #
################

# Display home page that is by default in descending year released order (DISPLAYS 12 MOVIES MAX)
def homepage(request):
    # Get recommended movies based on user's past reviews
    segment = str(request.user.username) + "'s homepage"
    recommended_movies = recommend_movies(request)

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

    # Set button click-ability
    buttons = {
        "alphabet": False,
        "date": True,
        "runtime": False,
        "watched": False,
    }

    # Find movies by yearReleased and ascending title names (DEFAULT VIEW)
    query2 = """
            SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
            FROM titleInfo ti
            ORDER BY ti.yearReleased DESC, ti.title ASC
            LIMIT 12
            """
    param = ""
    
    if request.method=='GET':
        sortOption = request.GET.get('sort')
        
        # If "Sort by Alphabetical" button was pressed
        if sortOption == 'alphabetical':
            # Set button click-ability
            buttons = {
                "alphabet": True,
                "date": False,
                "runtime": False,
                "watched": False,
            }

            # Find movies by alphabetical order, misc first then numbers then alphabets
            query2 = """
                     SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
                     FROM titleInfo ti
                     ORDER BY ti.title ASC
                     LIMIT 12
                     """
            param = ""

        # If "Sort by Date" button was pressed
        elif sortOption == 'date':
            # Set button click-ability
            buttons = {
                "alphabet": False,
                "date": True,
                "runtime": False,
                "watched": False,
            }

            # Find movies by latest release
            query2 = """
                     SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
                     FROM titleInfo ti
                     ORDER BY ti.yearReleased DESC, ti.title ASC
                     LIMIT 12
                     """
            param = ""

        # If "Sort by Runtime" button was pressed
        elif sortOption == 'runtime':
            # Set button click-ability
            buttons = {
                "alphabet": False,
                "date": False,
                "runtime": True,
                "watched": False,
            }

            # Find movies by longest runtime
            query2 = """
                     SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
                     FROM titleInfo ti
                     ORDER BY ti.runtime DESC, ti.title ASC
                     LIMIT 12
                     """
            param = ""
            
        # If "Sort by Watched" button was pressed
        elif sortOption == 'watched':
            # Set button click-ability
            buttons = {
                "alphabet": False,
                "date": False,
                "runtime": False,
                "watched": True,
            }

            # Find user's watched movies ordered by yearReleased
            query2 = """
                     SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
                     FROM titleInfo ti
                     INNER JOIN userMap um ON ti.titleID = um.titleID 
                     INNER JOIN userAccounts ua ON um.userID = ua.userID 
                     WHERE ua.userID = %s
                     ORDER BY ti.yearReleased DESC
                     """
            param = (request.user.id,)

    # Execute query and fetch all the rows
    cursor.execute(query2, param)
    mysqlList = cursor.fetchall()

    # Get the list of titleIDs from the MySQL result
    titleIDs = [row[0] for row in mysqlList]

    # Specify the database and collection name
    stats = mongoDatabase[statsCollection]

    moviesCursor = stats.aggregate([
        # Find by titleID in the list of titleIDs received from MySQL
        {
            "$match": {
                "titleID": { "$in": titleIDs } 
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

    # Define the movies list
    movies = []

    # For every movie, compile the details together (Slows down loading)
    for row in mysqlList:
        # Find the corresponding movie in the MongoDB result
        movieData = next(item for item in mongoList if item["titleID"] == row[0])

        # Compiling all details of movie into a dict
        movieDict = {
            "titleID": row[0],
            "name": row[1],
            "runtime": row[2],
            "yearReleased": row[3],
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
    # moviesJson1 = escapejs(json.dumps(mysqlList)) 
    # moviesJson2 = escapejs(json.dumps(mongoList)) 

    # context = {'segment': segment, 'moviesJson': moviesJson, 'availableMovies': availableMoviesJson, 'buttons': buttons}
    context = {'segment': segment, 'moviesJson': moviesJson, 'availableMovies': availableMoviesJson, 'recommended_movies': recommended_movies, 'buttons': buttons}
    # context = {'segment': segment, 'moviesJson1': moviesJson1, 'moviesJson2': moviesJson2, 'availableMovies': availableMoviesJson , 'recommended_movies': recommended_movies, 'buttons': buttons}
    return render(request, 'pages/homepage.html', context)

# Function to recommend movies based on user's past reviews
def recommend_movies(request):
    # Specify the database and collection names
    collection_reviews = mongoDatabase[reviewsCollection]
    collection_srcs = mongoDatabase[srcsCollection]
    collection_stats = mongoDatabase[statsCollection]

    # Get the user's reviews using the reviewName field
    user_reviews = collection_reviews.find({"reviewName": request.user.username})
    user_reviews_list = list(user_reviews)  # Convert the cursor to a list

    if not user_reviews_list or not user_reviews_list[0].get("reviewName"):
        return None  # No reviews found for the specified person or reviewName is blank

    # print(user_reviews_list)

    # Extract the titleIDs into a list
    title_ids = [review.get("titleID") for review in user_reviews_list if "titleID" in review]

    # print(title_ids)
    
    user_genre_ids = genreMap.objects.filter(titleID__in=title_ids).values_list("genreID", flat=True)
    user_genre_ids_list = list(user_genre_ids)

    # print(user_genre_ids_list)

    # Count the duplicates
    genre_counts = Counter(user_genre_ids_list)

    # # Count the occurrences of each genre name
    # for genre, count in genre_counts.items():
    #     print(f"Genre: {genre}, Count: {count}")
    #     print(user_genre_ids)

    # Find the genreID with the highest count
    max_count = 0
    for genre, count in genre_counts.items():
        if count > max_count:
            max_count = count
            top_genre_ID = genre

    top_genre_name = titleGenres.objects.get(genreID=top_genre_ID).genre
      
    # print("Top Genre ID:", top_genre_ID)
    # print("Top Genre Name:", top_genre_name)

    # Calculate the average rating for each movie, excluding documents with None values
    average_ratings = collection_stats.aggregate([
        {"$match": {"rating": {"$ne": None}}},
        {"$group": {"_id": "$titleID", "avg_rating": {"$first": "$rating"}}}
    ])

    # Sort movies based on average rating in descending order (id,avg_rating)
    sorted_movies = sorted(average_ratings, key=lambda x: x['avg_rating'], reverse=True)

    # Find movies with the top genre name 
    recommended_movies = []
    count = 0  # Counter variable

    # Check if the movie has the top genre
    for movie in sorted_movies:
        # print(f"TitleID: {movie['_id']}, Avg Rating: {movie['avg_rating']}")
        title_id = movie['_id']
        movie_info_queryset = genreMap.objects.filter(titleID=title_id, genreID=top_genre_ID)
        
        for movie_info in movie_info_queryset:
            # print("found")
            # Retrieve the imageSrc from the collection_srcs collection based on titleID
            src_info = collection_srcs.find_one({"titleID": title_id})
            if src_info:
                image_src = src_info.get("imageSrc")

            recommended_movies.append({
                "titleID": movie_info.titleID,
                "imageSrc": image_src
            })
            count += 1

        if count >= 5:
            break

    # print("Recommended Movies:")
    # for movieRec in recommended_movies:
    #     print(f"TitleID: {movieRec['titleID']}, Image: {movieRec['imageSrc']}")

    return recommended_movies

############################
# MOVIE SEARCH AND FILTERS #
############################

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
                SELECT titleID, title, runtime, yearReleased
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
                "yearReleased": row[3],
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
    username = request.user.username

    # Checks if it was a deletion POST
    if request.method=='POST':
        if 'deleteBtn' in request.POST:
            deleteReview(request, titleID, username)

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
                "reviewBtn": "block",
                "watchedButton": "none",
            }

            # Checks if it was a review POST
            if request.method=='POST':
                if 'reviewBtn' in request.POST:
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
                "reviewBtn": "none",
                "watchedButton": "block",
            }

            # Checks if it was a review POST
            if request.method=='POST':
                if 'reviewBtn' in request.POST:
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

                    # Update buttons
                    reviewForm = {
                        "reviewBox": "true",
                        "reviewLabel": "Write a review for this movie...",
                        "reviewBtn": "block",
                        "watchedButton": "none",
                    }

    # If user is not logged in, they are unable to review
    else:
        reviewForm = {
            "reviewBox": "false",
            "reviewLabel": "Log in to review movie...",
            "reviewBtn": "none",
            "watchedButton": "none",
        }
        
    # Initialise connection for mongoDB
    stats = mongoDatabase[statsCollection]
    reviews = mongoDatabase[reviewsCollection]

    # Find movie description, rating and votes from titleStats collection
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

    # Find movie reviews from titleReviews collection
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
   
    # Initialize an empty list to store the cast names
    castNames = []

    # Check all the castID from castMap of the given titleID and get the name from titleCasts
    query1 = """
            SELECT castName
            FROM titleCasts
            WHERE castID IN (
                SELECT castID
                FROM castMap
                WHERE titleID = %s
            )
            ORDER BY castName ASC
            """
    params = (titleID,)

    # Execute query
    cursor.execute(query1, params)

    # Fetch the next row
    row1 = cursor.fetchone()

    # Loop through the rows
    while row1:
        # Get the name of the cast
        castName = row1[0]

        # Add to list of cast names
        castNames.append(castName)

        # Fetch the next row
        row1 = cursor.fetchone()

    # Initialize an empty list to store the genres
    genres = []

    # Check all the genreID from genreMap of the given titleID and get the genre from titleGenres
    query2 = """
            SELECT genre 
            FROM titleGenres
            WHERE genreID IN (
                SELECT genreID
                FROM genreMap
                WHERE titleID = %s
            )
            """

    # Execute query
    cursor.execute(query2, params)

    # Fetch the next row
    row2 = cursor.fetchone()

    # Loop through the rows
    while row2:
        # Get the name of the cast
        genre = row2[0]

        # Add to list of cast names
        genres.append(genre)

        # Fetch the next row
        row2 = cursor.fetchone()

    # Find movie title, runtime and yearRelease from titleInfo table 
    query3 = """
            SELECT title, runtime, yearReleased 
            FROM titleInfo 
            WHERE titleID = %s
            """
    
    # Execute query
    cursor.execute(query3, params)

    # Fetch the specific row
    row3 = cursor.fetchone()

    # Get latest movie link for displaying (FOR PROJECT PURPOSES AS LINK EXPIRED)
    if movieStats[0]["videoSrc"] == "Video not found":
        videoSRC = ""
    else:
        videoSRC = getVideo(movieStats[0]["videoSrc"])

    # Structure to separate movie stats
    movieStats = {
        "titleID": titleID,
        "genres": genres,
        "name": row3[0],
        "runtime": row3[1],
        "votes": movieStats[0]["noOfVotes"],
        "rating": movieStats[0]["rating"],
        "casts" : castNames,
        "description": movieStats[0]["description"],
        "imageSrc": movieStats[0]["imageSrc"],
        "videoSrc": videoSRC,
    }

    # Close the connection
    cursor.close()

    # Send request to HTML page
    context = {'segment': segment, 'username': username, 'movieStats': movieStats, 'movieReviews': movieReviews, 'reviewForm': reviewForm}
    return render(request, 'pages/movie.html', context)

###################
# GENRE SELECTION #
###################

# Display genre page for user to choose from
def genre(request):
    segment = "genre"
    context = {'segment': segment}
    return render(request, 'pages/genre.html', context)

# Function to filter movies by genre selected
def genreSelect(request, genreselection):
    segment = genreselection

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
            LIMIT 99
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
                    "rating": 1,
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
            "rating": mongoList[0]['rating'],
            "description": mongoList[0]['description'],
            "imageSrc": mongoList[0]['imageSrc'],
        }
        movies.append(movieDict)
    
    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # For scripts
    moviesJson = escapejs(json.dumps(movies)) 

    context = {'segment': segment, 'moviesJson': moviesJson}
    return render(request, 'pages/genreSelect.html', context)

##################
# CAST SELECTION #
##################

# Display casts page for user to choose from (DISPLAYS 1200 NAMES MAX)
def cast(request):
    # Retrieve the list of cast members from the database
    cursor = mySQLConnection.cursor()

    # Execute a SELECT query to fetch the cast members
    query1 = """
            SELECT castID, castName 
            FROM titleCasts
            LIMIT 1200
            """
    # ORDER BY castName ASC
    cursor.execute(query1)

    # Fetch all the rows returned by the query
    rows1 = cursor.fetchall()

    # Execute a SELECT query to fetch the cast members
    query2 = """
            SELECT castName 
            FROM titleCasts
            """
    # ORDER BY castName ASC
    cursor.execute(query2)

    # Fetch all the rows returned by the query
    rows2 = cursor.fetchall()

    # Create a list of cast members
    cast_members = [{'cast_id': row[0], 'castName': row[1]} for row in rows1]
    availableCasts = [row[0] for row in rows2]
    availableCastsJson = escapejs(json.dumps(availableCasts))

    context = {'segment': 'casts', 'cast_members': cast_members, 'availableCasts': availableCastsJson}
    return render(request, 'pages/cast.html', context)

# Function to filter cast selected
def castSelect(request, cast):
    # Convert title '_'s to ' 's for queries
    newCast = cast.replace('_', ' ')
    segment = newCast

    # Split the search string by commas and put into a list
    castNames = [cast.strip() for cast in newCast.split(",")]

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # If only 1 cast name was givn
    if len(castNames) == 1:
        # Define the movies list
        movies = []

        # Find movie title according to cast name given by first finding the castID
        query = """
                SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased
                FROM titleInfo ti, castMap cm, titleCasts tc
                WHERE ti.titleID = cm.titleID
                AND cm.castID = tc.castID
                AND tc.castID IN (
                        SELECT castID
                        FROM titleCasts
                        WHERE castName = %s
                    )
                ORDER BY ti.yearReleased DESC, ti.title ASC
                """

        # Execute query
        cursor.execute(query, castNames)

        # Fetch all the rows
        castMovies = cursor.fetchall()

    elif len(castNames) > 1:
        # Create placeholders according to how mant cast names were given
        placeholders = ", ".join(["%s"] * len(castNames))

        # Define the movies list
        movies = []

        # Find movie title according to cast names given by first finding the castID and look for duplicates among them
        query = f"""
                SELECT titleID, title, runtime, yearReleased
                FROM titleInfo
                WHERE titleID IN (
                    SELECT titleID
                    FROM castMap
                    WHERE castID IN (
                        SELECT castID
                        FROM titleCasts
                        WHERE castName IN ({placeholders})
                    )
                    GROUP BY titleID
                    HAVING COUNT(DISTINCT castID) = {len(castNames)}
                )
                ORDER BY yearReleased DESC, title ASC
                """

        # Execute query
        cursor.execute(query, castNames)

        # Fetch all the rows
        castMovies = cursor.fetchall()

    # Specify the database and collection name
    stats = mongoDatabase[statsCollection]

    for row in castMovies:
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
                    "rating": 1,
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
            "rating": mongoList[0]['rating'],
            "description": mongoList[0]['description'],
            "imageSrc": mongoList[0]['imageSrc'],
        }
        movies.append(movieDict)
    
    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # For scripts
    moviesJson = escapejs(json.dumps(movies)) 

    context = {'segment': segment, 'moviesJson': moviesJson}
    return render(request, 'pages/castSelect.html', context)

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

#####################
# REVIEW OPERATIONS #
#####################

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

# Function to delete user's review
def deleteReview(request, titleID, username):
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

    # Find user's review rating for movie rating update
    deletedReviewRating = reviewStats[0]["reviewRating"]

    # Delete the review for specific movie where reviewName is the user's
    reviews.delete_one(
        {
           "titleID": titleID,
            "reviewName": username
        }
    )

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Remove mapping that indicates user has watched the show from userMap table
    query = """    
            DELETE FROM userMap 
            WHERE userID = %s AND titleID = %s
            """
    params = (request.user.id, titleID)

    # Execute query
    cursor.execute(query, params)
    mySQLConnection.commit()

    # Find movie rating and votes from titleStats table
    statsCursor = stats.aggregate([
        # Find by titleID
        {
            "$match": {
                "titleID": titleID,
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

    # Calculate the new rating by subtracting the deleted review's rating
    newRatingSum = float(oldRating) * oldNoOfVotes - float(deletedReviewRating)

    # Decrement the number of votes by 1
    newNoOfVotes = oldNoOfVotes - 1

    # Recalculate the average rating
    if newNoOfVotes > 0:
        newRating = newRatingSum / newNoOfVotes
    else:
        newRating = 0.0

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

#########################################
# USER AUTHENTICATION AND AUTHORIZATION #
#########################################

# Display user's MoviePopcorn's statistics
def profile(request):
    # If user is logged in
    if request.user.is_authenticated:
        segment = str(request.user.username) + "'s profile"

        # Retrieve additional information from the titleReviews collection
        reviews = mongoDatabase[reviewsCollection]

        # To accumulate total ratings given
        movieReviewsCursor1 = reviews.aggregate([
            # Find by username
            {
                '$match': {
                    'reviewName': request.user.username
                }
            },
            # Accumulate all the ratings made
            {
                '$group': {
                    '_id': None,
                    'totalRating': {'$sum': '$reviewRating'},
                    'totalReviews': {'$sum': 1}
                }
            }
        ])

        # Count ratings made and reviews made
        tallyCounter = next(movieReviewsCursor1, {})
        totalRatings = tallyCounter.get('totalRating', 0)
        totalReviews = tallyCounter.get('totalReviews', 0)

        # To find all reviews given by the user
        movieReviewsCursor2 = reviews.aggregate([
            # Find by username
            {
                "$match": {
                    'reviewName': request.user.username
                }
            },
            # Allow following data to be displayed and get it as null if does not exist
            {
                "$project": {
                    "_id": 0,
                    "titleID": 1,
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

        # Convert review data to list
        movieReviews = list(movieReviewsCursor2)

        # Convert the dates from mongoDB datetime to formatted date
        for review in movieReviews:
            reviewDate = review["reviewDate"]
            formattedDate = reviewDate.strftime("%d %b %Y")

            review["reviewDate"] = formattedDate
            review["popcornCount"] = range(review["reviewRating"])
            review["popcornCount2"] = range(review["reviewRating"], 10)

            # Initialise connection for mySQL
            cursor = mySQLConnection.cursor()

            # Find all movie titles from titleInfo table
            query1 = """
                    SELECT title 
                    FROM titleInfo 
                    WHERE titleID = %s
                    """
            param = (review["titleID"],)

            # Execute query
            cursor.execute(query1, param)

            # Get the row
            row = cursor.fetchone()

            # Assign the tutl
            review["title"] = row[0]

        # Send request to HTML page
        context = {'segment': segment, 'totalRatings': totalRatings, 'totalReviews': totalReviews, 'movieReviews': movieReviews}
        return render(request, 'pages/profile.html', context)
    
    # If user not logged in
    else:
        return HttpResponseRedirect('/login/')

# Display user's account information for updates
def account(request):
    # If user is logged in
    if request.user.is_authenticated:
        user = request.user

        # Check for updates
        if request.method == 'POST':
            form = EditProfileForm(request.POST, instance=user)

            # If field updates are correct
            if form.is_valid():
                form.save()
                messages.success(request, 'Profile updated successfully!')
                return redirect('profile')
            
        else:
            form = EditProfileForm(instance=user)

        return render(request, 'pages/account.html', {'form': form})
    # If user not logged in
    else:
        return HttpResponseRedirect('/login/')

# Display logout page when user logs out  
def logout_view(request):
    if request.user.is_authenticated:
        messages.success(request, 'Logged out successfully!')
        logout(request)
    return HttpResponseRedirect('/login/')

# Display login page as long as user is not authenticated 
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
                    messages.success(request, 'Logged in successfully!')
                    return HttpResponseRedirect('/homepage/')
        else:
            fm=LoginForm()
        return render(request,'pages/login.html', {'form':fm})
    else:
        return HttpResponseRedirect('/homepage/')
    
# Display registration page if user not authenticated and allow account sign ups
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
                return HttpResponseRedirect('/login/')
        else:
            if not request.user.is_authenticated:
                fm=SignUpForm()
        return render(request,'pages/register.html',{'form':fm})
    else:
        return HttpResponseRedirect('/dashboard/')
