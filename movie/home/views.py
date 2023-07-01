from django.shortcuts import render,HttpResponseRedirect
from django.contrib.auth import authenticate,login,logout
from django.contrib.auth.models import User,Group
from django.contrib import messages
from django.utils.html import escapejs
from pymongo import MongoClient
from .misc import getVideo
from .forms import SignUpForm,LoginForm,AddRatingForm
from .models import titleInfo,Rating,titleCasts,titleInfo,castMap
from django.shortcuts import render, redirect
from django.db import connection
import mysql.connector
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


# Homepage that displays movies in descending year released order
def homepage(request):
    segment = "homepage"

    # Define the movies list
    movies = []

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title and runtime from titleInfo table
    query = """
            SELECT ti.titleID, ti.title, ti.runtime, ti.yearReleased 
            FROM titleInfo ti
            ORDER BY ti.yearReleased DESC, ti.title ASC
            LIMIT 300
            """

    # Execute query
    cursor.execute(query)

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
    return render(request, 'pages/homepage.html', context)

# display genres to choose
def genre(request):
    segment = "genre"
    context = {'segment': segment}
    return render(request, 'pages/genre.html', context)

# display movies for the genres selected
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


# Hompage search bar to convert title '_'s to ' 's for queries
def movieSearch(request, title):
    newTitle = title.replace('_', ' ')

    # Initialise connection for mySQL
    cursor = mySQLConnection.cursor()

    # Find movie title, runtime and yearRelease from titleInfo table
    query = "SELECT titleID FROM titleInfo WHERE title LIKE %s"
    params = ('%' + newTitle + '%',)

    # Execute query
    cursor.execute(query, params)

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Close the cursor
    cursor.close()

    if rows:
        # Get the first row
        row = rows[0]
        return movie(request, row[0])
    else:
        # Handle case when no match is found
        return HttpResponseRedirect("No matching movie found.")

# Movie page when user clicks on a movie that displays reviews in descending date order
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
        "description": movieStats[0]["description"],
        "imageSrc": movieStats[0]["imageSrc"],
        "videoSrc": videoSRC,
    }

    # Send request to HTML page
    context = {'segment': segment, 'movieStats': movieStats, 'movieReviews': movieReviews}
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

    context = {'segment': 'cast_list', 'cast_members': cast_members}
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



def actor(request):
    segment = "actor"
    context = {'segment': segment}

    return render(request, 'pages/actor.html', context)

def account(request):
    segment = "account"
    context = {'segment': segment}

    return render(request, 'pages/account.html', context)

def profile(request):
        totalReview = 100
        totalwatchedmovie = 100
        username = request.user.username
        first_name = request.user.first_name
        last_name = request.user.last_name
        email = request.user.email

        return render(request, 'pages/profile.html', {
            'totalReview': totalReview,
            'totalwatchedmovie': totalwatchedmovie,
            'username': username,
            'first_name': first_name,
            'last_name': last_name,
            'email': email
        })


    
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
                    login(request,user)
                    messages.success(request,'Logged in Successfully!!')
                    return HttpResponseRedirect('/profile/')
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
                group=Group.objects.get(name='Editor')
                user.groups.add(group)
                messages.success(request,'Account Created Successfully!!!')
                return redirect('login')
        else:
            if not request.user.is_authenticated:
                fm=SignUpForm()
        return render(request,'pages/register.html',{'form':fm})
    else:
        return HttpResponseRedirect('/dashboard/')



def account(request):
    segment = "account"
    context = {'segment': segment}

    return render(request, 'pages/account.html', context)
