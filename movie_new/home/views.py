from django.shortcuts import render
from pymongo import MongoClient
from datetime import datetime

import certifi


def index(request):
    segment = "dashboard"

    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleStats"]

    movies_cursor = collection.aggregate([
        {
            "$match": {
                "titleID": {"$gte": 1, "$lte": 5}
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

    movies = list(movies_cursor)

    # Define the image URLs for each movie
    image_urls = [
        "https://m.media-amazon.com/images/M/MV5BZWYzOGEwNTgtNWU3NS00ZTQ0LWJkODUtMmVhMjIwMjA1ZmQwXkEyXkFqcGdeQXVyMjkwOTAyMDU@._V1_QL75_UX190_CR0,0,190,281_.jpg",
        "https://m.media-amazon.com/images/M/MV5BMTc5MDE2ODcwNV5BMl5BanBnXkFtZTgwMzI2NzQ2NzM@._V1_QL75_UX190_CR0,0,190,281_.jpg",
        "https://m.media-amazon.com/images/M/MV5BMDJhMGRjN2QtNDUxYy00NGM3LThjNGQtMmZiZTRhNjM4YzUxL2ltYWdlL2ltYWdlXkEyXkFqcGdeQXVyMTQxNzMzNDI@._V1_QL75_UY281_CR0,0,190,281_.jpg",
        "https://m.media-amazon.com/images/M/MV5BYTIxNjk3YjItYmYzMC00ZTdmLTk0NGUtZmNlZTA0NWFkZDMwXkEyXkFqcGdeQXVyNjAwNDUxODI@._V1_QL75_UX190_CR0,2,190,281_.jpg",
        "https://m.media-amazon.com/images/M/MV5BZmQ5NGFiNWEtMmMyMC00MDdiLTg4YjktOGY5Yzc2MDUxMTE1XkEyXkFqcGdeQXVyNTA4NzY1MzY@._V1_QL75_UY281_CR1,0,190,281_.jpg"
    ]

    # Iterate over movies and assign image URLs
    for i, movie in enumerate(movies):
        if i < len(image_urls):
            movie["imageUrl"] = image_urls[i]

    context = {'segment': segment, 'movies': movies}
    return render(request, 'pages/dashboard.html', context)

def login(request):
    return render(request, 'pages/login.html')

def register(request):
    return render(request, 'pages/register.html')

def movie(request):
    segment = "movie"
    context = {'segment': segment}

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

