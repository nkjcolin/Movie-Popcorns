from django.shortcuts import render
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

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

    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"

    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Specify the database and collection name
    db = client["PopcornHour"]
    collection = db["titleStats"]

    movie = collection.find_one(
        {
            "titleID": 20314
        }, 
        {
            "titleID": 1, 
            "description": 1
        }
    )

    # Define the image URLs for each movie
    image_urls = [
        "https://m.media-amazon.com/images/M/MV5BZWYzOGEwNTgtNWU3NS00ZTQ0LWJkODUtMmVhMjIwMjA1ZmQwXkEyXkFqcGdeQXVyMjkwOTAyMDU@._V1_QL75_UX190_CR0,0,190,281_.jpg",
    ]

    movie["imageUrl"] = image_urls[0]
    movie["videoUrl"] = getVideo()

    context = {'segment': segment, 'movie': movie}
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

# Supporting functions
def getVideo():
    # Create a browser instance for each thread
    chromedriver = "/chromedriver"
    option = webdriver.ChromeOptions()
    option.binary_location = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    option.add_argument('--headless')
    option.add_argument("--mute-audio")
    agent="Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36"
    option.add_argument(f'user-agent={agent}')
    s = Service(chromedriver)
    driver = webdriver.Chrome(service=s, options=option)

    # Set URL to movie home
    movieURL = "https://www.imdb.com/video/vi632472089/?playlistId=tt1745960&ref_=tt_pr_ov_vi"

    try:
        # Open movie home URL
        driver.get(movieURL)

        newVidElement = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//video[contains(@class, "jw-video")]')))
        test = newVidElement.get_attribute("src")

        if test == "":
            # Find the element by class name
            playButton = driver.find_element(By.CLASS_NAME, "jw-icon-display")

            # Click the element
            playButton.click()

            # Wait for the video element to be located
            newVidElement = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "video[src]")))

            # Find element for video
            test = newVidElement.get_attribute("src")

    except NoSuchElementException:
        pass
            
    except:
        driver.close()
        driver.quit()

    print(test)

    return test