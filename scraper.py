from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from pymongo import MongoClient

import concurrent.futures
import pandas as pd
import threading
import certifi
import os

# Web scraper
def scraper():
    # Get the count of processed movies
    movieCount = getCount()

    # Retrieve movie titles from DB
    movieIDLinkPairs = getMovieIDs()

    # Retrieve the movie list starting from the movieCount index
    movieList = list(movieIDLinkPairs.items())[movieCount:]

    # Specify the maximum number of concurrent threads
    max_threads = 4

    # Create a ThreadPoolExecutor with the specified maximum number of threads
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)

    # Create a lock object for synchronization
    lock = threading.Lock()

    # List to store the Future objects
    futures = []

    # Iterate over the dictionary of ID-titleDetail pairs
    for movieID, movieData in enumerate(movieList):
        # Increment the count of processed movies
        with lock:
            movieCount += 1

        # Get title details
        movieLink = movieData[1]['Link']
        description = movieData[1]['Description']
        rating = movieData[1]['Rating']
        noOfVotes = movieData[1]['NoOfVotes']

        # Submit the scrapeMovie function as a task to the thread pool
        future = executor.submit(scrapeMovie, movieID + 1, movieLink, description, rating, noOfVotes, lock)

        # Append the future object to the list
        futures.append(future)

        # Save the count of processed movies to the file
        saveCount(movieCount)

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)
    
    return

def scrapeMovie(movieID, movieLink, description, rating, votes, lock):    
    # Create a browser instance for each thread
    chromedriver = "/chromedriver"
    option = webdriver.ChromeOptions()
    option.binary_location = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
    # option.add_argument('--window-position=1300,0')
    option.add_argument('--window-size=1200,850')
    option.add_argument('--headless')
    option.add_argument("--mute-audio")
    agent="Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36"
    option.add_argument(f'user-agent={agent}')
    s = Service(chromedriver)
    driver = webdriver.Chrome(service=s, options=option)

    # For tracking in terminal
    print("======= CHECKING: " + str(movieID) + " =======")
    print("Link:\t\t" + movieLink)
    print("Description:\t" + description)
    print("Rating:\t\t" + str(rating))
    noOfVotes = parseVotes(votes)
    print("No of votes:\t" + str(noOfVotes))

    # URL base link
    URLBase = "https://www.imdb.com" 
    reviewsURLBase = "reviews?spoiler=hide&sort=curated&dir=desc&ratingFilter=0"

    # Set URL to extract casts and runtime
    movieURL = URLBase + movieLink

    # Open movie home URL
    driver.get(movieURL)

    try:
        # Find element for picture
        imgElement = driver.find_element(By.XPATH, '//img[starts-with(@src, "https://m.media-amazon.com/images/")]')
        
        # If movie has trailer in website
        if imgElement.is_displayed():
            # Extract link from element
            imageSrc = imgElement.get_attribute("src")
        
            print("Image:\t\t" + imageSrc[:100] + "...")
    
    except NoSuchElementException:
        imageSrc = "Image not found"
        print("Image:\t\tNot found")
    
    try:
        # Find element for video
        vidElement = driver.find_element(By.XPATH, '//a[@data-testid="video-player-slate-overlay"]')

        if vidElement.is_displayed():
            href = vidElement.get_attribute("href")

            # Access the href link for source video
            driver.get(href)

            newVidElement = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//video[contains(@class, "jw-video")]')))
            videoSrc = newVidElement.get_attribute("src")

            if videoSrc == "":
                # Find the element by class name
                playButton = driver.find_element(By.CLASS_NAME, "jw-icon-display")

                # Click the element
                playButton.click()

                # Wait for the video element to be located
                newVidElement = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "video[src]")))

                # Find element for video
                videoSrc = newVidElement.get_attribute("src")
                
            print("Video:\t\t" + videoSrc[:100] + "...")

    except NoSuchElementException:
        videoSrc = "Video not found"
        print("Video:\t\tNot found")

    # Set URL to extract reviews with no spoilers
    reviewURL = movieURL + reviewsURLBase

    # Open movie review URL
    driver.get(reviewURL)

    try:
        # Find element for list of reviews
        reviewBox = driver.find_elements(By.XPATH, '//div[contains(@class, "lister-item mode-detail imdb-user-review  collapsable")]')

        for reviews in reviewBox:
            try:
                # Find rating, reviewName, reviewDate and review element
                ratingElement = reviews.find_element(By.XPATH, './/span[@class="rating-other-user-rating"]/span[1]')
                reviewNameElement = reviews.find_element(By.CLASS_NAME, 'display-name-link')
                reviewDateElement = reviews.find_element(By.CLASS_NAME, 'review-date')
                reviewElement = reviews.find_element(By.XPATH, './/div[contains(@class, "text show-more__control")]')

                # Get text content
                ratingValue = ratingElement.text
                reviewName = reviewNameElement.text
                reviewDate = reviewDateElement.text
                review = reviewElement.text
            
                # Add all data into titleReviews table
                insertTitleReviews(movieID, reviewName, ratingValue, reviewDate, review)

            # If any of review elements not found, skip to next review
            except NoSuchElementException:
                pass    

    except NoSuchElementException:
        print("Review:\t\tNone found for " + str(movieID))

    finally:
        driver.close()
        driver.quit()

    # Add all data into titleStats table
    insertTitleStats(movieID, description, rating, noOfVotes)
    print("✅ Data stored in both tables")

    with lock:
        # Save movie srcs
        saveSrcs(movieID, imageSrc, videoSrc)
        print("========= DONE: " + str(movieID) + " =========\n")

    return

# Supporting functions
def getMovieIDs():
    # Open the Excel file
    file = pd.read_excel('titleDataset.xlsx', sheet_name='Sheet1')

    # Extract the ID and title details columns
    movieIDs = file[file.columns[0]].values.tolist()
    movieLinks = file[file.columns[11]].values.tolist()
    descriptions = file[file.columns[6]].values.tolist()
    ratings = file[file.columns[3]].values.tolist()
    noOfVotes = file[file.columns[4]].values.tolist()

    # Create a dictionary to store the ID-titleDetail pairs
    movieIDLinkPairs = {}

    # Iterate over the movieIDs and title details to store them in the dictionary
    for movieID, movieLink, description, rating, votes in zip(movieIDs, movieLinks, descriptions, ratings, noOfVotes):
        movieIDLinkPairs[movieID] = {
            'Link': movieLink,
            'Description': description,
            'Rating': rating,
            'NoOfVotes': votes
        }

    # Return the dictionary of ID-titleDetail pairs
    return movieIDLinkPairs

def getCount():
    if os.path.exists("progress.txt"):
        with open("progress.txt", "r") as file:
            count = int(file.read())
            return count
    else:
        return 0

def saveCount(count):
    with open("progress.txt", "w") as file:
        file.write(str(count))

def saveSrcs(movieID, imageSrc, videoSrc):
    # Read the titleSrcs XLSX file
    file = pd.read_excel('titleSrc.xlsx')

    # Retrieves by zero-based indexing
    movieID -= 1

    # Update the movie title and year in the dataset
    file.iloc[movieID, 1] = imageSrc
    file.iloc[movieID, 2] = videoSrc

    # Save the updated dataset back to XLSX file
    print("❗️❗️ CURRENTLY SAVING ❗️❗️")
    file.to_excel('titleSrc.xlsx', index=False)
    print("✅✅ Src file saved")
    return

def insertTitleStats(movieID, description, rating, noOfVotes):
    # MongoDB Atlas connection string
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
 
    # Create a MongoClient object
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Attempt to connect
    try:
        # Access the MongoDB database
        db = client["PopcornHour"]   

        # Access a collection within the database
        collection = db["titleStats"]
        
        # Insert a document into the collection
        data = {
            "titleID": movieID,
            "description": description,
            "rating": rating,
            "noOfVotes": int(noOfVotes)
        }
        collection.insert_one(data)

    except Exception as e:
        print(e)

    # Close the MongoDB connection
    client.close()

def insertTitleReviews(movieID, reviewName, reviewRating, reviewDate, review):
    # MongoDB Atlas connection string
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
 
    # Create a MongoClient object
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Attempt to connect
    try:
        # Access the MongoDB database
        db = client["PopcornHour"]   

        # Access a collection within the database
        collection = db["titleReviews"]
        
        # Insert a document into the collection
        data = {
            "titleID": movieID,
            "reviewName": reviewName,
            "reviewRating": reviewRating,
            "reviewDate": reviewDate,
            "review": review
        }
        collection.insert_one(data)

    except Exception as e:
        print(e)

    # Close the MongoDB connection
    client.close()

def parseVotes(votes):
    if votes.endswith('K'):
        # Remove the 'K' character
        votes = votes[:-1]
        # Convert to float and multiply by 1000, then convert to integer
        votes = int(float(votes) * 1000)

    elif votes.endswith('M'):
        # Remove the 'M' character
        votes = votes[:-1]
        # Convert to float and multiply by 1 million, then convert to integer
        votes = int(float(votes) * 1000000)

    else:
        # Convert to integer directly
        votes = int(votes)
        
    return votes


scraper()
