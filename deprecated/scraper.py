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
    # Retrieve movie titles from DB
    movieIDs = getMovieIDs()

    # Specify the maximum number of concurrent threads
    max_threads = 5

    # Create a ThreadPoolExecutor with the specified maximum number of threads
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)

    # Create a lock object for synchronization
    lock = threading.Lock()

    # List to store the Future objects
    futures = []

    # Iterate over the dictionary of ID-titleDetail pairs
    for movieID in movieIDs:
        movieData = getMovieDetails(movieID)

        # Get title details
        movieLink = movieData['Link']
        description = movieData['Description']
        rating = movieData['Rating']
        noOfVotes = movieData['NoOfVotes']

        # Submit the scrapeMovie function as a task to the thread pool
        future = executor.submit(scrapeMovie, movieID, movieLink, description, rating, noOfVotes, lock)

        # Append the future object to the list
        futures.append(future)

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
    print("Description:\t" + description[:180])
    print("Rating:\t\t" + str(rating))
    noOfVotes = parseVotes(votes)
    print("No of votes:\t" + str(noOfVotes))

    # URL base link
    URLBase = "https://www.imdb.com" 
    reviewsURLBase = "reviews?spoiler=hide&sort=curated&dir=desc&ratingFilter=0"

    # Set URL to movie home
    movieURL = URLBase + movieLink

    try:
        # Open movie home URL
        driver.get(movieURL)

        try:
            # Find element for picture
            imgElement = driver.find_element(By.XPATH, '//img[starts-with(@src, "https://m.media-amazon.com/images/")]')
            
            # If movie has trailer in website
            if imgElement.is_displayed():
                # Extract link from element
                imageSrc = imgElement.get_attribute("src")
            
                print("Image:\t\t" + imageSrc[:150] + "...")
        
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
                    
                print("Video:\t\t" + videoSrc[:150] + "...")

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

        # Add all data into titleStats table
        insertTitleStats(movieID, description, rating, noOfVotes)
        print("✅ Data stored in both tables")

        with lock:
            # Save movie srcs
            saveSrcs(movieID, imageSrc, videoSrc)
            print("========= DONE: " + str(movieID) + " =========\n")
            
    except:
        driver.close()
        driver.quit()

    return

# Supporting functions
def getMovieIDs():
    # Open the Excel file
    file = pd.read_excel('titleSrc.xlsx')

    # Extract the ID and title details columns
    movieIDs = file[file.columns[0]].values.tolist()

    # Return the dictionary of ID-titleDetail pairs
    return movieIDs

def getMovieDetails(movieID):
    # Open the Excel file
    file = pd.read_excel('titleDataset.xlsx')

    # Search for the row with the given movieID
    movieRow = file.loc[file[file.columns[0]] == movieID]
    
    # Extract the ID and title details columns from the movieRow
    movieLink = movieRow[file.columns[11]].values[0]
    description = movieRow[file.columns[6]].values[0]
    rating = movieRow[file.columns[3]].values[0]
    noOfVotes = movieRow[file.columns[4]].values[0]

    # Iterate over the movieIDs and title details to store them in the dictionary
    movieIDLinkPair = {
        'Link': movieLink,
        'Description': description,
        'Rating': rating,
        'NoOfVotes': noOfVotes
    }

    # Return the dictionary of ID-titleDetail pairs
    return movieIDLinkPair

def saveSrcs(movieID, imageSrc, videoSrc):
    # Read the titleSrcs XLSX file
    file = pd.read_excel('titleSrc.xlsx')

    # Search for the row with the given movieID
    movieRow = file.loc[file[file.columns[0]] == movieID]

    # Get the index of the movieRow
    movieIndex = movieRow.index[0]

    # Update the movie title and year in the dataset
    file.loc[movieIndex, file.columns[1]] = imageSrc
    file.loc[movieIndex, file.columns[2]] = videoSrc

    # Save the updated dataset back to XLSX file
    print("❗️❗️ CURRENTLY SAVING ❗️❗️")
    file.to_excel('titleSrc.xlsx', index=False)
    print("✅✅ Src file saved")
    return

def insertTitleStats(movieID, description, rating, noOfVotes):
    # MongoDB Atlas connection string
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
 
    # Attempt to connect
    try:
        # Create a MongoClient object
        client = MongoClient(connection, tlsCAFile=certifi.where())

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
 
    # Attempt to connect
    try:
        # Create a MongoClient object
        client = MongoClient(connection, tlsCAFile=certifi.where())

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


# scraper()

# Open the Excel file
file = pd.read_excel('titleDataset.xlsx')

# Search for the row with the given movieID
movieRow = file.loc[file[file.columns[0]] == 13]

# Extract the ID and title details columns from the movieRow
casts = movieRow[file.columns[8]].values[0]

# Remove brackets and single quotes
casts = casts.strip("[]").replace("'", "")

# Separate the names
names = [name.strip() for name in casts.split(',')]

# Print the separated names
for name in names:
    print(name)