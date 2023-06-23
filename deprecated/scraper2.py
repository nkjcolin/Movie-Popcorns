from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from pymongo import MongoClient

import concurrent.futures
import pandas as pd


# Web scraper
def scraper():
    # Retrieve movie titles from DB
    movieIDs = getMovieIDs()

    # Specify the maximum number of concurrent threads
    max_threads = 5

    # Create a ThreadPoolExecutor with the specified maximum number of threads
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)

    # List to store the Future objects
    futures = []

    # Iterate over the dictionary of ID-titleDetail pairs
    for movieID in movieIDs:
        movieData = getMovieDetails(movieID)

        # Get title link
        movieLink = movieData['Link']
        movieImage = movieData['Image']

        # Submit the scrapeMovie function as a task to the thread pool
        future = executor.submit(scrapeMovie, movieID, movieLink, movieImage)

        # Append the future object to the list
        futures.append(future)

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)
    
    return

def scrapeMovie(movieID, movieLink, imageSrc):    
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
    print("Image:\t\t" + imageSrc)

    # URL base link
    URLBase = "https://www.imdb.com" 

    # Set URL to movie home
    movieURL = URLBase + movieLink

    try:
        # Open movie home URL
        driver.get(movieURL)
       
        try:
            # Find element for video
            vidElement = driver.find_element(By.XPATH, '//a[@data-testid="video-player-slate-overlay"]')

            if vidElement.is_displayed():
                href = vidElement.get_attribute("href")

                videoSrc = href

        except NoSuchElementException:
            videoSrc = "Video not found"

        # Add all data into titleSrcs table
        # insertTitleSrcs(movieID, imageSrc, videoSrc)
        print("Video:\t\t" + videoSrc)
        print("========= DONE: " + str(movieID) + " =========\n")
            
    except:
        driver.close()
        driver.quit()

    return

# Supporting functions
def getMovieIDs(startingMovieID=6976):
    # Open the Excel file
    file = pd.read_excel('../docs/titleSrc.xlsx')

    # Extract the ID and title details columns
    movieIDs = file[file.columns[0]][file[file.columns[0]] >= startingMovieID].values.tolist()

    # Return the dictionary of ID-titleDetail pairs
    return movieIDs

def getMovieDetails(movieID):
    # Open the Excel file
    file1 = pd.read_excel('../docs/titleDataset.xlsx')
    file2 = pd.read_excel('../docs/titleSrc.xlsx')

    # Search for the row with the given movieID
    movieRow1 = file1.loc[file1[file1.columns[0]] == movieID]
    movieRow2 = file2.loc[file2[file2.columns[0]] == movieID]
    
    # Extract the ID and title details columns from the movieRow
    movieLink = movieRow1[file1.columns[11]].values[0]
    movieImage = movieRow2[file2.columns[1]].values[0]

    # Iterate over the movieIDs and title details to store them in the dictionary
    movieLinkImage = {
        'Link': movieLink,
        'Image': movieImage,
    }

    # Return the dictionary of ID-titleDetail pairs
    return movieLinkImage

def insertTitleSrcs(movieID, imageSrc, videoSrc):
    # MongoDB Atlas connection string
    connection = "mongodb+srv://root:root@cluster0.miky4lb.mongodb.net/?retryWrites=true&w=majority"
 
    # Attempt to connect
    try:
        # Create a MongoClient object
        client = MongoClient(connection)

        # Access the MongoDB database
        db = client["PopcornHour"]   

        # Access a collection within the database
        collection = db["titleSrcs"]
        
        # Insert a document into the collection
        data = {
            "titleID": movieID,
            "imageSrc": imageSrc,
            "videoSrc": videoSrc,
        }
        collection.insert_one(data)

    except Exception as e:
        print(e)

    # Close the MongoDB connection
    client.close()


scraper()
