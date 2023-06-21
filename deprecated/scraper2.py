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
    max_threads = 1

    # Create a ThreadPoolExecutor with the specified maximum number of threads
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_threads)

    # Create a lock object for synchronization
    lock = threading.Lock()

    # List to store the Future objects
    futures = []

    # Iterate over the dictionary of ID-titleDetail pairs
    for movieID in movieIDs:
        movieData = getMovieDetails(movieID)

        # Get title link
        movieLink = movieData['Link']

        # Submit the scrapeMovie function as a task to the thread pool
        future = executor.submit(scrapeMovie, movieID, movieLink, lock)

        # Append the future object to the list
        futures.append(future)

    # Wait for all tasks to complete
    concurrent.futures.wait(futures)
    
    return

def scrapeMovie(movieID, movieLink, lock):    
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

                print(href)

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

        # Add all data into titleStats table
        # insertTitleStats(movieID, description, rating, noOfVotes)

        with lock:
            # Save movie srcs
            # saveSrcs(movieID, imageSrc, videoSrc)
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

# scraper()
