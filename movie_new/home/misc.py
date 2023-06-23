from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

from bs4 import BeautifulSoup
import requests
from urllib.request import Request, urlopen

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

        vidElement = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, '//video[contains(@class, "jw-video")]')))
        vidSrc = vidElement.get_attribute("src")

        if vidSrc == "":
            # Find the element by class name
            playButton = driver.find_element(By.CLASS_NAME, "jw-icon-display")

            # Click the element
            playButton.click()

            # Wait for the video element to be located
            vidElement = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "video[src]")))

            # Find element for video
            vidSrc = vidElement.get_attribute("src")

    except NoSuchElementException:
        pass
            
    except:
        driver.close()
        driver.quit()

    return "None"

