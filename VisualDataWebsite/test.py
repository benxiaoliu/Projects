import time
from selenium import webdriver

driver = webdriver.Chrome('E:\\devSoftware\\chromeDriver\\chromedriver.exe')  # Optional argument, if not specified will search path.
driver.get('http://www.google.com/');