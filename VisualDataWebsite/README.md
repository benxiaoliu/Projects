### webdriver set up:
Download a chrome driver 
https://chromedriver.chromium.org/
###### Open a chrome and login to Midway 
Open `CMD`  
`cd` to the path of your local chrome.exe folder   (mine is C:\Program Files (x86)\Google\Chrome\Application)
Run the following command to open a chrome so that chromeDriver can catch it: 
> chrome.exe --remote-debugging-port=9222 --user-data-dir="C:\selenum\AutomationProfile"

