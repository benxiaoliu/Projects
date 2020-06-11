import collections
import ast
from bs4 import BeautifulSoup
import requests
from selenium import webdriver

DRIVERPATH = "E:\\devSoftware\\chromeDriver\\chromedriver.exe"
COSMOS_URL = "https://aad.cosmos09.osdinfra.net/cosmos/relevance/projects/metrics/charwan/sample/"

class Craw:
    def __init__(self):
        self.url = COSMOS_URL
        self.cookies = {}
        self.data_pairs = []
        self.insertDataMap = collections.defaultdict(dict)
        self.insert_data = []

    def get_data_list(self):

        html = requests.get(self.url, cookies=self.cookies).text
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find_all("tbody")[0]
        tag_a_list = table.find_all("a")

        data_list = []
        for a in tag_a_list:
            url = "https://aad.cosmos09.osdinfra.net" + a.get("href")
            date = a.string
            data_list.append({"url": url, "date": date})
        return data_list

    def get_files(self, url, date):
        html = requests.get(url, cookies=self.cookies).text
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find_all("tbody")[0]
        tag_a_list = table.find_all("a")
        files = []
        for a in tag_a_list:
            url = a.get("href")
            file_name = a.string
            files.append({"url": url, "file_name": file_name, "date": date})
        return files

    def parse_file(self, url, file_name, date):

        file_url = "https://aad.cosmos09.osdinfra.net/File/Download" + url.replace("?property=info", "")
        data = requests.get(file_url, cookies=self.cookies).text
        if file_name == "RMSE.tsv":
            rmse = get_rmse(data)
            self.insertDataMap[date]["RMSE"] = rmse
        elif file_name == "TrainingDataVolume.tsv":
            dataVolume = data.strip()
            self.insertDataMap[date]["trainingDataVolume"] = dataVolume
        else:
            features = ' '.join([item.split('\t')[1] for item in data.split("\r")[1:-1]])
            self.insertDataMap[date]["features"] = features



    def get_chrome_cookies(self):
        driver = webdriver.Chrome(DRIVERPATH, chrome_options=set_option())
        driver.get(self.url)
        cookies_list = driver.get_cookies()
        for cookies in cookies_list:
            if cookies.get("domain") == "aad.cosmos09.osdinfra.net":
                self.cookies[cookies.get("name")] = cookies.get("value")

    def format_insert_data(self):
        data = []  # [date, trainingDataVolume, RMSE, importantFeatures]
        for key in self.insertDataMap:
            item = [0, 0, 0, 0]
            item[0] = key
            for sub_key in self.insertDataMap[key]:
                if self.insertDataMap[key][sub_key]:
                    if sub_key == 'RMSE':
                        item[1] = ast.literal_eval(self.insertDataMap[key][sub_key])
                    elif sub_key == 'trainingDataVolume':
                        item[2] = ast.literal_eval(self.insertDataMap[key][sub_key])
                    else: item[3] = self.insertDataMap[key][sub_key]  # importantFeatures
                else:
                    if sub_key == 'RMSE':
                        item[1] = None
                    elif sub_key == 'trainingDataVolume':
                        item[2] = None
                    else:
                        item[3] = None
            data.append(item)
        self.insert_data = tuple(map(tuple, data))
        return self.insert_data


def set_option():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    return chrome_options


def get_rmse(data):
    return repr(data).replace("RMSE\\ttruth_label_std\\n", "").split("\\t")[0].replace("u'", "")



if __name__ == '__main__':
    craw = Craw()
    craw.get_chrome_cookies()
    data_list = craw.get_data_list()
    all_files = []
    for data in data_list:
        files = craw.get_files(data.get("url"), data.get("date"))
        all_files += files
    for file in all_files:
        craw.parse_file(file["url"], file["file_name"], file["date"])
    print craw.format_insert_data()








