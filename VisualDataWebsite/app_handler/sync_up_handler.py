# -*-coding:utf-8 -*-
from tornado.web import RequestHandler
from app_service.Crawler import Craw
from app_dao.visual_data_dao import insert_rows


class SyncUpHandler(RequestHandler):
    def get(self, *args, **kwargs):
        craw = Craw()
        craw.get_chrome_cookies()
        data_list = craw.get_data_list()
        all_files = []
        for data in data_list:
            files = craw.get_files(data.get("url"), data.get("date"))
            all_files += files

        for file in all_files:
            craw.parse_file(file["url"], file["file_name"], file["date"])
        insert_tuples = craw.format_insert_data()
        insert_rows(insert_tuples)
        self.write({"code": 200})

