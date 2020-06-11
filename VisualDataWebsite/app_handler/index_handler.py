# -*-coding:utf-8 -*-
from tornado.web import RequestHandler
from app_dao.visual_data_dao import select_time, select_colume_by_dateTime


class IndexHandler(RequestHandler):
    def get(self, *args, **kwargs):
        time_list = select_time()
        self.render('home.html', time_list=time_list)

    def post(self, *args, **kwargs):
        times = eval(self.get_argument("selected_set"))
        selected_charts = eval(self.get_argument("selected_charts"))
        data_map = {}
        for colume in selected_charts:
            data_map[colume] = find_y_by_dateTimes(colume, times)
        self.write({"data_map": data_map})


def find_y_by_dateTimes(colume, times):
    re = []
    for time in times:
        re.append(select_colume_by_dateTime(colume, time))
    return re
