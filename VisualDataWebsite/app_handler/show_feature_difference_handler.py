# -*-coding:utf-8 -*-
import collections

from tornado.web import RequestHandler
from app_dao.visual_data_dao import select_time, select_colume_by_dateTime


class ShowFeatureDifferenceHandler(RequestHandler):

    def post(self, *args, **kwargs):
        times = eval(self.get_argument("selected_set"))
        data_map = {}
        for time in times:
            data_map[time] = find_y_by_dateTimes('features', time)[0]
        feature_difference_render_list = feature_difference_render_data(data_map)
        self.write({"feature_list": feature_difference_render_list})

def find_y_by_dateTimes(colume, time):
    re = []
    re.append(select_colume_by_dateTime(colume, time))
    return re
def feature_difference_render_data(timeFeturesMap):
    time_feature_maps = collections.defaultdict(dict)
    for time, features in timeFeturesMap.items():
        for rank, feature in enumerate(features.split(' ')):
            time_feature_maps[time][feature] = rank + 1  # {exp1:{f1:2, f2:2, f3:3}, exp2:{f3:1, f2:2, f4:3}}
    render_data = collections.defaultdict(dict)  # {in all: {total_number_of_features: 2, feature1: (2,3), feature2: (2)}, only in time1:{} ...}
    # find features which are in all exps, record and corresponding ranks
    time_featureset_map = collections.defaultdict(set)  #{exp1:(f1, f2, f3), exp2:(f2,f3,f4)}
    for exp, value in time_feature_maps.items():
        time_featureset_map[exp] = set(value.keys())
    # get features_in_all
    for i, exp in enumerate(time_featureset_map.values()):
        if i == 0:
            features_in_all = exp
        else:
            features_in_all = features_in_all.intersection(exp)
    # get features_in_all and corresponding ranks
    for feature in features_in_all:
        render_data['in_all'][feature] = set()
        for exp in time_feature_maps.keys():
            render_data['in_all'][feature].add(time_feature_maps[exp][feature])
    # get only-in feature in every exp    #{exp1:(f1, f2, f3), exp2:(f2,f3,f4)}
    for cur_exp in time_featureset_map.keys():
        features_only_in_cur_exp = time_featureset_map[cur_exp]
        for exp, feature_set in time_featureset_map.items():
            if cur_exp != exp:
                features_only_in_cur_exp = features_only_in_cur_exp.difference(feature_set)
        for feature in features_only_in_cur_exp:
            render_data[cur_exp][feature] = time_feature_maps[cur_exp][feature]

    return string_render_data(render_data)

def string_render_data(render_data_map):
    string_result_list = []
    exp_item = ''
    for exp, feature_ranks in render_data_map.items():
        if exp != 'in_all':
            exp_item += 'features only in ' + exp + ':<br>'
        else:
            exp_item += 'features in all exps' + ':<br>'
        for feature, ranks in feature_ranks.items():
            exp_item += feature + ': '
            if str(ranks).isdigit():
                exp_item += str(ranks) + '<br>'
            else:
                exp_item += str([rank for rank in ranks]) + '<br>'
        string_result_list.append(exp_item)
        exp_item = ''
    string_result_list = [item.split(':') for item in string_result_list]
    return string_result_list








