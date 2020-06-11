from app_dao import db_query_for_list, db_query_for_one, db_insert_lastrowid, db_insert_many, db_delete


def select_time():  # guid
    sql = "SELECT dateTime FROM visualData" # dateTimes
    return map(lambda x: x[0], db_query_for_list(sql))


def select_colume_by_dateTime(colume, dateTime):
    sql = "SELECT " + colume + "  FROM visualData WHERE dateTime = %s"
    return db_query_for_one(sql, dateTime)


def insert_rows(rows_tuple):
    sql = 'DELETE FROM VisualData'
    db_delete(sql)
    sql = "INSERT INTO VisualData(dateTime, RMSE, trainingDataVolume, features) VALUES (%s, %d, %d, %s)"
    return db_insert_many(sql, rows_tuple)

    # db_insert("INSERT INTO VisualData (RMSE) VALUES (%d)", row[1])



if __name__ == '__main__':
    pass

    # sql = 'DELETE FROM VisualData'
    # db_delete(sql)
    #
    # sql = "INSERT INTO VisualData(dateTime, RMSE, trainingDataVolume) VALUES (%s, %d, %d)"
    # db_insert_many(sql,((u'2020-03-26_19:48:20', 35.58239, 44238378), (u'2020-03-26_19:44:59', 35.79434, 44269074), (u'2020-03-26_19:50:22', 35.65028, 44206651), (u'2020-03-25_19:54:38', 35.65028, 44206651), (u'2020-03-26_19:46:32', 35.6671, 44238378), (u'2020-03-26_19:44:28', 35.77069, 44249259)))
    #insert_rows(((u'2021-03-26_19:48:20', 35.58239, 44238378), (u'2020-03-26_19:44:59', 35.79434, 44269074), (u'2020-03-26_19:50:22', 35.65028, 44206651), (u'2020-03-25_19:54:38', 35.65028, 44206651), (u'2020-03-26_19:46:32', 35.6671, 44238378), (u'2020-03-26_19:44:28', 35.77069, 44249259)))
