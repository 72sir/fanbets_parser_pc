# coding: utf-8
import threading
import time
import json
import urllib2
import mysql.connector

from mysql.connector import Error

ALL_GAME = 'http://betting.gg11.bet/api/sportmatch/Get?sportID=2357'


def select_sql_db(select):
    dictionary = {}
    try:
        conn2 = mysql.connector.connect(host='localhost',
                                        database='k72gsi3s_dj2',
                                        user='k72gsi3s_dj2',
                                        password='1234567890')
        cursor2 = conn2.cursor()
        cursor2.execute(select)
        row = cursor2.fetchone()

        while row is not None:
            elem_str = str(row[1])
            dictionary[elem_str.upper()] = row[0]
            row = cursor2.fetchone()

        cursor2.close()
        conn2.close()

    except Error as e:
        print(e)
    """
    finally:
        print ("finally select", dictionary)
    """
    return dictionary


def sql_insert(select):
    print("select - ", select)
    """    """
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database='k72gsi3s_dj2',
                                       user='k72gsi3s_dj2',
                                       password='1234567890')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(select)

            if cursor.lastrowid:
                print('last insert id', cursor.lastrowid)
            else:
                print('last insert id not found')

            conn.commit()
            cursor.close()
            conn.close()

    except Error as e:
        print(e)

    finally:
        print('Insert final')


# Главная функция
def main():
    """"""
    # 1. Получаем json файл
    new_json_data = parse_courses(ALL_GAME)
    # 2. Следует из базы получить данные и создать по ним словари:
    # Tournament,
    dict_tournament = select_sql_db('SELECT * FROM  `Tournament`')
    # Category,
    dict_category = select_sql_db('SELECT * FROM  `Categoria`')
    # Title
    dict_title = select_sql_db('SELECT * FROM  `Title`')

    # 3. Производим сравнение данных и если поступили новые данные то заносим их в базу
    for new_elem in new_json_data:
        ################################# - проверяем все Tournaments ##########################################
        # находим элемент в словаре
        new_info_select_db = dict_tournament.get(new_elem["Tournament"].upper())
        # Если ключа нет то добавляем если есть то print
        if new_info_select_db is None:
            # в словаре ключа нет по данному наименованию, можно добавлять
            # Вызываем функцию поступления новых данных
            query = "INSERT INTO `tournament`(`name`) VALUES (\"" + new_elem["Tournament"] + "\")"
            sql_insert(query)
            ################################
            # Обновляем словарь - можно было вставлять лишь 1 элемент. И пока что сделаем через select
            dict_tournament.clear()
            dict_tournament = select_sql_db('SELECT * FROM  `Tournament`')
            print ("NOT_tournament - ", new_elem["Tournament"])

        ################################# - проверяем все Categoria ##########################################
        # находим элемент в словаре
        new_info_select_db = dict_category.get(new_elem["Category"].upper())
        # Если ключа нет то добавляем если есть то print
        if new_info_select_db is None:
            # в словаре ключа нет по данному наименованию, можно добавлять
            # Вызываем функцию поступления новых данных
            query = "INSERT INTO `categoria`(`name`) VALUES (\"" + new_elem["Category"] + "\")"
            sql_insert(query)
            ################################
            # Обновляем словарь - можно было вставлять лишь 1 элемент. И пока что сделаем через select
            dict_category.clear()
            dict_category = select_sql_db('SELECT * FROM  `categoria`')
            print ("NOT_Category - ", new_elem["Category"])
        ################################# - проверяем все Title ##########################################
        # 4. Производим сравнение данных и если поступили новые данные то заносим их в базу
        # находим элемент в словаре
        # нюанс в том что у нас на проверку идут 2 тайтала Title_0 и Title_1
        new_info_select_db = dict_title.get(new_elem["Title_0"].upper())
        # Если ключа нет то добавляем если есть то print
        if new_info_select_db is None:
            # в словаре ключа нет по данному наименованию, можно добавлять
            # Вызываем функцию поступления новых данных
            query = "INSERT INTO `title`(`name`) VALUES (\"" + new_elem["Title_0"] + "\")"
            sql_insert(query)
            ################################
            # Обновляем словарь - можно было вставлять лишь 1 элемент. И пока что сделаем через select
            dict_title.clear()
            dict_title = select_sql_db('SELECT * FROM  `title`')
            print ("NOT_Title_0 - ", new_elem["Title_0"])

        ################# Title_1
        new_info_select_db = dict_title.get(new_elem["Title_1"].upper())
        # Если ключа нет то добавляем если есть то print
        if new_info_select_db is None:
            # в словаре ключа нет по данному наименованию, можно добавлять
            # Вызываем функцию поступления новых данных
            query = "INSERT INTO `title`(`name`) VALUES (\"" + new_elem["Title_1"] + "\")"
            sql_insert(query)
            ################################
            # Обновляем словарь - можно было вставлять лишь 1 элемент. И пока что сделаем через select
            dict_title.clear()
            dict_title = select_sql_db('SELECT * FROM  `title`')
            print ("NOT_Title_1 - ", new_elem["Title_1"])

        ###############################
        # Проверяем дату
        id_match_select = select_sql_db(
            'select `id`, `number` from `matchid` where number = "' + str(new_elem["MatchID"]) + '"')

        # Проверяем имеется ли ид матча если {} занчить нет надо добавить
        if id_match_select == {}:
            print("id match is none")
            # Добавляем ид матча и находим сразу его ид
            # Вызываем функцию поступления новых данных
            query = "INSERT INTO `matchid`(`number`) VALUES (\"" + str(new_elem["MatchID"]) + "\")"
            sql_insert(query)
            id_match_select = select_sql_db(
                'select `id`, `number` from `matchid` where number = "' + str(new_elem["MatchID"]) + '"')

        # проверяем дату по матчу
        id_match = id_match_select.get(str(new_elem["MatchID"]))
        date_select = select_sql_db(
            'SELECT `ID_MatchID`, `name_date` FROM `dateofmatchlocalized` WHERE name_date = "'
            + str(new_elem["DateOfMatchLocalized"]) + '" and ID_MatchID = ' + str(id_match) + '')

        # Данных по дате не пришло, значить добавляем дату матча
        if date_select == {}:
            print("date is None")
            sql_insert('INSERT INTO `dateofmatchlocalized`(`ID_MatchID`, `name_date`) '
                       'VALUES (' + str(id_match) + ',\"' + str(new_elem["DateOfMatchLocalized"]) + '\")')

        ############################################
        # MarketsCount
        date_select = select_sql_db(
                'SELECT `ID_MatchID`, `number` FROM `marketscount` WHERE number = '
                + str(new_elem["MarketsCount"]) + ' and ID_MatchID = "' + str(new_elem["MatchID"]) + '"')

        if date_select == {}:
            print("MarketsCount - ", date_select)
            """"""
            sql_insert('INSERT INTO `marketscount`(`ID_MatchID`, `number`) '
                       'VALUES (\"' + str(new_elem["MatchID"]) + '\",' + str(new_elem["MarketsCount"]) + ')')

        ############################################
        # MarketID
        date_select = select_sql_db(
            'SELECT `ID_MatchID`, `number` FROM `marketid` WHERE number = '
            + str(new_elem["MarketID"]) + ' and ID_MatchID = "' + str(id_match) + '"')

        if date_select == {}:
            print("MarketID - ", date_select)
            sql_insert('INSERT INTO `marketid`(`ID_MatchID`, `number`) '
                       'VALUES (' + str(id_match) + ',\"' + str(new_elem["MarketID"]) + '\")')

    """
    sql_insert(
        str(new_elem["MarketID"]),
        str(new_elem["Value_0"]),
        str(new_elem["Value_1"]),

        str(new_elem["Tournament"]),
        str(new_elem["Category"]),
        str(new_elem["SportType"]),
        str(new_elem["DateOfMatchLocalized"]),
        str(new_elem["MarketsCount"]),
        str(new_elem["MatchID"]),
        str(new_elem["Title_0"]),
        str(new_elem["Title_1"]),
    )
    """

    # Здесь мы без проверок сразу данные вставляем, расчет на то, что программ один раз сделает вставку
    # и дальше будет в постоянной работе работать и не производить подобных вставок в начале работы

    i = 0
    while True:
        i += 1
        old_json_data = new_json_data
        try:
            new_json_data = parse_courses(ALL_GAME)

            for old_elem in old_json_data:
                for new_elem in new_json_data:
                    if new_elem["MatchID"] == old_elem["MatchID"] and new_elem["MarketID"] == old_elem["MarketID"]:
                        if new_elem["Value_0"] != old_elem["Value_0"]:
                            print("val 0")

                        if new_elem["Value_1"] != old_elem["Value_1"]:
                            print("val 1")

                        """
                        sql_insert(
                            str(new_elem["DateOfMatchLocalized"]),
                            str(new_elem["MarketsCount"]),

                            str(new_elem["MatchID"]),
                            str(new_elem["MarketID"]),

                            str(new_elem["Value_0"]),
                            str(new_elem["Value_1"]),

                            str(new_elem["Tournament"]),
                            str(new_elem["Category"]),
                            str(new_elem["Title_0"]),
                            str(new_elem["Title_1"]),
                            str(new_elem["SportType"]),
                        )
                        """
        except:
            print("not json data")

        print (i)
        time.sleep(1.0)


def parse_courses(url):
    opener = urllib2.build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0')]

    try:
        print ("go parse")
        f = opener.open(url)
        print ("try - opener.open(url)")
    except:
        print ("except - opener.open(url)")
        return []

    f_read = f.read()

    i = 0
    list = []

    try:
        print ("1 = try")
        f_json = json.loads(f_read)
        print ("try - f_json")
        while True:
            try:
                list.append({
                    "DateOfMatchLocalized": str(f_json[i]["DateOfMatchLocalized"]["Value"]),
                    "MarketsCount": f_json[i]["MarketsCount"],
                    "MatchID": f_json[i]["PreviewOdds"][0]["MatchID"],
                    "MarketID": f_json[i]["PreviewOdds"][0]["MarketID"],
                    "Title_0": f_json[i]["PreviewOdds"][0]["Title"],
                    "Title_1": f_json[i]["PreviewOdds"][1]["Title"],
                    "Value_0": f_json[i]["PreviewOdds"][0]["Value"],
                    "Value_1": f_json[i]["PreviewOdds"][1]["Value"],
                    "Tournament": f_json[i]["Tournament"]["Name"],
                    "Category": f_json[i]["Category"]["Name"],
                    "SportType": f_json[i]["SportType"]["Name"],
                })
                i += 1
            except:
                break

    except ValueError:
        print("error json structure")

    opener.close()
    print("list end")
    return list


if __name__ == "__main__":
    main()
