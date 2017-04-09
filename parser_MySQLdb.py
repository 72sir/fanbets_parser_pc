# coding: utf-8

import mysql.connector

from mysql.connector import MySQLConnection, Error


# 1. Вычисляем все ли параметры имеются в базе
# категория - Тип спорта - Тоурнамент - Номер Матча - Номер Маркера - Тайтлы команд
# Ищем дату - с ид матча
# связующая таблица тайтала и матча в которой имеется ид_связь_тайтла_команда
# и в основной таблице коэффициентов добавляем свзяку ид_связь_тайтла_команда - коэффициент


def select_sql_db(select):
    list = []
    print("select - ", select)
    try:
        conn2 = mysql.connector.connect(host='k72gsi3s.beget.tech',
                                        database='k72gsi3s_dj2',
                                        user='k72gsi3s_dj2',
                                        password='1234567890')
        cursor2 = conn2.cursor()
        cursor2.execute(select)
        row = cursor2.fetchone()
        list.append(row[0])
        print ("select_sql_db row - ", row)

    except Error as e:
        print(e)

    finally:
        cursor2.close()
        conn2.close()

    return list


def find_sql_result(id):
    try:
        list = []
        conn = mysql.connector.connect(host='k72gsi3s.beget.tech',
                                       database='k72gsi3s_dj2',
                                       user='k72gsi3s_dj2',
                                       password='1234567890')
        if id == 0:
            j = 2  # MatchID
            table_name = "`DateOfMatchLocalized`"
        elif id == 1: # MarketsCount
            j = 2  # MatchID
            table_name = "`MarketsCount`"
        elif id == 2:
            table_name = "`MatchID`"
        elif id == 3:
            j = 2  # MatchID
            table_name = "`MarketID`"
        elif id == 4:
            j = 2  # MatchID
            table_name = "`Title`"
        elif id == 5:
            j = 2  # MatchID
            table_name = "`Title`"
        elif id == 6: # Value_0
            j = 2  # MatchID
            table_name = "`Value`"
        elif id == 7: # Value_1
            j = 2  # MatchID
            table_name = "`Value`"
        elif id == 8:
            table_name = "`Tournament`"
            j = 2  # MatchID
        elif id == 9:
            table_name = "`Categoria`"
        elif id == 10:
            j = 2  # MatchID
            table_name = "`SportType`"
        else:
            j = 0  #
            table_name = "game_data"

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM " + table_name)
        row = cursor.fetchone()

        while row is not None:
            list.append(row[1])
            row = cursor.fetchone()

        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM game_data")

            row = cursor.fetchone()

            while row is not None:
                bool = True
                for elem in list:
                    if elem == row[id]:
                        print("ok")
                        # bool = False

                if bool:
                    id_matchid = select_sql_db("SELECT id, number FROM `MatchID` WHERE number = \"" + row[j] + "\"")
                    id_title = select_sql_db("SELECT * FROM `Title` WHERE name=\"" + row[5] + "\" ORDER BY `name` ASC")

                    print("bool", row[id], id_title[0], id_matchid[0])
                    # Находи ид матча для создания инсёрта вставки в базу
                    """
                    'INSERT INTO `Value`(`id_MatchID`, `id_TitleID`, `number`) VALUES ([value-1],[value-2],[value-3])'
                    """

                    query = "INSERT INTO `Value`(`id_MatchID`, `id_TitleID`, `number`) VALUES(" + str(id_matchid[0]) + "," \
                            + str(id_title[0]) + "," + row[id] + ")"
                    print("query - ", query)

                    try:
                        print ("if bool:", row[id], row[j])
                        conn2 = mysql.connector.connect(host='k72gsi3s.beget.tech',
                                                        database='k72gsi3s_dj2',
                                                        user='k72gsi3s_dj2',
                                                        password='1234567890')
                        if conn2.is_connected():
                            print("conn2 - ", row[id])

                            list.append(str(row[1]))

                            cursor2 = conn2.cursor()
                            cursor2.execute(query)

                            if cursor2.lastrowid:
                                print('last insert id ', cursor2.lastrowid)
                            else:
                                print('last insert id not found')
                            conn2.commit()

                    except Error as e:
                        print(e)

                    finally:
                        cursor2.close()
                        conn2.close()


                row = cursor.fetchone()

        except Error as e:
            print(e)

    except Error as e:
        print(e)

    print ("exit find_sql_result ")


def main():
    try:
        # 1. Добавим в базу все категории которых нет и выводим в список.
        find_sql_result(7)

    except Error as e:
        print(e)


if __name__ == "__main__":
    main()
    print("exit")
