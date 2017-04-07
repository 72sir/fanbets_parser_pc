# coding: utf-8
import threading
import time
import json
import urllib2
import mysql.connector

from mysql.connector import Error
from django.http import HttpResponse
from django.shortcuts import render

ALL_GAME = 'http://betting.gg11.bet/api/sportmatch/Get?sportID=2357'


def sql_insert(*args):
    print(args)

    try:
        conn = mysql.connector.connect(host='k72gsi3s.beget.tech',
                                       database='k72gsi3s_dj2',
                                       user='k72gsi3s_dj2',
                                       password='1234567890')
        if conn.is_connected():
            query = "INSERT INTO `game_data` (`DateOfMatchLocalized`, `MarketsCount`, `MatchID`, `MarketID`, `Title_0`, " \
                    "`Title_1`, `Value_0`, `Value_1`, `Tournament`, `Category`, `SportType`) " \
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cursor = conn.cursor()
            cursor.execute(query, args)

            if cursor.lastrowid:
                print('last insert id', cursor.lastrowid)
            else:
                print('last insert id not found')

            conn.commit()

    except Error as e:
        print(e)

    finally:
        cursor.close()
        conn.close()


def main():
    new_json_data = parse_courses(ALL_GAME)
    for new_elem in new_json_data:
        sql_insert(
            str(new_elem["DateOfMatchLocalized"]),
            str(new_elem["MarketsCount"]),
            str(new_elem["MatchID"]),
            str(new_elem["MarketID"]),
            str(new_elem["Title_0"]),
            str(new_elem["Title_1"]),
            str(new_elem["Value_0"]),
            str(new_elem["Value_1"]),
            str(new_elem["Tournament"]),
            str(new_elem["Category"]),
            str(new_elem["SportType"]),
        )

    i = 0
    while True:
        i += 1
        old_json_data = new_json_data
        try:
            new_json_data = parse_courses(ALL_GAME)

            for old_elem in old_json_data:
                for new_elem in new_json_data:
                    if new_elem["MatchID"] == old_elem["MatchID"] and new_elem["MarketID"] == old_elem["MarketID"] and \
                                    new_elem["Value_0"] != old_elem["Value_0"] and new_elem["Value_1"] != old_elem[
                        "Value_1"]:
                        sql_insert(
                            str(new_elem["DateOfMatchLocalized"]),
                            str(new_elem["MarketsCount"]),
                            str(new_elem["MatchID"]),
                            str(new_elem["MarketID"]),
                            str(new_elem["Title_0"]),
                            str(new_elem["Title_1"]),
                            str(new_elem["Value_0"]),
                            str(new_elem["Value_1"]),
                            str(new_elem["Tournament"]),
                            str(new_elem["Category"]),
                            str(new_elem["SportType"]),
                        )
                        print ("new_elem - ", new_elem)
        except:
            print("not json data")

        print (i)
        time.sleep(0.1)


def write_file_json_data(old_json, new_json):
    pass


def parse_courses(url):
    opener = urllib2.build_opener()
    opener.addheaders = [('User-Agent', 'Mozilla/5.0')]

    f = opener.open(url)
    f_read = f.read()

    i = 0
    list = []

    try:
        f_json = json.loads(f_read)
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

    return list


if __name__ == "__main__":
    main()
