import time
import requests
import psycopg2

from bs4 import BeautifulSoup

NET_KEIBA_RACEDATA_URL = "http://db.netkeiba.com/race/"
RACE_PLACE = {  "01":"札幌",
                "02":"函館",
                "03":"福島",
                "04":"新潟",
                "05":"東京",
                "06":"中山",
                "07":"中京",
                "08":"京都",
                "09":"阪神",
                "10":"小倉",
            }

def convert_result_record_to_list(record):
    l = list()
    for row in record.findAll("td"):
        if row.find("a"):
            l.append(row.a.get("href"))
        l.append(row.text.replace("\n", ""))
    return l

def convert_result_record_to_dictionary(record):
    d = dict()
    d["着順"] = record.findAll("td")[0].text.replace("\n", "")
    d["枠番"] = record.findAll("td")[1].text.replace("\n", "")
    d["馬番"] = record.findAll("td")[2].text.replace("\n", "")
    d["馬名"] = record.findAll("td")[3].text.replace("\n", "")
    d["馬名URL"] = record.findAll("td")[3].a.get("href")
    d["年齢"] = record.findAll("td")[4].text.replace("\n", "")
    d["斤量"] = record.findAll("td")[5].text.replace("\n", "")
    d["騎手"] = record.findAll("td")[6].text.replace("\n", "")
    d["騎手URL"] = record.findAll("td")[6].a.get("href")
    d["タイム"] = record.findAll("td")[7].text.replace("\n", "")
    d["着差"] = record.findAll("td")[8].text.replace("\n", "")
    d["通過"] = record.findAll("td")[10].text.replace("\n", "")
    d["上り"] = record.findAll("td")[11].text.replace("\n", "")
    d["単勝"] = record.findAll("td")[12].text.replace("\n", "")
    d["人気"] = record.findAll("td")[13].text.replace("\n", "")
    d["馬体重"] = record.findAll("td")[14].text.replace("\n", "")
    d["調教師"] = record.findAll("td")[18].text.replace("\n", "")
    d["調教師URL"] = record.findAll("td")[18].a.get("href")
    d["馬主"] = record.findAll("td")[19].text.replace("\n", "")
    d["馬主URL"] = record.findAll("td")[19].a.get("href")
    d["賞金"] = record.findAll("td")[20].text.replace("\n", "")

    return d

def get_race_data(year, place, place_count, day, race_count, update = 0):
    race_id = str(year) + place + '{0:02d}'.format(place_count) + '{0:02d}'.format(day) + '{0:02d}'.format(race_count)
    result_header, result_detail = get_race_data_by_id(race_id, update)
    return race_id, result_header, result_detail

def get_race_data_by_id(race_id, update = 0):
    host = "127.0.0.1"
    port = "5432"
    dbname = "netkeiba"
    user = "vagrant"
    password = "vagrant"

    connection_str = "host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user + " password=" + password

    with psycopg2.connect(connection_str) as connection:
        cur = connection.cursor()

        cur.execute("select * from racedataheader where race_id = %s", [race_id])
        if cur.fetchone() is not None:
            print("すでに存在します。[%s]" % race_id)

            return None, None

    time.sleep(1)
    with requests.get(NET_KEIBA_RACEDATA_URL + race_id + "/") as r:
        r.encoding = "euc-jp"
#        print(r.url)
        bsObj = BeautifulSoup(r.text, "html.parser")

        try:
            result_header = list()
            result_header.append(bsObj.find("div", {"class": "mainrace_data fc"}).find("dt").text.strip())
            result_header.append(bsObj.find("div", {"class": "mainrace_data fc"}).find("h1").text)
            result_header.extend(bsObj.find("div", {"class": "mainrace_data fc"}).find("diary_snap_cut").text.strip().split(' / '))

            result_detail = list()
            for row in bsObj.find("table", {"class": "race_table_01 nk_tb_common"}).findAll("tr"):
                if len(row.findAll("td")) > 0:
    #                result_list.append(convert_result_record_to_list(row))
                    result_detail.append(convert_result_record_to_dictionary(row))

        except Exception as ex:
            print(ex)
            print("レース結果が取得できませんでした。[%s]" % r.url)
            return None, None

        return result_header, result_detail

    return None, None

def save_to_db(race_id, h, detail, update = 0):

    if "障" in h[2]:
        print("障害のレースの為保存しません。[%s]" % race_id)
        return

    if "直線" in h[2]:
        print("直線のレースの為保存しません。[%s]" % race_id)
        return

    host = "127.0.0.1"
    port = "5432"
    dbname = "netkeiba"
    user = "vagrant"
    password = "vagrant"

    connection_str = "host=" + host + " port=" + port + " dbname=" + dbname + " user=" + user + " password=" + password

    with psycopg2.connect(connection_str) as connection:
        cur = connection.cursor()

        cur.execute("select * from racedataheader where race_id = %s", [race_id])
        if cur.fetchone() is None:
            header_list = list()
            header_list.append(race_id)  # race_id
            header_list.append(race_id[:4])  # 年
            header_list.append(race_id[4:6])  # 場所コード
            header_list.append(h[0])  # レース順
            header_list.append(h[1])  # レース名
            header_list.append(h[2][1].strip())  # 向き
            header_list.append(h[2][2:].strip())
            header_list.append(h[3][h[3].find(":") + 1:].strip())
            header_list.append(h[4][:h[4].find(":")].strip())
            header_list.append(h[4][h[4].find(":") + 1:].strip())

            print(header_list)
            try:
                cur.execute(
                    "insert into racedataheader values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    header_list)
                connection.commit()

                for d in detail:
                    detail_list = list()
                    detail_list.append(race_id)
                    detail_list.append(d["着順"])
                    detail_list.append(d["枠番"])
                    detail_list.append(d["馬番"])
                    detail_list.append(d["馬名"])
                    detail_list.append(d["馬名URL"])
                    detail_list.append(d["年齢"])
                    detail_list.append(d["斤量"])
                    detail_list.append(d["騎手"])
                    detail_list.append(d["騎手URL"])
                    detail_list.append(d["タイム"])
                    detail_list.append(d["着差"])
                    detail_list.append(d["通過"])
                    detail_list.append(d["上り"])
                    detail_list.append(d["単勝"])
                    detail_list.append(d["人気"])
                    detail_list.append(d["馬体重"])
                    detail_list.append(d["調教師"])
                    detail_list.append(d["調教師URL"])
                    detail_list.append(d["馬主"])
                    detail_list.append(d["馬主URL"])
                    detail_list.append(d["賞金"])

                    cur.execute(
                        "insert into racedatadetail values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        detail_list)
                    connection.commit()
                print("データベースを更新しました。[%s]" % race_id)
            except:
                pass
        else:
            print("すでに存在します。[%s]" % race_id)


def scraping(from_year, to_year):
    for year in range(from_year, to_year + 1):
        for place_id in RACE_PLACE:
            for place_count in range(1, 6):
                race_id, h, d = get_race_data(year, place_id, place_count, 1, 1)
                if h is None:
                    break

                for days in range(1, 10):
                    race_id, h, d = get_race_data(year, place_id, place_count, days, 1)
                    if h is None:
                        break

                    for race_count in range(1, 13):
                        race_id, h, d = get_race_data(year, place_id, place_count, days, race_count)
                        if days == 1 and h is not None:
                            break

                        if d is not None:
                            save_to_db(race_id, h, d)

scraping(2009, 2018)

#race_id = "200608050603"
#h, d = get_race_data_by_id(race_id)
#print(race_id)
#print(h)
#print(d)
#save_to_db(race_id, h, d)
