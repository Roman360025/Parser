#! /usr/bin/env python
# -*- coding: utf-8 -*-

from urllib.request import urlopen
from bs4 import BeautifulSoup
import logging
from time import sleep
from random import uniform
import csv
from contextlib import closing
import psycopg2
import os
from datetime import datetime

now = datetime.now()

now = str(now).replace(".", "_")

path = "log_parser_animals/"

log_f = path

if not os.path.exists(log_f):
    os.makedirs(log_f)

log_f = log_f + "log_of_parser_animalse" + "_" + str(now).replace(" ", "_") + ".log"

log_f = log_f.replace(" ", "_")[:-7].replace("-", "_").replace(":", "_")

fh = logging.FileHandler(log_f, "w")

log = logging.getLogger('Error')
log_inf = logging.getLogger("Info")

log.setLevel(logging.INFO)
log_inf.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
fh.setFormatter(formatter)

log.addHandler(fh)
log_inf.addHandler(fh)

# url = "https://market.yandex.ru/catalog--uvlazhnenie-i-pitanie/57356/list?text=%D0%A4%D0%BB%D1%8E%D0%B8%D0%B4%20%D0%B4%D0%BB%D1%8F%20%D0%BB%D0%B8%D1%86%D0%B0%20Alterra%20%D0%A3%D0%B2%D0%BB%D0%B0%D0%B6%D0%BD%D1%8F%D1%8E%D1%89%D0%B8%D0%B9%20%D0%B4%D0%BB%D1%8F%20%D1%81%D0%BA%D0%BB%D0%BE%D0%BD%D0%BD%D0%BE%D0%B9%20%D0%BA%20%D0%B2%D0%BE%D1%81%D0%BF%D0%B0%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%D0%BC%20%D0%B8%20%D0%B6%D0%B8%D1%80%D0%BD%D0%BE%D0%B9%20%D0%BA%D0%BE%D0%B6%D0%B8&cvredirect=3&track=srch_visual&glfilter=8512725%3A8512737&glfilter=15210253%3A1&glfilter=15210275%3A1&onstock=0&deliveryincluded=0&local-offers-first=0&viewtype=list"
# 1343: Ароматизаторы для дома
# 1339: Бумажные полотенца
# 1338: Туалетная бумага
# 1340: Бумажные салфетки
# 1344: Декоративная косметика
# 1348: Средства личной гигиены
# 1341: Стирка и уход за бельём
# 1346: Уход за волосами
# 1345: Уход за кожей
# 1347: Уход за полостью рта
# 1342: Чистящие средства

url_whole = {1335: [
    "https://market.yandex.ru/catalog--sredstva-dlia-ukhoda-i-gigieny-zhivotnykh/62198/list?hid=12718223&glfilter=12761301%3A12761325%2C12775270%2C12761352%2C12761547%2C12775272%2C15954294%2C12761323%2C12761327&local-offers-first=0&viewtype=list",
],
    1336: [
        "https://market.yandex.ru/catalog--sredstva-dlia-ukhoda-i-gigieny-zhivotnykh/62198/list?hid=12718223&glfilter=12751452%3A12761546&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--tualety-pelenki-dlia-koshek-i-sobak/62821/list?hid=12704139&glfilter=12733616%3A12733654%2C12733653&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--tualety-i-pelenki-dlia-sobak/62815/list?hid=15962102&glfilter=15962218%3A15962233%2C15962238&local-offers-first=0&viewtype=list"
    ],
    1332: [
        "https://market.yandex.ru/catalog--seno-i-napolniteli-dlia-gryzunov/62879/list?hid=12704208&glfilter=12769233%3A12785555&local-offers-first=0&viewtype=list"],
    1333: [
        "https://market.yandex.ru/catalog--seno-i-napolniteli-dlia-gryzunov/62879/list?hid=12704208&glfilter=12769233%3A12769235&local-offers-first=0&viewtype=list"
    ],
    1316: [
        "https://market.yandex.ru/catalog--shleiki-osheiniki-dlia-koshek/62823/list?hid=15967511&local-offers-first=0&viewtype=list"
    ],
    1311: [
        "https://market.yandex.ru/catalog--vlazhnye-korma-dlia-koshek/75263/list?hid=15685457&glfilter=13333460%3A13333462&local-offers-first=0&viewtype=list"
    ],
    1317: [
        "https://market.yandex.ru/catalog--igrushki-dlia-koshek/62827/list?hid=15959385&glfilter=12719164%3A15959797%2C12719186&local-offers-first=0&viewtype=list",
    ],

    1318: [
        "https://market.yandex.ru/catalog--kogtetochki-i-kompleksy-dlia-koshek/73285/list?hid=15963644&local-offers-first=0&viewtype=list"
    ],

    1313: [
        "https://market.yandex.ru/catalog--lakomstva-dlia-koshek/62819/list?hid=15963668&local-offers-first=0&viewtype=list",
    ],
    1319: [
        "https://market.yandex.ru/catalog--miski-dlia-koshek/62837/list?hid=12718255&glfilter=12719164%3A12719186&local-offers-first=0&viewtype=list"
    ],

    1320: [
        "https://market.yandex.ru/catalog--lezhaki-domiki-spalnye-mesta-dlia-koshek/62825/list?hid=12714755&glfilter=15953197%3A15953200&local-offers-first=0&viewtype=list"
    ],
    1315: [
        "https://market.yandex.ru/catalog--napolniteli-dlia-koshachikh-tualetov/62882/list?hid=12766642&local-offers-first=0&viewtype=list"
    ],
    1312: [
        "https://market.yandex.ru/catalog--sukhie-korma-dlia-koshek/75262/list?hid=15685457&glfilter=13333460%3A13333461&local-offers-first=0&viewtype=list"
    ],
    1326: [
        "https://market.yandex.ru/catalog--osheiniki-povodki-i-shleiki-dlia-sobak/62841/list?hid=12704731&glfilter=12719164%3A12719190&local-offers-first=0&viewtype=list"
    ],
    1322: [
        "https://market.yandex.ru/catalog--vlazhnye-korma-dlia-sobak/75266/list?hid=15685787&glfilter=13333460%3A13333462&local-offers-first=0&viewtype=list"
    ],
    1327: [
        "https://market.yandex.ru/catalog--igrushki-dlia-sobak/62862/list?hid=15959385&glfilter=12719164%3A15959797%2C12719190&local-offers-first=0&viewtype=list"
    ],
    1324: [
        "https://market.yandex.ru/catalog--lakomstva-dlia-sobak/62813/list?hid=12718332&local-offers-first=0&viewtype=list"
    ],
    1328: [
        "https://market.yandex.ru/catalog--miski-dlia-koshek/62837/list?hid=12718255&glfilter=12719164%3A12719190&local-offers-first=0&viewtype=list"
    ],
    1329: [
        "https://market.yandex.ru/catalog--lezhaki-i-domiki-dlia-sobak/62844/list?hid=12714755&glfilter=15953197%3A15953198&local-offers-first=0&viewtype=list"
    ],
    1323: [
        "https://market.yandex.ru/catalog--sukhie-korma-dlia-sobak/75265/list?hid=15685787&glfilter=13333460%3A13333461&local-offers-first=0&viewtype=list"
    ],

}


def get_total_pages(url):
    while True:
        try:
            sleep(uniform(3, 6))
            html = urlopen(url)
            break
        except:
            print("Error opening the URL")  # fix this
            log.info("Error to detect count of pages")

    soup = BeautifulSoup(html, 'html.parser')
    divs = soup.find('div', {"class": "n-filter-applied-results__content"})
    while divs == None:
        print("Error page")
        log.info("Error to detect count of pages")
        try:
            sleep(uniform(3, 6))
            html = urlopen(url)
        except:
            print("Error opening the URL")  # fix this
            log.info("Error openning the URL")
        soup = BeautifulSoup(html, 'html.parser')
        divs = soup.find('div', {"class": "n-filter-applied-results__content"})

    divs = soup.find('div', class_='n-pager')
    if divs == None:
        total_pages = 1
    else:
        string = str(divs)
        start = string.find(r'"pagesCount":')  # Находим, где находится число страниц
        end = string[start:].find(",")  # Находим, на какой позиции заканчивается эта цифра
        total_pages = int(string[start + 13:end + start])
        if total_pages > 50:
            total_pages = 50
    return total_pages, soup


for category in url_whole:
    product_id = str(category)
    for url in url_whole[category]:

        count, soup = get_total_pages(url)

        print(count)
        log_inf.info("Count of pages: {}".format(count))

        base_url = url
        page_part = "&page="

        for i in range(1, count + 1):
            info = []
            if i > 1:
                url = base_url + page_part + str(i)
                print(url)
                log_inf.info("Current url: {}".format(url))
                while True:
                    try:
                        sleep(uniform(3, 6))
                        page = urlopen(url)
                        break
                    except:
                        log.exception("Error openning the URL")  # fix this
                soup = BeautifulSoup(page, 'html.parser')
            content = soup.find('div', {"class": "n-filter-applied-results__content"})
            while True:
                try:
                    goods = content.findAll('div', class_='n-snippet-card2')
                    break
                except:
                    print("Error")
                    log.exception("Error to open the url")
                    while True:
                        sleep(uniform(3, 6))
                        try:
                            page = urlopen(url)
                            break
                        except:
                            print("Error opening the URL")  # fix this
                            log.exception("Error openning the URL")
                    soup = BeautifulSoup(page, 'html.parser')
                    content = soup.find('div', {"class": "n-filter-applied-results__content"})

            for j in goods:
                name = j.find('div', class_='n-snippet-card2__title')
                if name == None:
                    name = j.find('div', class_='n-snippet-cell2__title')
                name = name.text

                try:
                    description = j.find('ul', class_='n-snippet-card2__desc')
                    if description == None:
                        description = j.find('ul', class_='n-snippet-cell2__desc')
                    if description == None:
                        description = j.find('div', class_='n-snippet-card2__desc')
                    description_whole = ""

                    for des in description.contents:
                        description_whole = description_whole + des.text + '; '
                except:
                    description_whole = ""

                if description_whole == "":
                    try:
                        description_whole = description.text
                    except:
                        description_whole = ""

                if len(description_whole) >= 255:
                    description_whole = description_whole[:255]

                if len(name) >= 255:
                    name = name[:255]

                try:
                    src = j.find('img', class_='image')["src"]
                except:
                    src = ""

                info.append([product_id, name, description_whole, src])

            with open("data_animals.csv", "a", newline='', encoding='utf-8') as fp:
                writer = csv.writer(fp, delimiter=';')
                writer.writerows(info)

            with closing(psycopg2.connect(dbname='pfm', user='postgres',
                                          password='1735799172', host='localhost')) as conn:
                with conn.cursor() as cursor:
                    for d in info:
                        d.append(d[1])
                        cursor.execute(
                            '''INSERT INTO goods (product_id, name, description, image) select %s, %s, %s, %s
                            WHERE NOT EXISTS (SELECT product_id from goods where name = %s)''', d)
                        del d[4]

                conn.commit()

            print(len(info))
            print(info)
