#! /usr/bin/env python
# -*- coding: utf-8 -*-

from urllib.request import urlopen
from bs4 import BeautifulSoup
import psycopg2
from urllib.request import urlretrieve
import os
import logging
from time import sleep
from random import uniform
import csv
from contextlib import closing
import datetime

now = datetime.datetime.now()

log_f = "log_of_parser/"

if not os.path.exists(log_f):
    os.makedirs(log_f)

log_f = log_f + "log_of_parser" + "_" + str(now) + ".log"

fh = logging.FileHandler("log", "w")

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

url_whole = {1343: [
    # "https://market.yandex.ru/catalog--osvezhiteli-vozdukha/55080/list?hid=90689&glfilter=15714059%3A15714816%2C16357745%2C15714064%2C15715065%2C15714066&local-offers-first=0&viewtype=list",
    # "https://market.yandex.ru/catalog--tovary-dlia-doma-sberbank/73570/list?hid=16033837&glfilter=16034147%3A16034148&local-offers-first=0&viewtype=list",
    # "https://market.yandex.ru/catalog--dekorativnye-svechi/54625/list?hid=91304&glfilter=12860530%3A1&local-offers-first=0&viewtype=list",
    "https://market.yandex.ru/catalog--efirnye-masla/54735/list?hid=818945&glfilter=14421791%3A16106321&local-offers-first=0&viewtype=list",
],
    1339: [
        "https://market.yandex.ru/catalog--tualetnaia-bumaga-i-bumazhnye-polotentsa/70836/list?hid=15002303&glfilter=15002344%3A15002373&local-offers-first=0&viewtype=list"],
    1338: [
        "https://market.yandex.ru/catalog--tualetnaia-bumaga-i-bumazhnye-polotentsa/70836/list?hid=15002303&glfilter=15002344%3A15002400&local-offers-first=0&viewtype=list"],
    1340: [
        "https://market.yandex.ru/catalog--bumazhnye-polotentsa-i-salfetki/65613/list?hid=91173&glfilter=8492368%3A13360580&local-offers-first=0&viewtype=list"],
    1344: [
        "https://market.yandex.ru/catalog--paletki-pomad-i-bleskov-dlia-gub/16681159/list?hid=14996541&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--gubnaia-pomada/16681160/list?hid=4748057&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--bleski-i-tinty-dlia-gub/16681161/list?hid=8480752&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kontur-dlia-gub/16681164/list?hid=8510396&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--khailaitery-i-skulpturiruiushchie-sredstva/16681215/list?hid=14996659&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--tonalnye-sredstva-dlia-litsa/16681216/list?hid=13276667&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--bb-cc-i-dd-kremy/16681218/list?hid=14996686&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--osnova-i-fiksatory-dlia-makiiazha/16681219/list?hid=13276669&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--pudra-dlia-litsa/16681220/list?hid=4748072&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--rumiana-i-bronzery-dlia-litsa/16681221/list?hid=4748074&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--korrektory-i-konsilery-dlia-litsa/16681222/list?hid=8480754&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--tush-dlia-resnits/16681206/list?hid=4748062&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--teni-dlia-vek/16681210/list?hid=4748064&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kontur-dlia-glaz/16681212/list?hid=4748066&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--karandashi-dlia-brovei/16681197/list?hid=13276918&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kraska-dlia-brovei-i-resnits/16681198/list?hid=13276920&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--tush-i-gel-dlia-brovei/16681201/list?hid=14995788&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--teni-i-nabory-tenei-dlia-brovei/16681202/list?hid=14995813&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kisti-sponzhi-i-applikatory-dlia-makiiazha/16681132/list?hid=13277088&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--resnitsy-i-klei/16681133/list?hid=13277089&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--zerkala-kosmeticheskie/16681134/list?hid=13277104&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--melochi-dlia-makiiazha/16681135/list?hid=13277108&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--zazhimy-i-rascheski-dlia-resnits/16681136/list?hid=15000738&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--vremennye-tatuirovki/16681140/list?hid=7774311&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--nabory-dekorativnoi-kosmetiki/16680823/list?hid=4748078&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-ochishcheniia-i-sniatiia-makiiazha/16680825/list?hid=8476098&local-offers-first=0&viewtype=list"
    ],
    1348: [
        "https://market.yandex.ru/catalog--gigienicheskie-prokladki-i-tampony/16418222/list?hid=13314855&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--urologicheskie-prokladki/16418223/list?hid=14456451&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-intimnoi-gigieny/16418225/list?hid=14994695&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--vlazhnye-salfetki/16418232/list?hid=14995755&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--vatnye-palochki-i-diski/16418237/list?hid=7693914&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-kupaniia-malyshei/16418246/list?hid=989024&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--prisypki-i-kremy-pod-podguznik/16418254/list?hid=13475285&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--vlazhnye-salfetki-dlia-malyshei/16418255/list?hid=13475238&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--prezervativy/16418226/list?hid=13744375&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--intimnye-smazki/16418227/list?hid=14695400&local-offers-first=0&viewtype=list"
    ],
    1341: [
        "https://market.yandex.ru/catalog--stiralnyi-poroshok/55082/list?hid=90688&track=pieces&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--otbelivateli-i-piatnovyvoditeli/55081/list?hid=90691&track=pieces&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--aksessuary-dlia-stirki-belia/64756/list?hid=13041154&track=pieces&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--geli-i-zhidkie-sredstva-dlia-stirki/64772/list?hid=13041429&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--konditsionery-i-opolaskivateli-dlia-belia/64774/list?hid=13041430&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--khoziaistvennoe-mylo/64778/list?hid=13041456&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kapsuly-tabletki-plastiny-dlia-stirki/72055/list?hid=15696738&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-glazheniia-podkrakhmalivateli-antistatiki/16395162/list?hid=15934091&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--gladilnye-doski/55197/list?hid=1564519&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sushilki-dlia-belia/55202/list?hid=6496840&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--mashinki-dlia-udaleniia-katyshkov/64758/list?hid=13041163&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--aksessuary-dlia-sushki-belia/71959/list?hid=15645424&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--veshalki-napolnye/71999/list?hid=15687757&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--veshalki-nastennye/72001/list?hid=15687765&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--aksessuary-dlia-glazheniia-belia/73314/list?hid=15989012&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--chekhly-dlia-gladilnykh-dosok/64754/list?hid=13041121&local-offers-first=0&viewtype=list"
    ],

    1346: [
        "https://market.yandex.ru/catalog--shampuni-dlia-volos/16681313/list?hid=91183&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--balzamy-opolaskivateli-i-konditsionery-dlia-volos/16681315/list?hid=91184&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--maski-i-syvorotki-dlia-volos/16681318/list?hid=4854062&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-okrashivaniia-volos/16681319/list?hid=13239454&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sukhie-i-tverdye-shampuni-dlia-volos/16681324/list?hid=13239358&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-ukladki-volos/16681325/list?hid=13239480&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-khimicheskoi-zavivki-volos/16681332/list?hid=15001132&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--feny-i-fen-shchetki/16681333/list?hid=16336734&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--elektrobigudi/16681335/list?hid=90573&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--bigudi-dlia-volos/16681340/list?hid=13244155&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--rascheski-i-shchetki-dlia-volos/16681346/list?hid=13243353&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--zakolki-dlia-volos/16681348/list?hid=14993459&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--rezinki-obodki-poviazki-dlia-volos/16681349/list?hid=14993483&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--shchiptsy-ploiki-i-vypriamiteli/16681350/list?hid=16336768&local-offers-first=0&viewtype=list"
    ],
    1345: [
        "https://market.yandex.ru/catalog--uvlazhnenie-i-pitanie-kozhi-litsa/16693461/list?hid=8476099&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--maski-dlia-litsa/16693462/list?hid=8476097&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--antivozrastnoi-ukhod/16693463/list?hid=15011042&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--skraby-i-pilingi-dlia-litsa/16693459/list?hid=8476539&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-tonizirovaniia-kozhi-litsa/16693460/list?hid=8476100&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--pribory-dlia-ukhoda-za-litsom/16693464/list?hid=13203592&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-ukhoda-za-kozhei-gub/16693466/list?hid=8476102&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-ukhoda-za-kozhei-vokrug-glaz/16693467/list?hid=8476101&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-zagara-i-zashchity-ot-solntsa-dlia-litsa/16693468/list?hid=8476103&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-problemnoi-kozhi-litsa/16693469/list?hid=8476110&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-po-ukhodu-za-litsom-dlia-muzhchin/16693470/list?hid=8480713&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kosmeticheskie-nabory/16693471/list?hid=8480738&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--akses suary-dlia-ukhoda-za-litsom/16734794/list?hid=16133574&local-offers-first=0&viewtype=list",

        "https://market.yandex.ru/catalog--avtozagar-i-sredstva-dlia-soliariia-dlia-tela/16693450/list?hid=13314823&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--solntsezashchitnye-sredstva-dlia-tela/16693451/list?hid=14993676&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-ukhoda-za-kozhei-tela-posle-zagara/16693452/list?hid=13314841&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--aksessuary-dlia-ukhoda-za-telom/16693444/list?hid=6470214&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--mochalki-i-shchetki-dlia-vanny-i-dusha/16693447/list?hid=16042844&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--tualetnoe-i-zhidkoe-mylo/16693453/list?hid=14989652&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--pena-sol-maslo-dlia-vanny/16693455/list?hid=14994526&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-po-ukhodu-za-kozhei-ruk/16693434/list?hid=4852773&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-po-ukhodu-za-kozhei-nog/16693435/list?hid=4852774&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--kremy-i-losony-dlia-tela/16693431/list?hid=8475955&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--skraby-i-pilingi-dlia-tela/16693454/list?hid=14989707&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-dusha/16693430/list?hid=91176&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-pokhudeniia-i-borby-s-tselliulitom/16693432/list?hid=8475961&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--gigienicheskie-vkladyshi-dlia-odezhdy/16693448/list?hid=16000235&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--dezodoranty-dlia-zhenshchin/16693433/list?hid=91167&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-do-i-posle-depiliatsii/16693441/list?hid=91180&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--zhenskie-sredstva-dlia-depiliatsii/16693440/list?hid=91179&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--ukhod-za-kozhei-malyshei/16681246/list?hid=13491512&local-offers-first=0&viewtype=list"
    ],

    1347: [
        "https://market.yandex.ru/catalog--zubnaia-pasta/16402864/list?hid=13334231&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--gigiena-polosti-rta-dlia-detei/16402866/list?hid=13491643&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--poloskanie-i-ukhod-za-polostiu-rta/16402870/list?hid=91174&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--zubnye-shchetki/16402883/list?hid=13314877&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--elektricheskie-zubnye-shchetki/16479372/list?hid=278374&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--irrigatory/16479469/list?hid=7683675&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--aksessuary-dlia-zubnykh-shchetok-i-irrigatorov/16479562/list?hid=7683677&local-offers-first=0&viewtype=list"

    ],
    1342: [
        "https://market.yandex.ru/catalog--chistiashchie-sredstva-dlia-kafelia-santekhniki-i-trub/64780/list?hid=13041460&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-mebeli-kovrov-i-napolnykh-pokrytii/64788/list?hid=13041512&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-chistki-kukhonnykh-poverkhnostei/64784/list?hid=13041507&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--sredstva-dlia-mytia-okon-i-zerkal/64786/list?hid=13041511&local-offers-first=0&viewtype=list",
        "https://market.yandex.ru/catalog--chistiashchie-prinadlezhnosti-dlia-kompiuternoi-tekhniki/65607/list?hid=91078&local-offers-first=0&viewtype=list"
    ],

}


def get_total_pages(url):
    while True:
        try:
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


path = "/usr/local/share/gpb_pfm/files/goods-"  # To save images

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

            data = [] #Для записи в csv

            with closing(psycopg2.connect(dbname='pfm1', user='postgres',
                                          password='1735799172', host='localhost')) as conn:
                with conn.cursor() as cursor:
                    with conn.cursor() as cursor1:

                        for d in info:
                            d.append(d[1])
                            cursor.execute(
                                '''INSERT INTO goods (product_id, name, description, image) select %s, %s, %s, %s
                                WHERE NOT EXISTS (SELECT product_id from goods where name = %s)''', d)
                            del d[4]

                        cursor.execute('''select image, id, name from goods where product_id = %s''', [product_id])
                        with open('test.csv', 'a') as fp:
                            for row in cursor:
                                if row[0] != '' and not row[0].startswith("goods-"):
                                    destination = path + str(row[1]) + ".png"
                                    url = "http:" + row[0]
                                    urlretrieve(url, destination)
                                    data.append((row[1], row[0], row[2]))

                            writer = csv.writer(fp, delimiter=',')
                            writer.writerows(data)

                        cursor.execute('SELECT id from goods where product_id = %s', [product_id])

                        for row in cursor:
                            destination = path + str(row[0]) + ".png"
                            if os.path.exists(destination):
                                sett = "goods-" + str(row[0]) + ".png"
                                cursor1.execute('UPDATE goods SET image = %s WHERE id = %s and product_id = %s',
                                                (sett, row[0], product_id))

                        conn.commit()

            print(len(info))
            print(info)
