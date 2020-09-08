import csv
from contextlib import closing
import psycopg2
import os
import logging
from datetime import datetime
import httplib2

now = datetime.now()

now = str(now).replace(".", "_")

path = "log/"

log_f = path

if not os.path.exists(log_f):
    os.makedirs(log_f)

log_f = log_f + "log_of_check" + "_" + str(now).replace(" ", "_") + ".log"

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

path = "/usr/local/share/gpb_pfm/files/goods-"

h = httplib2.Http('.cache')

# Заносим значения в БД, чтобы получить id
with closing(psycopg2.connect(dbname='pfm', user='postgres',
                              password='postgres', host='localhost')) as conn:
    with conn.cursor() as cursor:
        with conn.cursor() as cursor1:
            with open("data.csv", encoding='utf-8') as fp:
                reader = csv.reader(fp, delimiter=';')
                for row in reader:
                    # print(row)
                    if row != [] and len(row) == 4:
                        # row.append(row[1])
                        row[0] = int(row[0])
                        try:
                            cursor.execute(
                                '''INSERT INTO goods (product_id, name, description, image) select %s, %s, %s, %s
                                WHERE NOT EXISTS (SELECT product_id from goods where name = %s)''',
                                (row[0], row[1], row[2], row[3], row[1]))
                            # del row[4]
                            conn.commit()
                        except:
                            print("Не удалось добавить запись")
                            log.exception("Запись не добавлена")
                        cursor.execute('''select image, id from goods where product_id = %s and name = %s and description = %s
                        and image = %s''', row)
                        for row_d in cursor:
                            destination = path + str(row_d[1]) + ".png"
                        url = "http:" + row_d[0]
                        try:
                            # urlretrieve(url, destination)
                            response, content = h.request(url)
                            out = open(destination, 'wb')
                            out.write(content)
                            out.close()
                        except:
                            log.info("{} не скачалось".format(row[0]))
                            print("{} не скачалась".format(row_d[1]))

                        if os.path.exists(destination):
                            sett = "goods-" + str(row_d[1]) + ".png"
                            try:
                                cursor1.execute(
                                    'UPDATE goods SET image = %s WHERE id = %s and product_id = %s and name = %s and description = %s'
                                    'and image = %s', (sett, row_d[1], row[0], row[1], row[2], row[3]))
                                log_inf.info("{}, {} записалась, картинка {} скачалась".format(row[0], row_d[1], sett))
                            except:
                                print("Запись {}, {} не удалось обновить название картинки".format(row[0], row_d[1]))
                                log.exception(
                                    "У записи {}, {} не удалось обновить название картинки".format(row[0], row_d[1]))
                        else:
                            try:
                                cursor1.execute(
                                    '''UPDATE goods SET image = NULL WHERE id = %s and product_id = %s and name = %s and description = %s
                                    and image = %s''',
                                    (row_d[1], row[0], row[1], row[2], row[3]))
                            except:
                                print("Запись {}, {} не удалось обновить название картинки".format(row[0], row_d[1]))
                                log.exception(
                                    "У записи {}, {} не удалось обновить название картинки".format(row[0], row_d[1]))
                        conn.commit()
