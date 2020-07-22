#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#   Author  :   vzam
#   Date    :   21.9.2018
#   Desc    :   base hh.ru spider

#https://tyumen.hh.ru/search/vacancy?area=95&specialization=1
#autopager.urls(requests.get('https://tyumen.hh.ru/search/vacancy?area=95&specialization=1&from=cluster_professionalArea))

import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import random
from time import sleep

from peewee import *

#from multiprocessing import Pool
#from random import choice
#from random import uniform
from time import strftime   # Load just the strftime Module from Time
import logging


global file_log
file_name_log = str(strftime("%Y-%m-%d") + "_from_hh_" + ".log")
folder_out_linux = '/var/log/glory/out'
file_log = str(os.path.join(folder_out_linux, file_name_log))


global DOMEN
global BASE_URL
global FILE_USER_AGENT

#global 

DOMEN = 'tyumen.hh.ru'

BASE_URL = 'https://tyumen.hh.ru/search/vacancy?area=95'

#BASE_URL = 'https://tyumen.hh.ru/search/vacancy?area=95&specialization=1'


# BASE_URL = 'https://tyumen.hh.ru/search/vacancy?area=95&specialization=1'
# BASE_URL_LIST = { 'https://tyumen.hh.ru/search/vacancy?area=95&specialization=1',
#     ''
#
# }


FILE_USER_AGENT = 'useragents.txt'
USER_AGENT = 'Mozilla/5.0 (Windows; U; Win 9x 4.90; de-DE; rv:0.9.4) Gecko/20011019 Netscape6/6.2'

db = SqliteDatabase('hh_tyumen.db')


# Model for our entry table
class Person(Model):
    uid = CharField(max_length=255, default="")
    fio = CharField(max_length=255, default="")
    tel1 = CharField(max_length=255, default="")
    tel2 = CharField(max_length=255, default="")
    tel2 = CharField(max_length=255, default="")
    email = CharField(max_length=255, default="")
    vac = CharField(max_length=255, default="")
    city = CharField(max_length=255, default="Tyumen")
    addr = TextField(default="")
    company = CharField(max_length=255, default="")
    html_url = CharField(max_length=255, default="")
    employer_url = CharField(max_length=255, default="")

    create_time = DateTimeField(default=datetime.now)
    update_time = DateTimeField(default=datetime.now)


    class Meta:
        database = db
        indexes = (
            # create a unique on ...
            (('company', 'html_url'), True),)

def add_to_file(file_name, ss):
    with open(file_name, 'a') as f:
        if not ss.count('\n'):
            ss = ss + '\n'
        f.write(ss)


def get_list_from_file(file):
    lst = []
    with open(file, "r") as f: #, errors='ignore'
        lst = f.read().splitlines()
        if len(lst) > 2:
            lst[-1] = lst[-1].strip()  # удаляем последний \n
            # del lst[-1]
        f.close()
    return lst


def get_next_user_agent():
    global USER_AGENT
    ua_list = get_list_from_file(FILE_USER_AGENT)
    USER_AGENT = random.choice(ua_list)
    USER_AGENT.replace("\'", "\"")
    #print('new user agent is: ' + USER_AGENT)
    return USER_AGENT


def save_html(url, file_name):
    get_next_user_agent()  # new USER_AGENT
    useragent = {'User-Agent': USER_AGENT}
    #{'User-Agent': 'Mozilla/5.0 (Windows; U; Win 9x 4.90; de-DE; rv:0.9.4) Gecko/20011019 Netscape6/6.2'}
    if not len(str(file_name)): file_name = 'test.html'
    # open in binary mode
    with open(file_name, "wb") as f:  # ,
        try:
            # r = requests.get(url)  #r.encoding = 'utf-8'
            r = requests.get(url, headers=useragent)
            f.write(r.content)
            f.close()
            r.raise_for_status()
        except requests.exceptions.ReadTimeout:
            print('Oops. Read timeout occured')
        except requests.exceptions.ConnectTimeout:
            print('Oops. Connection timeout occured!')
        except requests.exceptions.ConnectionError:
            print('Seems like dns lookup failed..')
        except requests.exceptions.HTTPError as err:
            print('Oops. HTTP Error occured')
            print('Response is: {content}'.format(content=err.response.content))
    return r.text

def get_html(url):

    get_next_user_agent()  # new USER_AGENT

    useragent = {'User-Agent': USER_AGENT}
    print(useragent)
    
    #{'User-Agent': 'Mozilla/5.0 (Windows; U; Win 9x 4.90; de-DE; rv:0.9.4) Gecko/20011019 Netscape6/6.2'}
    try:
        # r = requests.get(url)  #r.encoding = 'utf-8'
        r = requests.get(url, headers=useragent)
        return r.text
    except requests.exceptions.ReadTimeout:
        print('Oops. Read timeout occured')
    except requests.exceptions.ConnectTimeout:
        print('Oops. Connection timeout occured!')
    except requests.exceptions.ConnectionError:
        print('Seems like dns lookup failed..')
    except requests.exceptions.HTTPError as err:
        print('Oops. HTTP Error occured')
        print('Response is: {content}'.format(content=err.response.content))
    
    return ''


def get_last_count(url):
    # For testing BS4
    # import requests
    # from bs4 import BeautifulSoup
    # useragent = {'User-Agent': "Mozilla/5.0 (Windows; U; Win 9x 4.90; de-DE; rv:0.9.4) Gecko/20011019 Netscape6/6.2"}
    # html = requests.get("https://tyumen.hh.ru/search/vacancy?area=95&specialization=1", headers=useragent).text
    # soup = BeautifulSoup(html, 'lxml')
    # tt = soup.findAll('a',class_='bloko-button HH-Pager-Control')
    # tt = soup.findAll('a',class_='bloko-link HH-LinkModifier')

    #get_next_user_agent() # new USER_AGENT
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    tt = soup.findAll('a',class_='bloko-button HH-Pager-Control')
    last_val = tt[-1].text
    print ("Надено: " + str(last_val))
    return int(last_val)


def get_all_links_vacancies(url):

    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    tt = soup.findAll('a', class_='bloko-link HH-LinkModifier')
    for item in tt:
        qq = item.get('href')
        iid = str(qq).split("/")[-1]
        vac = item.text
        print(qq)
        hh_person = Person()
        try:
            #hh_person = Person.create(html_url=qq)
            hh_person.uid = iid
            hh_person.vac = vac
            hh_person.html_url = qq
            hh_person.save()
        except IntegrityError:
            Person.get(Person.uid == iid)
            print("UID уже есть: " + iid)



def get_all_pages_vacancies():
    global LIST_USER_AGENT
    # print(os.getcwd())

    LIST_USER_AGENT = get_list_from_file(FILE_USER_AGENT)
    print(' User Agents found: ' + str(len(LIST_USER_AGENT)))

    last_page = get_last_count(BASE_URL)

    get_all_links_vacancies(BASE_URL)

    for ii in range(1, last_page):
        url = BASE_URL + "&page=" + str(ii)
        print("Обрабатываем страницу " + str(ii) + " из " + str(last_page))
        print(url)
        t = random.uniform(1,8)
        sleep(t)
        print('Sleeping for ' + str(t) + ' seconds')
        get_all_links_vacancies(url)


def get_vacancies_info():

    ii = 0
    cnt = len(Person.select())

    for person in Person.select():

        print(person.html_url)
        print(str(ii) + " из " + str(cnt))

        get_vac_info(person.html_url)

        t = random.uniform(1,8)
        sleep(t)
        print('Sleeping for ' + str(t) + ' seconds')
        ii = ii + 1


def get_vac_info(url):
    # import requests
    # from bs4 import BeautifulSoup
    # useragent = {'User-Agent': "Mozilla/5.0 (Windows; U; Win 9x 4.90; de-DE; rv:0.9.4) Gecko/20011019 Netscape6/6.2"}
    # html = requests.get("https://tyumen.hh.ru/vacancy/27682784", headers=useragent).text
    # soup = BeautifulSoup(html, 'lxml')
    # tt = soup.findAll('a',class_='bloko-button HH-Pager-Control')
    # tt = soup.findAll('a',class_='bloko-link HH-LinkModifier')


    uid = str(url).split("/")[-1]
    person = Person.get(Person.uid == uid)
    print(person.vac)
    logging.info(person.vac)
    html = get_html(url)
    soup = BeautifulSoup(html, 'lxml')

    try:
        tt = soup.findAll('span', itemprop="name")
        company = tt[0].text
        person.company = company
    except:
        logging.info("Компания не найдена " + url + " !")

    try:
        tt = soup.findAll(attrs={"data-qa": "vacancy-view-raw-address"})
        addr = tt[0].text
        person.addr = addr
    except:
        logging.info("Адрес не найден " + url + " !")

    try:
        tt = soup.findAll(attrs={"data-qa": "vacancy-contacts__fio"})
        fio = tt[0].text
        person.fio = fio
    except:
        logging.info("ФИО не найдена " + url + " !")
    try:
        tt = soup.findAll(attrs={"data-qa": "vacancy-contacts__phone"})
        tel = tt[0].text
        person.tel1 = tel
    except:
        logging.info("ТЕЛ не найден " + url + " !")

    try:
        tt = soup.findAll(attrs={"data-qa": "vacancy-contacts__email"})
        email = tt[0].text
        person.email = email
        logging.info(email)

    except:
        logging.info("EMAIL не найден " + url + " !")

    person.save()




def Main():

    print('Start!!!')
    time1 = datetime.now()
    print(time1)


    for handler in logging.root.handlers[:]:  # Remove all handlers associated with the root logger object.
        logging.root.removeHandler(handler)
    logging.basicConfig(filename=file_log, format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG,
                        filemode='w')  #
    logging.info(file_log)
    logging.info(time1)

    # Creating SQLIte DB

    db.connect()
    db.create_tables([Person], safe=True)

    # Starting this first - collect all records like  "https://tyumen.hh.ru/vacancy/27539275"
    get_all_pages_vacancies()

    get_vacancies_info()

    #get_vac_info("https://tyumen.hh.ru/vacancy/25288624")




    time2 = datetime.now()
    logging.info(time2)
    print(time2)
    print(time2-time1)
    logging.info(str(time2-time1))
    print('DONE !!!!')

if __name__ == '__main__':
    Main()
