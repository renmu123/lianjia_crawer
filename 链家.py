import requests
from bs4 import BeautifulSoup
import random
from datetime import datetime, date
import datetime
import re
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["lianjia"]
mycol = mydb["flat"]
mycol.create_index([('name', pymongo.ASCENDING), ('price', pymongo.ASCENDING)], unique=True, background=True)

user_agents = ['Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:15.0) Gecko/20100101 Firefox/15.0.1',
                'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/16.0.1',
                'Opera/9.80 (X11; Linux i686; U; ru) Presto/2.8.131 Version/11.11']

headers = {
    'User-Agent': random.choice(user_agents)
}

base_url = 'https://sh.lianjia.com/zufang/pg{}brp{}erp{}'

# TODO:阿虎局增量更新
# 获取当前页的所有页数


def get_pages(url):
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    total_page = soup.find(class_='content__pg').get('data-totalpage')

    return int(total_page)


def get_data(url):
    '''
    :param url:
    :return: like ['嘉定-安亭', '54㎡', '南', '1室1厅1卫', '低楼层（24层）']
    '''
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    total_data = soup.find_all(class_='content__list--item')
    for data in total_data:
        name = data.find(class_='content__list--item--title twoline').get_text().strip()
        detail = data.find(class_='content__list--item--des').get_text().replace(' ', '').replace('\n', '').split('/')
        try:
            source = data.find(class_='content__list--item--brand oneline').get_text().strip()
        except:
            source = ''
        time = data.find(class_='content__list--item--time oneline').get_text()
        time = deal_time(time)
        price = data.find(class_='content__list--item-price').find('em').get_text()
        data = {
            'name': name,
            'address': detail[0],
            'size': detail[1],
            'toward': detail[2],
            'area': detail[3],
            'floor': detail[4],
            'source': source,
            'time': time,
            'price': price
        }
        yield data


# 今天， xx天前， xx月前，xx年前
def deal_time(time_string):
    pattern = re.compile(r'[0-9]+')
    today = date.today()
    re_time = re.search(pattern, time_string)
    if re_time is not None:
        time = int(re_time.group(0))
    if time_string == '今天发布':
        return today.strftime("%Y-%m-%d")
    elif'天' in time_string:
        return (today - datetime.timedelta(days=time)).strftime("%Y-%m-%d")
    elif'月' in time_string:
        return (today - datetime.timedelta(days=time*30)).strftime("%Y-%m-%d")
    elif'年' in time_string:
        return (today - datetime.timedelta(days=time*362)).strftime("%Y-%m-%d")
    else:
        return time_string


def insert_many(data):
    for item in data:
        try:
            mycol.insert_one(item)
        except:
            print(item, '与索引重复')
            continue


# 生成页面的url
def genereate_page_url(num, total_pages, base_url):
    for page in range(1, total_pages + 1):
        url = base_url.format(page, num, num+500)
        yield url


def generate_page_url_list():
    for num in range(500, 30000, 500):
        url = base_url.format(1, num, num+500)
        total_pages = get_pages(url)
        yield list(genereate_page_url(num, total_pages, base_url))


def main():
    for url_list in generate_page_url_list():
        for url in url_list:
            data = list(get_data(url))
            # data = [set(item) for item in data]
            # print(url, data)
            # print(len(data), len(set(data)))
            insert_many(data)



def test():
    pass


if __name__ == '__main__':
    # pass
    main()
    # test()
