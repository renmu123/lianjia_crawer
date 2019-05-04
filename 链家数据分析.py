import pymongo
import csv
import pandas as pd

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["lianjia"]
collection = db["house"]


def write_to_csv(data):
    headers = collection.find_one().keys()
    with open('output.csv', 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for item in get_data():
            writer.writerow(item)


def get_data():
    data = collection.find()
    # print(data)
    for item in data:
        yield item


def main():
    df = pd.read_csv('output.csv')
    # print(df.groupby(['time', 'price']))
    for item, value in df.groupby(['price', 'area']):
        # from_0_1000 = value[value['price'] < 1000 & value['price'] > 0]['price']
        print(item, value['price'].count())
    # for item, value in df.groupby(['time']):
    #     print(item, value['price'].count(), value[value['price'] < 6000]['price'].mean())


def test():
    data = collection.find({'name': '泰安公寓 2室2厅 1400元'})
    for item in data:
        print(item)


if __name__ == '__main__':
    # main()
    test()
