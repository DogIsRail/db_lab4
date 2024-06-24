from datetime import datetime

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://dogisrail:dbPassword616@cluster0.abp18nm.mongodb.net/"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['lab4_db']

db.create_collection('items')
items = db['items']

# 1
print("Request 1:")
myItems = [
    {
        'category': 'Drone',
        'model': 'DJI Mavic Air 2',
        'producer': 'DJI',
        'price': 799,
        'flight_time': '34 minutes',
        'camera_resolution': '48MP',
        'video_resolution': '4K',
        'max_speed': '68.4 km/h',
        'range': '10 km'
    },
    {
        'category': 'Refrigerator',
        'model': 'LG InstaView Door-in-Door',
        'producer': 'LG',
        'price': 2199,
        'capacity': '29 cu. ft.',
        'energy_rating': 'A++',
        'features': ['Smart Cooling', 'InstaView', 'Door-in-Door'],
        'color': 'Black Stainless Steel'
    },
    {
        'category': 'Smart Home Speaker',
        'model': 'Amazon Echo (4th Gen)',
        'producer': 'Amazon',
        'price': 99,
        'assistant': 'Alexa',
        'connectivity': ['Wi-Fi', 'Bluetooth'],
        'color_options': ['Charcoal', 'Glacier White', 'Twilight Blue'],
        'dimensions': '144 x 144 x 133 mm'
    },
    {
        'category': 'Camera',
        'model': 'Nikon Z6 II',
        'producer': 'Nikon',
        'price': 1999,
        'sensor_resolution': '24.5MP',
        'video_resolution': '4K UHD',
        'iso_range': '100-51200',
        'image_stabilization': True,
        'connectivity': ['Wi-Fi', 'Bluetooth']
    },
    {
        'category': 'Microwave Oven',
        'model': 'Panasonic NN-SN966S',
        'producer': 'Panasonic',
        'price': 219,
        'capacity': '2.2 cu. ft.',
        'power': '1250W',
        'features': ['Inverter Technology', 'Sensor Cooking'],
        'color': 'Stainless Steel'
    },
    {
        'category': 'E-Reader',
        'model': 'Kindle Paperwhite',
        'producer': 'Amazon',
        'price': 129,
        'display_size': '6 inches',
        'storage': '8GB',
        'battery_life': 'Up to 6 weeks',
        'waterproof': True,
        'connectivity': ['Wi-Fi']
    }
]

items.insert_many(myItems)

# 2
print("Request 2:")
for item in items.find():
    print(item)
print()

# 3
print("Request 3:")
print(items.count_documents({'category': 'Drone'}))
print()

# 4
print("Request 4:")
print(len(items.distinct('category')))
print()

# 5
print("Request 5:")
print(items.distinct('producer'))
print()

# 6a
print("Request 6a:")
for item in items.find({'category': 'Drone', 'price': {'$gt': 100, '$lt': 1000}}):
    print(item)
print()

# 6b
print("Request 6b:")
for item in items.find({'$or': [{'model': 'DJI Mavic Air 2'}, {'model': 'Kindle Paperwhite'}]}):
    print(item)
print()

# 6c
print("Request 6c:")
for item in items.find({'producer': {'$in': ['Amazon', 'Nikon']}}):
    print(item)
print()

# 7
print("Request 7:")
items.update_one({'model': 'Kindle Paperwhite'}, {'$set': {'price': 109}})

items.update_many({'price': {'$lt': 400}}, {'$set': {'available': False}})
items.update_many({'price': {'$gt': 400}}, {'$set': {'available': True}})

for item in items.find():
    print(item)
print()

# 8
print("Request 8:")
for item in items.find({'power': {'$exists': True}}):
    print(item)
print()

# 9
print("Request 9:")
items.update_many({'power': {'$exists': True}}, {'$inc': {'price': 200}})

for item in items.find({'power': {'$exists': True}}):
    print(item)
print()

# 1
print("Request 1:")
db.create_collection('orders')
orders = db['orders']


def get_item_id(model):
    item = items.find_one({'model': model}, {'_id': 1})
    return item['_id'] if item else None


models = [
    'DJI Mavic Air 2',
    'LG InstaView Door-in-Door',
    'Amazon Echo (4th Gen)',
    'Nikon Z6 II',
    'Panasonic NN-SN966S',
    'Kindle Paperwhite'
]

itemsID = [get_item_id(model) for model in models]

models = [
    'DJI Mavic Air 2',
    'LG InstaView Door-in-Door',
    'Amazon Echo (4th Gen)',
    'Nikon Z6 II',
    'Panasonic NN-SN966S',
    'Kindle Paperwhite'
]

myOrders = [
    {
        'order_number': 201519,
        'date': datetime(2024, 5, 30),
        'total_sum': 2599,
        'customer': {
            'name': 'Dmytro',
            'surname': 'Ivanov',
            'phones': [9876543, 1234567],
            'address': 'Shevchenka 12, Kyiv, UA'
        },
        'payment': {
            'card_owner': 'Dmytro Ivanov',
            'cardId': 12345678
        },
        'items_id': [itemsID[0], itemsID[1]]
    },
    {
        'order_number': 201520,
        'date': datetime(2024, 6, 15),
        'total_sum': 899,
        'customer': {
            'name': 'Olena',
            'surname': 'Petrenko',
            'phones': [9876543],
            'address': 'Hrushevskoho 25, Lviv, UA'
        },
        'payment': {
            'card_owner': 'Olena Petrenko',
            'cardId': 87654321
        },
        'items_id': [itemsID[2], itemsID[5]]
    },
    {
        'order_number': 201521,
        'date': datetime(2024, 7, 16),
        'total_sum': 2199,
        'customer': {
            'name': 'Ivan',
            'surname': 'Symonenko',
            'phones': [9876543, 5555555],
            'address': 'Svobody 7, Odesa, UA'
        },
        'payment': {
            'card_owner': 'Ivan Symonenko',
            'cardId': 98765432
        },
        'items_id': [itemsID[1]]
    },
    {
        'order_number': 201522,
        'date': datetime(2024, 8, 18),
        'total_sum': 1999,
        'customer': {
            'name': 'Yulia',
            'surname': 'Bondarenko',
            'phones': [9876543],
            'address': 'Kharkivska 15, Kharkiv, UA'
        },
        'payment': {
            'card_owner': 'Yulia Bondarenko',
            'cardId': 13579246
        },
        'items_id': [itemsID[3], itemsID[4]]
    },
    {
        'order_number': 201523,
        'date': datetime(2024, 9, 19),
        'total_sum': 799,
        'customer': {
            'name': 'Mykola',
            'surname': 'Shevchenko',
            'phones': [9876543, 8888888],
            'address': 'Poltavska 9, Zaporizhzhia, UA'
        },
        'payment': {
            'card_owner': 'Mykola Shevchenko',
            'cardId': 24681357
        },
        'items_id': [itemsID[0], itemsID[2]]
    }
]

orders.insert_many(myOrders)

# 2
print("Request 2:")
for order in orders.find():
    print(order)
print()

# 3
print("Request 3:")
for order in orders.find({'total_sum': {'$gt': 300}}):
    print(order)
print()

# 4
print("Request 4:")
for order in orders.find({'customer.name': 'Mykola', 'customer.surname': 'Shevchenko'}):
    print(order)
print()

# 5
print("Request 5:")
for order in orders.find({'items_id': itemsID[0]}):
    print(order)
print()

# 6
print("Request 6:")
orders.update_many({'items_id': itemsID[0]}, {'$push': {'items_id': itemsID[1]}, '$inc': {'total_sum': 400}})

for order in orders.find({'items_id': itemsID[0]}):
    print(order)
print()

# 7
print("Request 7:")
print(len(orders.find_one({'order_number': 201522})['items_id']))

# 8
print("Request 8:")
for order in orders.find({'total_sum': {'$gt': 500}}, {'_id': 0, 'customer': 1, 'payment.cardId': 1}):
    print(order)
print()

# 9
print("Request 9:")
orders.update_many({'date': {'$gte': datetime(2024, 5, 1), '$lte': datetime(2024, 8, 1)}},
                   {'$pull': {'items_id': itemsID[0]}})

for order in orders.find({'date': {'$gte': datetime(2024, 5, 1), '$lte': datetime(2024, 8, 1)}}):
    print(order)
print()

# 10
print("Request 10:")
orders.update_many({}, {'$set': {'customer.name': 'Ivan'}})

for order in orders.find():
    print(order)
print()


# 11
print("Request 11:")
pipeline = [
    {'$match': {'customer.surname': 'Shevchenko'}},

    {'$lookup': {'from': 'items', 'localField': 'items_id', 'foreignField': '_id', 'as': 'items_in_order'}},

    {'$project': {'_id': 0, 'customer': 1, 'items_in_order.model': 1, 'items_in_order.price': 1}}
]
for order in orders.aggregate(pipeline):
    print(order)
print()

# 1
print("Request 1:")
db.create_collection('last_reviews', capped=True, max=5, size=10000)
last_reviews = db['last_reviews']

myReviews = [
    {
        'name': 'user1',
        'text': 'Good',
        'date': datetime(2024, 1, 1)
    },
    {
        'name': 'user2',
        'text': 'Bad',
        'date': datetime(2024, 2, 2)
    },
    {
        'name': 'user3',
        'text': 'Not bad',
        'date': datetime(2024, 3, 3)
    },
    {
        'name': 'user4',
        'text': 'So-so',
        'date': datetime(2024, 4, 4)
    },
    {
        'name': 'user5',
        'text': 'Nice',
        'date': datetime(2024, 5, 5)
    }
]

last_reviews.insert_many(myReviews)

for review in last_reviews.find():
    print(review)
print()

new_review = {
    'name': 'user6',
    'text': 'Well',
    'date': datetime(2024, 6, 6)
}

last_reviews.insert_one(new_review)

for review in last_reviews.find():
    print(review)
print()
