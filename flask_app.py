#!flask/bin/python
from flask import Flask, jsonify
from flask import abort
from flask import make_response
from flask import request

from kafka import KafkaConsumer

from threading import Thread, Lock
import json

from misc_utils import Tree, LRU, csv_reader

# Sample data
# User_Id,USD,EUR,BTC,BCH,ETH
# 100,0,3128.39,81.807344,68.653219,136.152897
# 101,0,4752.07,58.768843,11.174564,46.886388
# 102,0,6958.4,32.038362,196.488585,574.613031
# 103,0,1788.62,3.636841,195.477764,661.278188
# 104,0,6454.23,89.810242,69.12759,108.972753
# 105,0,2927.4,55.589599,151.516239,366.233824
# 106,0,1761.76,60.689684,157.163595,270.000714


def get_disk_data(user_id):
    if user_id in user_pos_disk:
        return user_pos_disk[user_id]
    else:
        return None


## Apply LRU class aka decorator
get_disk_data_LRU = LRU(get_disk_data, maxsize=300)

def put_disk_data(user_id, user):
    user_pos_disk[user_id] = user
    get_disk_data_LRU.cache.pop((user_id,), None)  # invalidate cache for user_id
    get_disk_data_LRU(user_id)  # add user_id again
    if _debug_:
        print("new cache after update", get_disk_data_LRU.cache)

def kafka_consumer():
    print ("Start consuming Kafka msgs")

    global user_pos_disk
    global user_order_id_dic

    offset_dict = {}
    # consume json messages
    consumer = KafkaConsumer('test', value_deserializer=lambda m: json.loads(m.decode('ascii')))


    for msg in consumer:

        offset = msg.offset

        if offset in offset_dict:
            print ("Ignore offset", offset)
            continue

        offset_dict[offset] = 1
        payload = msg.value

        if _debug_:
            print(msg)
            print("offset", offset)

        settl_id  = payload["settl_id"]
        user_id = str(payload["user_id"])
        bought_token = payload["bought_token"]
        bought_quantity = payload["bought_quantity"]
        sold_token = payload["sold_token"]
        sold_quantity = payload["sold_quantity"]
        order_id = str(payload["order_id"])
        print("Parsing settlement order", settl_id, user_id, bought_token, bought_quantity, sold_token, sold_quantity, order_id)

        if order_id_dict[order_id]["settled"] == "":

            with x_lock:
                requested_bal = float(order_id_dict[order_id]["requested_bal"])

                user = get_disk_data_LRU(user_id)
                user[sold_token] = float(user[sold_token]) + requested_bal
                user[sold_token] = str(float(user[sold_token]) - float(sold_quantity))
                user[bought_token] = str(float(user[bought_token]) + float(bought_quantity))
                order_id_dict[order_id]["settled"] = settl_id

                put_disk_data(user_id, user)   ## update disk and cache
        else:
            print ("Ignore duplicate msg sett_id", settl_id, "order_id", order_id)


### Start processing

curr_dict = {}
curr_dict['USD'] = 1
curr_dict['EUR'] = 1
curr_dict['BTC'] = 1
curr_dict['BCH'] = 1
curr_dict['ETH'] = 1


user_pos_disk = Tree()
order_id_dict = Tree()
_debug_ = 0

x_lock = Lock()

input_csv_file = 'Risk Engine Test Data Set.csv'
user_pos_disk = csv_reader(input_csv_file)

i = 0
if _debug_ :
    for user_id in user_pos_disk:
        print("user_pos_disk", user_id, " == >", user_pos_disk[user_id]['user_id'])
        i += 1
        if i > 10:
            break

t = Thread(target=kafka_consumer, args=(), daemon=True)
t.start()

if t.is_alive():
    print ("Thread kafka_consumer() is running")
else:
    print("Thread kafka_consumer() completed")


app = Flask(__name__)

@app.route('/user', methods=['GET'])
def _get_users():
    return jsonify({'users': user_pos_disk})


@app.route('/users/<int:user_id>', methods=['GET'])
def _get_full_user(user_id):
    user = ""
    user_id = str(user_id)
    print ("REST users/user_id = ", user_id)

    user = get_disk_data_LRU(user_id)

    if user is None or len(user) == 0:
        abort(404)
    return jsonify({'user': user})


@app.route('/check_balance/<int:user_id>', methods=['GET'])
def check_balance(user_id):
    result = "INSUFFICIENT_BALANCE"
    user = ""
    user_id = str(user_id)
    print("REST check_balance, user_id = ", user_id)

    params = request.args.to_dict()
    print("params", params)

    if t.is_alive():
        print("Thread kafka_consumer() still running")
    else:
        print("Thread kafka_consumer() completed")

    if 'curr' in params and 'balance' in params and 'order_id' in params:

        curr = params['curr']
        if curr not in curr_dict:
            print("Wrong  param curr", curr)
            abort(404)

        requested_bal = float(params['balance'])
        order_id = params['order_id']

    else:
        print("Wrong  params")
        abort(404)

    with x_lock:
        user = get_disk_data_LRU(user_id)
        if float(user[curr]) - requested_bal >= 0:
            user[curr] = str(float(user[curr]) - requested_bal)
            put_disk_data(user_id, user)

            result = "SUFFICIENT_BALANCE"
            order_id_dict[order_id]["user_id"] = user_id
            order_id_dict[order_id]["curr"] = curr
            order_id_dict[order_id]["requested_bal"] = requested_bal
            order_id_dict[order_id]["settled"] = ""
            print ("SUFFICIENT_BALANCE", user_id, curr, requested_bal, "order_id", order_id)
            print ("order_id_dict", order_id_dict)


    if len(user) == 0:
        abort(404)

    ## return jsonify({'result': result, 'user': user})
    return jsonify({'result': result, 'user': {'user_id' : user_id}})


@app.route('/order_id', methods=['GET'])
def _get_order_id():
    return jsonify({'order_id': order_id_dict})


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(debug=True)

