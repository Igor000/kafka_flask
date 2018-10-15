import json
import requests
from kafka import KafkaProducer

def run_all_tests():

    ## Test starting positions for user_id = 100

    r1 = requests.get('http://localhost:5000/users/100')
    ## print(r1.status_code, r1.text, r1.json())
    print(r1.status_code, r1.json())

    assert(r1.status_code == 200)

    result = r1.json()['user']
    assert (result['user_id'] == '100')
    assert (result['BCH'] == '68.653219')
    assert (result['BTC'] == '81.807344')
    assert (result['ETH'] == '136.152897')
    assert (result['EUR'] == '3128.39')
    assert (result['USD'] == '0')


    ## Test insufficient balance for user_id = 100 and USD
    r2= requests.get('http://localhost:5000/check_balance/100?curr=USD&balance=250&order_id=1')
    ## print (r2.status_code, r2.text, r2.json())
    print(r2.status_code, r2.json())

    assert (r2.status_code == 200)

    result2 = r2.json()['user']
    assert (result2['user_id'] == '100')
    assert (r2.json()['result'] == "INSUFFICIENT_BALANCE")

    ## Test sufficient balance for user_id = 100 and BTC
    r3 = requests.get('http://localhost:5000/check_balance/100?curr=BTC&balance=7&order_id=2')
    ## print (r3.status_code, r3.text, r3.json())
    print(r3.status_code, r3.json())

    assert (r3.status_code == 200)

    result3 = r3.json()['user']
    assert (result3['user_id'] == '100')
    assert (r3.json()['result'] == "SUFFICIENT_BALANCE")


    ## Send a settlement order using Kafka Producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))

    print ("Start producing msgs")

    my_settlemnet_dict = {}

    my_settlemnet_dict[1] = {"settl_id": 1, "user_id": 100, "bought_token": "ETH", "bought_quantity": 87.35,
                             "sold_token": "BTC", "sold_quantity": 6.9, "order_id":2}


    for id in my_settlemnet_dict :
        print (id, my_settlemnet_dict[id])
        producer.send('test', my_settlemnet_dict[id])

        producer.flush()

    ## Test all positions for user_id = 100 after receiving a settlement order in Kafka consumer
    r4 = requests.get('http://localhost:5000/users/100')
    print (r4.status_code, r4.json())

    assert(r4.status_code == 200)

    result = r4.json()['user']
    assert (result['user_id'] == '100')
    assert (result['BCH'] == '68.653219')
    assert (result['BTC'] == '74.907344')
    assert (result['ETH'] == '223.502897')
    assert (result['EUR'] == '3128.39')
    assert (result['USD'] == '0')


    ## Test missing user_id = 11
    r5 = requests.get('http://localhost:5000/users/11')
    print (r5.status_code, r5.json())

    assert(r5.status_code == 404)   # No such user_id

    ## Test existing user_id 101
    r6 = requests.get('http://localhost:5000/users/101')
    print(r6.status_code, r6.json())

    assert(r6.status_code == 200)   # existing user_id

    result = r6.json()['user']
    assert (result['user_id'] == '101')
    assert (result['BCH'] == '11.174564')
    assert (result['BTC'] == '58.768843')
    assert (result['ETH'] == '46.886388')
    assert (result['EUR'] == '4752.07')
    assert (result['USD'] == '0')

    ## Test insufficient balance for user_id = 101 and EUR
    r7= requests.get('http://localhost:5000/check_balance/101?curr=EUR&balance=100000&order_id=3')
    print(r7.status_code, r7.json())

    assert (r7.status_code == 200)

    result7 = r7.json()['user']
    assert (result7['user_id'] == '101')
    assert (r7.json()['result'] == "INSUFFICIENT_BALANCE")


    ## Test sufficient balance for user_id = 101 and EUR
    r8 = requests.get('http://localhost:5000/check_balance/101?curr=EUR&balance=20&order_id=4')
    print(r8.status_code, r8.json())

    assert (r8.status_code == 200)

    result8 = r8.json()['user']
    assert (result8['user_id'] == '101')
    assert (r8.json()['result'] == "SUFFICIENT_BALANCE")

    ## Test (result8['EUR'] == '4732.07')   ## -20 EUR
    r8_1 = requests.get('http://localhost:5000/users/101')
    print(r8_1.status_code, r8_1.json())

    assert(r8_1.status_code == 200)

    result = r8_1.json()['user']
    assert (result['user_id'] == '101')
    assert (result['EUR'] == '4732.07')


    print ("Send settl order for 10 EUR (20 was reserved)")

    my_settlemnet_dict = {}

    my_settlemnet_dict[2] = {"settl_id": 2, "user_id": 101, "bought_token": "BTC", "bought_quantity": 1.00,
                             "sold_token": "EUR", "sold_quantity": 10.0, "order_id":4}


    for id in my_settlemnet_dict :
        print (id, my_settlemnet_dict[id])
        producer.send('test', my_settlemnet_dict[id])

        producer.flush()

    ## Test existing user_id 101 after the settl ordeer
    r9 = requests.get('http://localhost:5000/users/101')
    print(r9.status_code, r9.json())

    assert(r9.status_code == 200)   # existing user_id

    result = r9.json()['user']
    assert (result['user_id'] == '101')
    assert (result['BCH'] == '11.174564')
    assert (result['BTC'] == '59.768843')
    assert (result['ETH'] == '46.886388')
    assert (result['EUR'] == '4742.07')
    assert (result['USD'] == '0')


    ## Another 20 EUR check for 101
    ## Test sufficient balance for user_id = 101 and EUR
    r10 = requests.get('http://localhost:5000/check_balance/101?curr=EUR&balance=20&order_id=5')
    print(r10.status_code, r10.json())

    assert (r10.status_code == 200)

    result10 = r10.json()['user']
    assert (result10['user_id'] == '101')
    assert (r10.json()['result'] == "SUFFICIENT_BALANCE")

    # Test (result8['EUR'] == '4722.07')   ## -20 EUR
    r10_1 = requests.get('http://localhost:5000/users/101')
    print(r10_1.status_code, r10_1.json())

    assert(r10_1.status_code == 200)   # existing user_id

    result = r10_1.json()['user']
    assert (result['user_id'] == '101')
    assert (result['BCH'] == '11.174564')
    assert (result['BTC'] == '59.768843')
    assert (result['ETH'] == '46.886388')
    assert (result['EUR'] == '4722.07')
    assert (result['USD'] == '0')

    print ("Send settl order for 10 EUR (20 was reserved)")

    my_settlemnet_dict = {}

    my_settlemnet_dict[2] = {"settl_id": 3, "user_id": 101, "bought_token": "BTC", "bought_quantity": 1.00,
                             "sold_token": "EUR", "sold_quantity": 10.0, "order_id":5}


    for id in my_settlemnet_dict :
        print (id, my_settlemnet_dict[id])
        producer.send('test', my_settlemnet_dict[id])

        producer.flush()

    ## Test existing user_id 101 after the settl ordeer
    r11 = requests.get('http://localhost:5000/users/101')
    print(r11.status_code, r11.json())

    assert(r11.status_code == 200)   # existing user_id

    result = r11.json()['user']
    assert (result['user_id'] == '101')
    assert (result['BCH'] == '11.174564')
    assert (result['BTC'] == '60.768843')
    assert (result['ETH'] == '46.886388')
    assert (result['EUR'] == '4732.07')
    assert (result['USD'] == '0')


    ## Test wrong parameters for user_id = 102 and curr = xxx
    r12 = requests.get('http://localhost:5000/check_balance/102?curr=XXX&balance=20&order_id=6')
    print(r12.status_code, r12.json())

    assert (r12.status_code == 404)

if __name__ == '__main__':

    run_all_tests()


