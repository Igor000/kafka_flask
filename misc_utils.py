
import csv
import collections

class Tree(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return value


class LRU:

    def __init__(self, func, maxsize=128):
        self.cache = collections.OrderedDict()
        self.func = func
        self.maxsize = maxsize

    def __call__(self, *args):
        cache = self.cache
        if args in cache:
            cache.move_to_end(args)
            return cache[args]
        result = self.func(*args)
        cache[args] = result
        if len(cache) > self.maxsize:
            cache.popitem(last=False)
        return result

def csv_reader(input_csv_file):

    with open(input_csv_file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        user_pos_disk = Tree()

        print("Loading data file ", input_csv_file)
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                user_id = row[0]
                usd_mv  = row[1]
                eur_mv  = row[2]
                btc_mv  = row[3]
                bch_mv  = row[4]
                eth_mv  = row[5]

                user_pos_disk[user_id]['user_id'] = user_id
                user_pos_disk[user_id]['USD'] = usd_mv
                user_pos_disk[user_id]['EUR'] = eur_mv
                user_pos_disk[user_id]['BTC'] = btc_mv
                user_pos_disk[user_id]['BCH'] = bch_mv
                user_pos_disk[user_id]['ETH'] = eth_mv

                line_count += 1
        print(f'Processed {line_count} lines.')
    return user_pos_disk