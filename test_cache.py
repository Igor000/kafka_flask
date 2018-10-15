from misc_utils import LRU


if __name__ == '__main__':

    f = LRU(ord, maxsize=3)
    for c in 'ABCDCE':
        print('%s -> %s\t\t%r' % (c, f(c), f.cache))

    print ("========= removing 'C' ")
    f.cache.pop(('C',), None)          # invalidate 'C'
    print(f.cache)

    print ("========= Add  'C' bac ")
    f('C')          # add 'C' again
    print(f.cache)

    f.cache.clear()