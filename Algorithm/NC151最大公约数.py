def gcd(a,b):
    print("============This is a: ", a)
    print("------------This is b: ", b)
    delta = a % b
    print("!!!!!!!!!!!!This is delta: ", delta)
    a, b = b, delta
    print("~~~here is a, and b: ", a, b)
    while delta != 0:
        return gcd(a, delta)
    return a

if __name__ == '__main__':
    a = 107
    b = 63
    print(gcd(a,b))

    """
    辗转相除,关键是递归
    https://baike.baidu.com/item/%E6%AC%A7%E5%87%A0%E9%87%8C%E5%BE%97%E7%AE%97%E6%B3%95/1647675?fromtitle=%E8%BE%97%E8%BD%AC%E7%9B%B8%E9%99%A4%E6%B3%95&fromid=4625352&fr=aladdin
    """