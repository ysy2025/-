"""
KMP算法参考:
https://www.ruanyifeng.com/blog/2013/05/Knuth%E2%80%93Morris%E2%80%93Pratt_algorithm.html
https://blog.csdn.net/weixin_39561100/article/details/80822208
"""

def genPnext(substring):
    """
    构造临时数组Pnext
    :param substring:
    :return:
    """
    j, m = 0, len(substring)
    # 初始化一个长度为m的0数组
    pnext = [0] * m

    i = 0
    while i < m:
        if substring[j] == substring[i]:
            pnext[i] = j + 1
            j += 1
            i += 1
        elif j != 0:
            j =
