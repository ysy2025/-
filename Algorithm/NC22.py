def findMiddle(target, inputArray):
    """
    例如target = 10，inputArray = [1,2,3,11,13];那么需要找到3和11中间;然后插入
    :param target:
    :param inputArray:
    :return:
    """
    length = len(inputArray)
    """
    如果array长度为0;直接返回0
    如果array长度为1,判断和a[0]的关系
    """
    if length == 0:
        return 0
    if length == 1:
        return 0 if inputArray[0] > target else 1

    """
    如果长度大于1,需要不断缩小毒圈
    先判断首尾
    """
    head = 0
    tail = length-1
    if inputArray[head] > target:
        return 0
    if inputArray[tail] < target:
        return length-1
    while tail - head > 1:
        mid = (tail - head)/2



def merge(A,m,B,n):
    pass

if __name__ == '__main__':
    target = 10
    inputArray = [1,2,5,7,10,11]
    res = find(target, inputArray)
    print(res)