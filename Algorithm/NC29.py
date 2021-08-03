def Find(target, inputArray):
    # get length
    print("This is target", target)
    print("This is input array", inputArray)
    i = len(inputArray)
    print(i)
    #使用二分法,先判断长度
    if i == 0:
        print("length is 0")
        return False
    elif i == 1:
        print("length is 1")
        return inputArray[0] == target
    else:
        #首先判断范围
        head = 0
        tail = i-1
        print(inputArray[head], inputArray[tail])
        if inputArray[head] > target or inputArray[tail] < target:
            print("no result")
            return False
        #两分
        while tail - head > 1:
            print("This is head and tail!!!!!!!!!!!!!", head, tail)
            if inputArray[head] == target or inputArray[tail] == target:
                print("Great!!!!!!!!!!! result")
                return True
            middle = int((tail-head)/2)
            print("This is value in middle @@@@@@@@@@@@@@@@@", inputArray[middle])
            if inputArray[middle] > target:
                print("=========,  middle > target")
                tail = middle
            if inputArray[middle] < target:
                print("=========!!!!!!!!!!!!!!!!!!!!,  middle < target")
                head = middle
            if inputArray[middle] == target:
                print("////////////////////////////////////////")
                return True
        return (inputArray[head] == target) or (inputArray[tail] == target)

# def Find2Array(target, inputArray):
#     i = len(inputArray)
#     j = len(inputArray[0])
#
#     #首先,最小的大于target或者最大的小于target,直接false
#     if inputArray[0][0] > target or inputArray[i-1][j-1] < target:
#         return False
#     head = 0
#     tail = i-1
#     middle = int((tail-head)/2)
#
#     for each in range(i):
#         print(inputArray[each][0])
#         return False

def  Find2(target2, inputArray2):
    i = len(inputArray2)
    for each in range(i):
        return Find(target2, inputArray2[each])

def Find3(target, array):
    # write code here
    i= len(array)
    j = len(array[0])
    temp = []
    for each in range(i):
        temp.extend(array[each])
    if temp.count(target):
        return True
    return False

def Find4(target, array):
    # write code here
    i= len(array)
    j = len(array[0])
    temp = []
    for each in range(i):
        temp.extend(array[each])
    if temp.count(target):
        return True
    return False
if __name__ == '__main__':
    # target = 3
    # inputArray = [1,5]
    # result = Find(target, inputArray)
    # print(result)

    target= 10
    inputArray = [[1,2,3,4],[5,6,7,8],[9,10,11,12]]
    # result = Find3(target, inputArray)
    # print(result)
    result2 = Find2(target, inputArray)
    print("Final!",result2)