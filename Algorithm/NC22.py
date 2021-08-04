# def findMiddle(target, inputArray):
#     """
#     例如target = 10，inputArray = [1,2,3,11,13];那么需要找到3和11中间;然后插入
#     :param target:
#     :param inputArray:
#     :return:
#     """
#     length = len(inputArray)
#     """
#     如果array长度为0;直接返回0
#     如果array长度为1,判断和a[0]的关系
#     """
#     if length == 0:
#         return 0
#     if length == 1:
#         return 0 if inputArray[0] > target else 1
#
#     """
#     如果长度大于1,需要不断缩小毒圈
#     先判断首尾
#     """
#     head = 0
#     tail = length-1
#     # print("kaishi de head he tail!!!!!!!!!!!!", head, tail)
#     if inputArray[head] >= target:
#         return 0
#     if inputArray[tail] <= target:
#         return length
#     while tail - head > 1:
#         mid = int((tail + head)/2)
#         # print("现在中间位是", mid)
#         if inputArray[mid] <= target:
#             # print("inputArray[mid] is less than target!", inputArray[mid])
#             head = mid
#             # print("Now head是====", head)
#             # print("Now 尾巴是====", tail)
#         elif inputArray[mid] > target:
#             # print("inputArray[mid]大于目标", inputArray[mid])
#             tail = mid
#             # print("Now 尾巴是~~~~~~~", tail)
#     return mid
#
#
#
#
# def merge(A,m,B,n):
#     for each in B:
#         index = findMiddle(each, A)
#         print(index)
#         A.insert(index, each)
#     return A
#
# if __name__ == '__main__':
#     # target = 5
#     # inputArray = [1,2,3,4]
#     # res = findMiddle(target, inputArray)
#     # print(res)
#     A = [1,2,3,4,5]
#     B = [4]
#     C = merge(A,4,B,3)
#     print(C)


"""
解法2
"""

def merge(A,m,B,n):
    if not A and not B: return []
    elif not A: return B
    elif not B: return A

    #A,B都非空
    while m and n:
        if A[m-1] > B[n-1]:
            A[m+n-1] = A[n-1]
            m-=1
        elif A[m-1] < B[n-1]:
            A[m+n-1] = B[n-1]
            n-=1
        """
        结束后,有两种情况:
        m!=0,n=0,此时像,最初A=[1,2,5],B=[3,4,6]
        m=0,n!=0,此时像,最初A=[3,4,5],B=[1,2,6]
        """
        if n!= 0:
            A[:n] = B[:n]
    return A
if __name__ == '__main__':
    A = [1,3,10]
    B = [2,4,5,6,9]
    merge(A,3,B,5)