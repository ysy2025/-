def maxProfit(prices):
    # write code here
    size = len(prices)
    a = [[0 for col in range(size)] for row in range(size)]
    print(a)
    # 动态规划
    for i in range(size):
        print(a)
        for j in range(i, size):
            if i == j:
                a[i][j] = 0
            # 如果当前价格小于买入价格,收益为0;反之计算收益
            if prices[j] < prices[i]:
                a[i][j] = 0
            else:
                profit = (prices[j] - prices[i]) / float(prices[i])
                if profit >= a[i][j - 1]:
                    a[i][j] = profit
                else:
                    a[i][j] = a[i][j - 1]
    result = []
    for each in a:
        result.extend(each)
    print(result)
    return max(result)

if __name__ == '__main__':
    input_list = [1,4,3]
    maxProfit = maxProfit(input_list)
    print(maxProfit)