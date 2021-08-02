def twoSum(numbers , target ):
    for i in range(len(numbers)):
        print("This is i!", i)
        delta = target - numbers[i]
        if delta not in numbers:
            result = []
        else:
            print("This is numbers", numbers)
            print(delta)
            print(delta != numbers[i])
            if delta != numbers[i]:
                print("delta != numbers[i]")
                return i, numbers.index(delta, i)
            else:
                if numbers.count(delta) > 1:
                    print(delta == numbers[i])
                    return i, numbers.index(delta, i+1)
                else:
                    result = []
    return 0,0

if __name__ == '__main__':
    a = [3,2,4]
    b = 6
    result = twoSum(a, b)
    print(result)