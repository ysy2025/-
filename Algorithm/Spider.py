import requests
import re
import os

# def geturl(content):
#     js = Py4Js()
#     tk = js.getTk(content)
#     url_result = "http://translate.google.cn/translate_a/single?client=t" \
#           "&sl=EN&tl=zh-CN&hl=zh-CNdt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca" \
#           "&dt=rw&dt=rm&dt=ss&dt=t&ie=UTF-8&oe=UTF-8&clearbtn=1&otf=1&pc=1" \
#           "&srcrom=0&ssel=0&tsel=0&kc=2&tk=%s&q=%s" % (tk, content)
#     return url_result

def getHTMLText(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}
        r = requests.get(url, headers = headers)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ''

def getUrlLinks(target, content):
    lines = content.split('\n')
    potential_links = [i.split('"')[3] for i in lines if (target in i and "href" in i and "No." in i)]
    return potential_links

def getUrlPages(content):
    lines = content.split('\n')
    potential_links = []
    for each in lines:
        if "下一页" not in each:
            pass
        else:
            each_list = each.split('href="')
            each_url = [i.split('"')[0] for i in each_list]
            potential_links += each_url[1:-1]
    return potential_links

def getPicLinks(target, content):
    lines = content.split('\n')
    links = [i.split('"') for i in lines if (target in i and i.startswith("<img src=") and "No." in i)]
    potential_links = [i for i in links[0] if i.startswith("https:")]
    return potential_links

def mkdir(path):
    path = path.strip()# 去除首位空格
    path = path.rstrip("\\") # 去除尾部 \ 符号
    isExists = os.path.exists(path)  # 判断路径是否存在  # 存在 True # 不存在   False
    if not isExists:  # 判断结果
        os.makedirs(path)# 如果不存在则创建目录 # 创建目录操作函数
        return True#print (path + ' 创建成功')
    else: # 如果目录存在则不创建，并提示目录已存在
        print(path + ' 目录已存在')
    return False

def downloadimage(url, path):
    ##下载大图和带水印的高质量大图
    r = requests.get(url)
    print(r.status_code)
    if(r.status_code!=200):
        r=requests.get(url)
        with open(path, 'wb') as f:
            f.write(r.content)
            print("下载成功")

if __name__ == '__main__':
    url = "https://www.tujigu.com/t/437/"
    content = getHTMLText(url)
    target = "就是阿朱啊"
    #获取到了全部链接
    potential_links = getUrlLinks(target, content)
    print(potential_links)
    #针对每个获取进一步的图
    """
    首先,获取链接,然后获取网页数量
    每个链接,都是有下一页,下一页,因此需要构建多个网页
    """
    for sublink in potential_links[:1]:
        content_sublink = getHTMLText(sublink)
        potential_sub_links = getUrlPages(content_sublink)
        """
        获取了网页链接后,可以获取图片链接了
        """
        for each_link in potential_sub_links[:1]:
            print(each_link)
            each_link_text = each_link.split("/")[-1].split(".")[0]
            number = 0 if each_link_text == '' else int(each_link_text)
            print(number)
            #准备工作完毕,开始解析"https://www.tujigu.com/a/44656/"网页
            each_link_content = getHTMLText(each_link)
            each_link_content_links = getPicLinks(target, each_link_content)
            print(each_link_content_links)
            print(len(each_link_content_links))
