import requests
import parsel
# 获取url  伪装成用户
url='https://bscscan.com/labelcloud'
headers= {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36 SE 2.X MetaSr 1.0'}

# 对页面请求 得到数据
response=requests.get(url=url , headers=headers)
html_data=response.text

#解析数据
selector=parsel.Selector(html_data)
print(selector)
# data_list=selector.xpath('').getall()
# for data in data_list:
