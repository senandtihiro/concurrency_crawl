# asyncio爬虫，去重，入库

'''
pip install aiohttp
pip install aiomysql
pip install pyquery

aiohttp官网：https://aiohttp.readthedocs.io/en/stable/
其分为client和server
我们是作为client来使用的
可以直接查看它的quickstart开始编码

'''

import asyncio
import re
import aiohttp
import aiomysql
from pyquery import PyQuery

start_url = 'http://www.jobbole.com/'
# 待爬取url
waiting_urls = []
# 已经爬取过的url
seen_urls = set()

stopping = False

# 定义一个协程，用来请求url
async def fetch(url, session):
    # 由于get一个url是耗费网络IO的过程，所以前面要加async
    try:
        async with session.get('http://httpbin.org/get') as resp:
            print('url status:', resp.status)
            if resp.status in [200, 201]:
                # 拿到值的时候要用await ？
                data = await resp.text()
                return data
    except Exception as e:
        print(e)


def extract_urls(html):
    urls = []
    pq = PyQuery(html)
    for link in pq.items('a'):
        url = link.attr('href')
        if url and url.startswith('http') and url not in seen_urls:
            urls.append(url)
            waiting_urls.append(url)
    return urls


async def fetch_all_urls(url, session):
    '''
    第一步，从首页获取所有文章详情页的url
    首先要把首页爬下来，解析html，拿到所有的文章详情页的url
    :return:
    '''
    html = await fetch(url, session)
    seen_urls.add(url)
    urls = extract_urls(html)


async def article_handler(url, session, pool):
    '''
    获取文章详情并解析入库
    :param url:
    :param session:
    :return:
    '''
    html = await fetch(start_url, session)
    extract_urls(html)
    seen_urls.add(url)
    pq = PyQuery(html)
    title = pq('title').text()

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # 新建数据库，新建表
            insert_sql = "insert into article_test(title) values ('{}')".format(title)
            await cur.execute(insert_sql)
            print(cur.description)
            (r,) = await cur.fetchone()
            assert r == 42
    pool.close()
    await pool.wait_closed()


async def consumer(pool):
    '''
    从waiting_urls中不停地拿到url,启动协程抓取数据，把协程扔到事件循环中
    :return:
    '''
    async with aiohttp.ClientSession() as session:
        while not stopping:
            if len(waiting_urls) == 0:
                await asyncio.sleep(0.5)
                continue

            url = waiting_urls.pop()
            print('start get url:{}'.format(url))
            if re.match('http://.*?jobbole.com/\d+/', url):
                if url not in seen_urls:
                    # article_handler(url, session)
                    asyncio.ensure_future(article_handler(url, session, pool))
                    # 为了防止反爬，这里等待一段时间，让请求不那么频繁
                    await asyncio.sleep(1)
                # else:
                #     if url not in seen_urls:
                #         asyncio.ensure_future(fetch_all_urls(url, session))


async def main(loop):
    # 等待db连接好
    pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='Hyl805051',
                                      db='aiomysql_test', loop=loop,
                                      charset='urt8', autocommit=True)
    # asyncio.ensure_future(fetch_all_urls(start_url, session))

    async with aiohttp.ClientSession() as session:
        html = await fetch(start_url, session)
        seen_urls.add(start_url)
        urls = extract_urls(html)
    asyncio.ensure_future(consumer(pool))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.ensure_future(main(loop))
    loop.run_forever()
