import argparse
import asyncio
import csv
import os
import sys
import traceback
from itertools import cycle
from random import choice

import httpx
from pydash import get as _
from selectolax.parser import HTMLParser
import pandas as pd

scraped = set()

maxInt = sys.maxsize

while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 10)


def get_text(selector: str, soup: HTMLParser, separator: str = ' '):
    try:
        t = (soup.css_first(selector) if selector else soup).text(
            separator=separator, strip=True)
        return normalize(t)
    except:
        return ''


def get_attr(selector: str, attr: str, soup: HTMLParser):
    try:
        return (soup.css_first(selector) if selector else soup).attributes.get(
            attr, '').replace('\xa0', ' ').strip()
    except:
        return ''


def file2list(filename):
    if os.path.isfile(filename):
        with open(filename, encoding='utf-8') as f:
            return [x.strip() for x in f.read().splitlines() if x.strip()]
    return []


USER_AGENTS = file2list('./user_agents.txt') or [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:104.0) Gecko/20100101 Firefox/104.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15'
]

proxies = cycle(set(file2list('./proxies.txt')))


def normalize(s: str) -> str:
    if not s:
        return ''
    for c in ['\n', '\r', '\t', '\xa0']:
        s = s.replace(c, ' ')
    while '  ' in s:
        s = s.replace('  ', ' ')
    return s.strip()


async def fetch(url, headers=None, method='GET', data=None):
    try:
        proxy = next(proxies, None)
        async with httpx.AsyncClient(timeout=30,
                                     headers=headers
                                     or {'User-Agent': choice(USER_AGENTS)},
                                     follow_redirects=True,
                                     verify=False,
                                     proxies={
                                         'http://': f'http://{proxy}',
                                         'https://': f'http://{proxy}'
                                     } if proxy else None) as client:
            if method == 'GET':
                r = await client.get(url)
            elif method == 'POST' and data:
                r = await client.post(url, json=data)
            else:
                raise Exception('Invalid method')
            if r.status_code == 200:
                return r
    except Exception as e:
        print(e)
        return None


async def bs(url, headers=None):
    r = await fetch(url, headers)
    return HTMLParser(r.text)


async def scrape_profile(profile_url, writer, f):
    async with sem:
        print(f'Scraping {profile_url}')
        idz = profile_url.split('/')[-1]
        json_data = {
            'operationName':
            'GET_PROMOTER',
            'variables': {
                'id': idz,
            },
            'query':
            'query GET_PROMOTER($id: ID!) {\n  promoter(id: $id) {\n    id\n    name\n    contentUrl\n    followerCount\n    isFollowing\n    website\n    facebook\n    twitter\n    instagram\n    youtube\n    email\n    blurb\n    logoUrl\n    socialMediaLinks {\n      id\n      link\n      platform\n      __typename\n    }\n    area {\n      id\n      name\n      urlName\n      country {\n        id\n        name\n        urlCode\n        __typename\n      }\n      __typename\n    }\n    tracking(types: [PAGEVIEW]) {\n      id\n      code\n      event\n      __typename\n    }\n    __typename\n  }\n}\n',
        }
        headers = {
            'authority': 'ra.co',
            'accept': '*/*',
            'origin': 'https://ra.co',
            'ra-content-language': 'en',
            'referer': f'https://ra.co/promoters/{idz}',
            'sec-ch-device-memory': '8',
            'sec-fetch-site': 'same-origin',
            'user-agent': choice(USER_AGENTS),
        }

        r = await fetch('https://ra.co/graphql', headers, 'POST', json_data)
        if r:
            js = _(r.json(), 'data.promoter')
            data = {
                'Promoter': _(js, 'name'),
                'About': _(js, 'blurb'),
                'Region': _(js, 'area.name'),
                'Website': _(js, 'website') or profile_url,
                'URL': profile_url,
                'Facebook': _(js, 'facebook'),
                'Instagram': _(js, 'instagram'),
                'E-Mail': _(js, 'email'),
                'Twitter': _(js, 'twitter'),
                'YouTube': _(js, 'youtube'),
            }
            if _(js, 'name'):
                for k, v in data.items():
                    data[k] = normalize(v)
                writer.writerow(data)
                f.flush()
                scraped.add(profile_url)

async def get_profiles():
    soup = await bs('https://ra.co/promoters/[CountryCode]}/[City}')
    urls = set()
    for link in soup.css('a[data-tracking-id]'):
        url = get_attr(None, 'href', link)
        if url and url not in scraped and url.startswith('/promoters'):
            urls.add(f'https://ra.co{url}')

    return urls


columns = [
    'URL', 'Promoter', 'About', 'Region', 'Website', 'Facebook', 'Instagram',
    'E-Mail', 'Twitter', 'YouTube'
]


async def main(output, to_excel=False):
    mode = 'a' if os.path.exists(output) else 'w'
    if mode == 'a':
        with open(output, encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            next(reader, None)
            for line in reader:
                scraped.add(line[0])
    print(f'Scraped {len(scraped)} profiles')
    with open(output, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        if mode == 'w':
            writer.writeheader()
            f.flush()
        profiles = await get_profiles()
        for ii in range(3):
            print(f'Run #{ii + 1} ({len(profiles - scraped)} profiles)')
            tasks = []
            for profile in profiles - scraped:
                tasks.append(scrape_profile(profile, writer, f))

            print(f'Running {len(tasks)} tasks')
            if not tasks:
                break
            for task in asyncio.as_completed(tasks):
                try:
                    await task
                except KeyboardInterrupt:
                    return
                except:
                    traceback.print_exc()

    if to_excel:
        df = pd.read_csv(output)
        writer = pd.ExcelWriter(
            output.replace('.csv', '.xlsx'),
            engine='xlsxwriter',
            engine_kwargs={"options": {
                "strings_to_urls": False
            }})
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-o',
                        '--output',
                        type=str,
                        default='output.csv',
                        help="Output file (default: output.csv)")
    parser.add_argument('-t',
                        '--threads',
                        type=int,
                        default=5,
                        help="Number of threads (default: 5)")
    parser.add_argument(
        '--no-excel',
        action='store_true',
        help='Do not convert to Excel (.xlsx) file. (default: False)')

    args = parser.parse_args()
    try:
        sem = asyncio.Semaphore(args.threads)
    except:
        sem = asyncio.Semaphore(5)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main(args.output, not args.no_excel))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
