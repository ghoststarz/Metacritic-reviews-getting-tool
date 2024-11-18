import asyncio
import aiohttp
import logging
import re
from bs4 import BeautifulSoup
import pandas as pd
import os
import xlsxwriter

# 配置日志显示
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
HEADERS = {"User-Agent": "Mozilla/5.0"}

#标准化游戏名
def sanitize_name(name):
    name = re.sub(r"[<>:\"/\\|?*'().,]", '', name).lower()  # 去除非法字符
    name = re.sub(r"\s+", "-", name)  # 将空格替换为短横线，连续空格视为一个
    name = name.replace('&', 'and')  # 将&替换为and
    return name

#获取html信息
async def fetch_html(session, url):
    async with session.get(url, headers=HEADERS) as response:
        response.raise_for_status()
        logging.info(f"Fetched HTML for URL: {url}")
        return await response.text()

#获取游戏名
async def fetch_game_names(page_url, session):
    html = await fetch_html(session, page_url)
    soup = BeautifulSoup(html, 'lxml')
    game_elements = soup.find_all('div', class_='c-finderProductCard_title')
    game_names = [game['data-title'] for game in game_elements]
    logging.info(f"Fetched game names from page: {page_url}")
    return game_names

#游戏详情信息
async def fetch_game_details(url, session):
    html = await fetch_html(session, url)
    soup = BeautifulSoup(html, 'lxml')
    details = {
        "Release Date": "N/A",
        "Developer": "N/A",
        "Publisher": "N/A",
        "Genres": "N/A"
    }

    release_date_element = soup.find('span', class_='g-outer-spacing-left-medium-fluid g-color-gray70 u-block')
    if release_date_element:
        details["Release Date"] = release_date_element.get_text(strip=True)

    developer_element = soup.find('div', class_='c-gameDetails_Developer')
    if developer_element:
        developer = developer_element.find('li', class_='c-gameDetails_listItem')
        if developer:
            details["Developer"] = developer.get_text(strip=True)

    publisher_element = soup.find('div', class_='c-gameDetails_Distributor')
    if publisher_element:
        publisher = publisher_element.find('span', class_='g-outer-spacing-left-medium-fluid g-color-gray70 u-block')
        if publisher:
            details["Publisher"] = publisher.get_text(strip=True)

    genres_element = soup.find('div', class_='c-gameDetails_sectionContainer u-flexbox u-flexbox-row u-flexbox-alignBaseline')
    if genres_element:
        genres = [genre.get_text(strip=True) for genre in genres_element.find_all('span', class_='c-globalButton_label')]
        details["Genres"] = ', '.join(genres)
    
    logging.info(f"Extracted game details: {details}")
    return details

#游戏平台信息
async def fetch_platform_from_game_details(url, session):
    html = await fetch_html(session, url)
    soup = BeautifulSoup(html, 'lxml')
    platform_elements = soup.select('.c-gameDetails_Platforms li.c-gameDetails_listItem')
    
    platforms = []
    for platform in platform_elements:
        platform_name = platform.get_text(strip=True).replace(' ', '-').lower()
        if platform_name == 'ios-(iphone/ipad)':
            platform_name = 'ios-iphoneipad'
        platforms.append(platform_name)
    
    logging.info(f"Extracted platforms from game details URL: {url}")
    return platforms

#获取评论
async def fetch_game_reviews(game_name, platform, review_type, session, max_reviews=200):
    """
    异步获取游戏评论内容并区分好中差评。
    """
    reviews = []
    offset = 0
    limit = 50
    api_key = "1MOZgmNFxvmljaQR1X9KAij9Mo4xAY3u"
    
    while len(reviews) < max_reviews:
        ajax_url = (
            f"https://backend.metacritic.com/v1/xapi/reviews/metacritic/{review_type}/games/"
            f"{game_name}/platform/{platform}/web?apiKey={api_key}&offset={offset}&limit={limit}&filterBySentiment=all"
            f"&sort=date&componentName={review_type}-reviews&componentDisplayName={review_type}%20Reviews&componentType=ReviewList"
        )
        json_data = await fetch_json_with_retries(session, ajax_url)
        
        if not json_data or "data" not in json_data or "items" not in json_data["data"]:
            break

        for review in json_data["data"]["items"]:
            score = review.get("score")
            if score is None:
                sentiment = "tbd"  # 无评分的情况
            else:
                if review_type == "user":
                    # User评论：大于等于7为好评，小于等于4为差评
                    sentiment = "positive" if score >= 7 else "negative" if score <= 4 else "mixed"
                elif review_type == "critic":
                    # Critic评论：大于等于70为好评，小于等于40为差评
                    sentiment = "positive" if score >= 70 else "negative" if score <= 40 else "mixed"
            
            reviews.append([sentiment.upper(), review.get("quote", "")])

            if len(reviews) >= max_reviews:
                break

        if len(json_data["data"]["items"]) < limit:
            break
        offset += limit
        await asyncio.sleep(1)

    return reviews


#获取json信息
async def fetch_json_with_retries(session, url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status in [500, 503]:
                    logging.warning(f"Retrying {url} due to server error ({response.status}). Attempt {attempt + 1}")
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            logging.error(f"Request failed for {url}: {e}")
            await asyncio.sleep(delay)
    logging.error(f"Failed to retrieve data after {retries} attempts for {url}.")
    return None

#保存为excel
async def save_to_excel(review_type, reviews, folder, game_details):
    data = [[game_details["Release Date"], game_details["Developer"], game_details["Publisher"], game_details["Genres"], rt, rc] for rt, rc in reviews]
    df = pd.DataFrame(data, columns=["Release Date", "Developer", "Publisher", "Genres", "Review Type", "Review Content"])
    
    filename = f"{folder}/{review_type}_reviews.xlsx"
    os.makedirs(folder, exist_ok=True)
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    logging.info(f"Saved {review_type} reviews to {filename}")

#链接评论页网站
async def process_game_reviews(game_name, session):
    sanitized_name = sanitize_name(game_name)
    game_url = f"https://www.metacritic.com/game/{sanitized_name}"
    platforms = await fetch_platform_from_game_details(game_url, session)
    
    for platform in platforms:
        platform_url = f"https://www.metacritic.com/game/{platform}/{sanitized_name}"
        game_details = await fetch_game_details(platform_url, session)
        folder = f"gamelist/{sanitized_name}/{platform}"

        logging.info(f"Processing reviews for {game_name} on platform {platform}")
        
        user_reviews = await fetch_game_reviews(sanitized_name, platform, "user", session)
        await save_to_excel("user", user_reviews, folder, game_details)
        
        critic_reviews = await fetch_game_reviews(sanitized_name, platform, "critic", session)
        await save_to_excel("critic", critic_reviews, folder, game_details)

async def main():
    start_page = int(input("Enter the start page number: "))
    end_page = int(input("Enter the end page number: "))

    async with aiohttp.ClientSession() as session:
        page_tasks = []
        for page_number in range(start_page, end_page + 1):
            page_url = f"https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page={page_number}"
            logging.info(f"Fetching game names from page {page_number}")
            page_tasks.append(fetch_game_names(page_url, session))

        all_game_names = await asyncio.gather(*page_tasks)

        game_tasks = []
        for game_names in all_game_names:
            for game_name in game_names:
                logging.info(f"Starting review processing for game: {game_name}")
                game_tasks.append(process_game_reviews(game_name, session))

        await asyncio.gather(*game_tasks)

# 运行主函数
asyncio.run(main())
