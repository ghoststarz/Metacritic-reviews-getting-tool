import os
import re
import time
from urllib.parse import urlparse
from selenium import webdriver
from bs4 import BeautifulSoup
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

#-----------------------------------------------

#提示网络问题是梯子错误
#自动连接到https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page=1
#输入起始截止页码即可
#需要配置Edge版Webdriver，教程参考https://blog.csdn.net/ProgrammerZY/article/details/136373514

#已知问题
#游戏信息应该只存放在excel的第一行，现在会密铺到所有行

#以下报错忽略，不影响功能
#usb-error
#ERROR:fallback_task_provider.cc(127)]

#ver-0.1
#成功提取评论
#ver-0.2
#能够区分媒体、玩家评论
#ver-0.3
#能够标注positive、mixed、negative、tbd四类评论
#ver-0.4
#改用excel存储
#ver-0.5.1
#爬取发售平台数据
#ver-0.5.2
#能从主页爬取游戏列表并转为url
#ver-0.5.3
#修复iphone平台识别错误
#ver-0.5.4
#能够选择起始、截止页面
#ver-0.5.5
#创建高层gamelist文件夹存储
#ver-0.6
#提取游戏信息存放到excel
#ver-0.6.1
#修复开发商发行商提取错误问题
#ver-0.7
#使用多线程加快效率
#ver-beta-0.8
#在crawling_metacritic_async.py中提供了异步执行爬取的方式
#ver-beta-0.9
#添加并发功能，提高效率
#ver-beta-1.0
#补充部分规则
#-----------------------------------------------

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#标准化游戏名并连接到主页
def format_game_url(game_name):

    formatted_name = re.sub(r"[:/()']", "", game_name).lower()
    formatted_name = re.sub(r"\s+", "-", formatted_name)
    return f"https://www.metacritic.com/game/{formatted_name}/", formatted_name

#从列表页提取游戏名
def fetch_game_names(page_url):

    edge_options = webdriver.EdgeOptions()
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    game_names = []
    try:
        driver = webdriver.Edge(options=edge_options)
        driver.get(page_url)
        time.sleep(2)  # 等待页面加载
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        # 查找所有包含游戏名的元素
        game_elements = soup.find_all('div', class_='c-finderProductCard_title')
        for game in game_elements:
            name = game['data-title']
            game_names.append(name)

    except Exception as e:
        logging.error("An error occurred while fetching game names:", e)
    finally:
        if 'driver' in locals():
            driver.quit()
    
    logging.info("Extracted game names: " + ", ".join(game_names))
    return game_names

#提取详细信息
def fetch_game_details(url):

    edge_options = webdriver.EdgeOptions()
    edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    details = {
        "Release Date": "N/A",
        "Developer": "N/A",
        "Publisher": "N/A",
        "Genres": "N/A"
    }

    try:
        driver = webdriver.Edge(options=edge_options)
        driver.get(url)
        time.sleep(2)  # 等待页面加载完成
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        # 提取发售日期
        release_date_element = soup.find('span', class_='g-outer-spacing-left-medium-fluid g-color-gray70 u-block')
        if release_date_element:
            details["Release Date"] = release_date_element.get_text(strip=True)
        
        # 提取开发商
        developer_element = soup.find('div', class_='c-gameDetails_Developer')
        if developer_element:
            developer = developer_element.find('li', class_='c-gameDetails_listItem')
            if developer:
                details["Developer"] = developer.get_text(strip=True)
        
        # 提取发行商
        publisher_element = soup.find('div', class_='c-gameDetails_Distributor')
        if publisher_element:
            publisher = publisher_element.find('span', class_='g-outer-spacing-left-medium-fluid g-color-gray70 u-block')
            if publisher:
                details["Publisher"] = publisher.get_text(strip=True)
        
        # 提取标签
        genres_element = soup.find('div', class_='c-gameDetails_sectionContainer u-flexbox u-flexbox-row u-flexbox-alignBaseline')
        if genres_element:
            genres = [genre.get_text(strip=True) for genre in genres_element.find_all('span', class_='c-globalButton_label')]
            details["Genres"] = ', '.join(genres)

    except Exception as e:
        logging.error("An error occurred while fetching game details:", e)
    finally:
        if 'driver' in locals():
            driver.quit()
    
    logging.info("Game details extracted: " + str(details))
    return details




#识别平台
def fetch_platforms(url):

    edge_options = webdriver.EdgeOptions()
    edge_options.add_argument("--log-level=3")
    edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    platforms_list = []
    try:
        driver = webdriver.Edge(options=edge_options)
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        platforms_div = soup.find('div', class_='c-gameDetails_Platforms')
        if platforms_div:
            platforms = platforms_div.find_all('li', class_='c-gameDetails_listItem')
            for platform in platforms:
                # 获取平台名称并去除括号和斜杠
                platform_name = platform.get_text(strip=True)
                platform_name = re.sub(r"[()/]", "", platform_name).replace(" ", "-")  # 去除括号、斜杠，替换空格为连字符
                platforms_list.append(platform_name)
    except Exception as e:
        logging.error("An error occurred while fetching platforms:", e)
    finally:
        if 'driver' in locals():
            driver.quit()
    
    logging.info("Platforms: " + ", ".join(platforms_list))
    return platforms_list

#创建文件夹
def create_game_and_platform_folders(game_name, platforms):

    base_folder = os.path.join("gamelist", game_name)  # 将 gamelist 作为主文件夹
    os.makedirs(base_folder, exist_ok=True)
    
    platform_folders = {}
    for platform in platforms:
        platform_folder = os.path.join(base_folder, platform)
        os.makedirs(platform_folder, exist_ok=True)
        platform_folders[platform] = platform_folder  # 保存平台文件夹路径
    
    return platform_folders

#保存为excel
def save_to_excel(review_type, reviews, folder, game_details):

    # 将游戏详情和评论数据组合到 DataFrame 中
    data = [[game_details["Release Date"], game_details["Developer"], game_details["Publisher"], game_details["Genres"], rt, rc] for rt, rc in reviews]
    df = pd.DataFrame(data, columns=["Release Date", "Developer", "Publisher", "Genres", "Review Type", "Review Content"])
    
    # 保存为 Excel 文件
    filename = f"{folder}/{review_type}_reviews.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    logging.info(f"Saved reviews to {filename}")


#用颜色识别评价类型
def get_review_type_from_color(color_class):

    if "green" in color_class:
        return "positive"
    elif "yellow" in color_class:
        return "mixed"
    elif "red" in color_class:
        return "negative"
    elif "white" in color_class:
        return "tbd"
    return "unknown"

#爬取评论
def fetch_reviews(url, platform_folder):
    edge_options = webdriver.EdgeOptions()
    edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    reviews = []  # 确保 reviews 为列表
    try:
        driver = webdriver.Edge(options=edge_options)
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        review_type, review_selector, score_selector = '', '', ''
        if "critic-reviews" in url:
            review_type, review_selector, score_selector = 'critic', ".c-siteReview_quote span", ".c-siteReviewScore"
        elif "user-reviews" in url:
            review_type, review_selector, score_selector = 'user', ".c-siteReview_quote span", ".c-siteReviewScore"
        
        score_elements = soup.select(score_selector)[1:]  # 跳过第一个评分元素
        for review, score in zip(soup.select(review_selector), score_elements):
            color_class = score.get("class", [])
            review_type_label = get_review_type_from_color(' '.join(color_class))
            review_text = review.get_text(strip=True)
            reviews.append([review_type_label.upper(), review_text])
        
    except Exception as e:
        logging.error(f"An error occurred while fetching reviews: {e}")
    finally:
        if 'driver' in locals():
            driver.quit()

    return reviews  # 始终返回一个列表

#将一个平台的处理划归给单个线程
def process_game(game_name):

    url, formatted_game_name = format_game_url(game_name)
    
    # 提取游戏详情
    game_details = fetch_game_details(url)
    
    platforms = fetch_platforms(url)
    if not platforms:
        logging.error(f"No platforms found for {game_name}.")
        return
    
    platform_folders = create_game_and_platform_folders(formatted_game_name, platforms)
    
    # 为每个平台爬取 Critic 和 User 评论
    for platform, folder in platform_folders.items():
        critic_url = f"{url}critic-reviews/?platform={platform.lower()}"
        user_url = f"{url}user-reviews/?platform={platform.lower()}"
        
        # 爬取 Critic 评论
        reviews = fetch_reviews(critic_url, folder)
        if reviews:
            save_to_excel('critic', reviews, folder, game_details)
        
        # 爬取 User 评论
        reviews = fetch_reviews(user_url, folder)
        if reviews:
            save_to_excel('user', reviews, folder, game_details)

def main():
    start_page = int(input("Enter the start page: "))
    end_page = int(input("Enter the end page: "))
    
    for page_number in range(start_page, end_page + 1):
        logging.info(f"Processing page {page_number}...")
        page_url = f"https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page={page_number}"
        
        # 获取当前页的所有游戏名
        game_names = fetch_game_names(page_url)
        
        # 使用多线程加速游戏爬取
        with ThreadPoolExecutor(max_workers=5) as executor:  # 最大线程数可根据需求调整
            futures = [executor.submit(process_game, game_name) for game_name in game_names]
            
            # 逐个完成任务
            for future in as_completed(futures):
                try:
                    future.result()  # 获取结果
                except Exception as e:
                    logging.error(f"An error occurred in thread: {e}")

if __name__ == "__main__":
    main()
