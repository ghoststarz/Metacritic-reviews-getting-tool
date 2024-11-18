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

#��ʾ�������������Ӵ���
#�Զ����ӵ�https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page=1
#������ʼ��ֹҳ�뼴��
#��Ҫ����Edge��Webdriver���̳̲ο�https://blog.csdn.net/ProgrammerZY/article/details/136373514

#��֪����
#��Ϸ��ϢӦ��ֻ�����excel�ĵ�һ�У����ڻ����̵�������

#���±�����ԣ���Ӱ�칦��
#usb-error
#ERROR:fallback_task_provider.cc(127)]

#ver-0.1
#�ɹ���ȡ����
#ver-0.2
#�ܹ�����ý�塢�������
#ver-0.3
#�ܹ���עpositive��mixed��negative��tbd��������
#ver-0.4
#����excel�洢
#ver-0.5.1
#��ȡ����ƽ̨����
#ver-0.5.2
#�ܴ���ҳ��ȡ��Ϸ�б�תΪurl
#ver-0.5.3
#�޸�iphoneƽ̨ʶ�����
#ver-0.5.4
#�ܹ�ѡ����ʼ����ֹҳ��
#ver-0.5.5
#�����߲�gamelist�ļ��д洢
#ver-0.6
#��ȡ��Ϸ��Ϣ��ŵ�excel
#ver-0.6.1
#�޸������̷�������ȡ��������
#ver-0.7
#ʹ�ö��̼߳ӿ�Ч��
#ver-beta-0.8
#��crawling_metacritic_async.py���ṩ���첽ִ����ȡ�ķ�ʽ
#ver-beta-0.9
#��Ӳ������ܣ����Ч��
#ver-beta-1.0
#���䲿�ֹ���
#-----------------------------------------------

# ������־��¼
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#��׼����Ϸ�������ӵ���ҳ
def format_game_url(game_name):

    formatted_name = re.sub(r"[:/()']", "", game_name).lower()
    formatted_name = re.sub(r"\s+", "-", formatted_name)
    return f"https://www.metacritic.com/game/{formatted_name}/", formatted_name

#���б�ҳ��ȡ��Ϸ��
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
        time.sleep(2)  # �ȴ�ҳ�����
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        # �������а�����Ϸ����Ԫ��
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

#��ȡ��ϸ��Ϣ
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
        time.sleep(2)  # �ȴ�ҳ��������
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        # ��ȡ��������
        release_date_element = soup.find('span', class_='g-outer-spacing-left-medium-fluid g-color-gray70 u-block')
        if release_date_element:
            details["Release Date"] = release_date_element.get_text(strip=True)
        
        # ��ȡ������
        developer_element = soup.find('div', class_='c-gameDetails_Developer')
        if developer_element:
            developer = developer_element.find('li', class_='c-gameDetails_listItem')
            if developer:
                details["Developer"] = developer.get_text(strip=True)
        
        # ��ȡ������
        publisher_element = soup.find('div', class_='c-gameDetails_Distributor')
        if publisher_element:
            publisher = publisher_element.find('span', class_='g-outer-spacing-left-medium-fluid g-color-gray70 u-block')
            if publisher:
                details["Publisher"] = publisher.get_text(strip=True)
        
        # ��ȡ��ǩ
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




#ʶ��ƽ̨
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
                # ��ȡƽ̨���Ʋ�ȥ�����ź�б��
                platform_name = platform.get_text(strip=True)
                platform_name = re.sub(r"[()/]", "", platform_name).replace(" ", "-")  # ȥ�����š�б�ܣ��滻�ո�Ϊ���ַ�
                platforms_list.append(platform_name)
    except Exception as e:
        logging.error("An error occurred while fetching platforms:", e)
    finally:
        if 'driver' in locals():
            driver.quit()
    
    logging.info("Platforms: " + ", ".join(platforms_list))
    return platforms_list

#�����ļ���
def create_game_and_platform_folders(game_name, platforms):

    base_folder = os.path.join("gamelist", game_name)  # �� gamelist ��Ϊ���ļ���
    os.makedirs(base_folder, exist_ok=True)
    
    platform_folders = {}
    for platform in platforms:
        platform_folder = os.path.join(base_folder, platform)
        os.makedirs(platform_folder, exist_ok=True)
        platform_folders[platform] = platform_folder  # ����ƽ̨�ļ���·��
    
    return platform_folders

#����Ϊexcel
def save_to_excel(review_type, reviews, folder, game_details):

    # ����Ϸ���������������ϵ� DataFrame ��
    data = [[game_details["Release Date"], game_details["Developer"], game_details["Publisher"], game_details["Genres"], rt, rc] for rt, rc in reviews]
    df = pd.DataFrame(data, columns=["Release Date", "Developer", "Publisher", "Genres", "Review Type", "Review Content"])
    
    # ����Ϊ Excel �ļ�
    filename = f"{folder}/{review_type}_reviews.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    logging.info(f"Saved reviews to {filename}")


#����ɫʶ����������
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

#��ȡ����
def fetch_reviews(url, platform_folder):
    edge_options = webdriver.EdgeOptions()
    edge_options.add_argument("--headless")
    edge_options.add_argument("--disable-gpu")
    edge_options.add_argument("--no-sandbox")
    edge_options.add_experimental_option("excludeSwitches", ["enable-logging"])

    reviews = []  # ȷ�� reviews Ϊ�б�
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
        
        score_elements = soup.select(score_selector)[1:]  # ������һ������Ԫ��
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

    return reviews  # ʼ�շ���һ���б�

#��һ��ƽ̨�Ĵ�����������߳�
def process_game(game_name):

    url, formatted_game_name = format_game_url(game_name)
    
    # ��ȡ��Ϸ����
    game_details = fetch_game_details(url)
    
    platforms = fetch_platforms(url)
    if not platforms:
        logging.error(f"No platforms found for {game_name}.")
        return
    
    platform_folders = create_game_and_platform_folders(formatted_game_name, platforms)
    
    # Ϊÿ��ƽ̨��ȡ Critic �� User ����
    for platform, folder in platform_folders.items():
        critic_url = f"{url}critic-reviews/?platform={platform.lower()}"
        user_url = f"{url}user-reviews/?platform={platform.lower()}"
        
        # ��ȡ Critic ����
        reviews = fetch_reviews(critic_url, folder)
        if reviews:
            save_to_excel('critic', reviews, folder, game_details)
        
        # ��ȡ User ����
        reviews = fetch_reviews(user_url, folder)
        if reviews:
            save_to_excel('user', reviews, folder, game_details)

def main():
    start_page = int(input("Enter the start page: "))
    end_page = int(input("Enter the end page: "))
    
    for page_number in range(start_page, end_page + 1):
        logging.info(f"Processing page {page_number}...")
        page_url = f"https://www.metacritic.com/browse/game/?releaseYearMin=1958&releaseYearMax=2024&page={page_number}"
        
        # ��ȡ��ǰҳ��������Ϸ��
        game_names = fetch_game_names(page_url)
        
        # ʹ�ö��̼߳�����Ϸ��ȡ
        with ThreadPoolExecutor(max_workers=5) as executor:  # ����߳����ɸ����������
            futures = [executor.submit(process_game, game_name) for game_name in game_names]
            
            # ����������
            for future in as_completed(futures):
                try:
                    future.result()  # ��ȡ���
                except Exception as e:
                    logging.error(f"An error occurred in thread: {e}")

if __name__ == "__main__":
    main()
