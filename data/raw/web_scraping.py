# %% [markdown]
# # Cào Web [QIPEDC](https://qipedc.moet.gov.vn/dictionary)

# %%
import asyncio
import re
import time
import csv
from typing import List, Tuple, Optional

import pandas as pd
from bs4 import BeautifulSoup
from requests_html import AsyncHTMLSession

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# %% [markdown]
# ## Thiết lập Selenium WebDriver

# %%
def setup_webdriver(chrome_driver_path: str, headless: bool = True) -> webdriver.Chrome:
    """
    Thiết lập và trả về một đối tượng Selenium WebDriver.
    
    :param chrome_driver_path: Đường dẫn đến ChromeDriver
    :param headless: Chạy Chrome ở chế độ không giao diện
    :return: Đối tượng Chrome WebDriver
    """
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    
    service = Service(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# %% [markdown]
# ## Hàm Cào Nội Dung Bằng Requests-HTML (Tùy Chọn)

# %%
async def fetch_content(url: str) -> str:
    """
    Lấy nội dung HTML của một trang web sử dụng requests-html.
    
    :param url: URL của trang web
    :return: Nội dung HTML dưới dạng chuỗi
    """
    asession = AsyncHTMLSession()
    response = await asession.get(url)
    await response.html.arender()
    return response.html.html

# %% [markdown]
# ## Hàm Xử Lý BeautifulSoup

# %%
def parse_html(content: str) -> BeautifulSoup:
    """
    Phân tích nội dung HTML bằng BeautifulSoup.
    
    :param content: Nội dung HTML
    :return: Đối tượng BeautifulSoup
    """
    return BeautifulSoup(content, 'html.parser')

# %% [markdown]
# ## Hàm Thu Thập Dữ Liệu Từ BeautifulSoup

# %%
def extract_data(soup: BeautifulSoup) -> Tuple[List[str], List[str]]:
    """
    Thu thập đoạn văn bản và liên kết hình ảnh từ BeautifulSoup.
    
    :param soup: Đối tượng BeautifulSoup
    :return: Tuple chứa danh sách văn bản và danh sách liên kết hình ảnh
    """
    # Thu thập tất cả đoạn văn bản với class cụ thể
    list_text = [p.get_text(strip=True) for p in soup.find_all('p', class_='t-a-center f-s-18 f-f-Lato-Black mb-0')]
    
    # Thu thập tất cả liên kết hình ảnh với class cụ thể, bỏ hình ảnh đầu tiên
    images = [img['src'] for img in soup.find_all('img', class_='m-w-100')][1:]
    
    return list_text, images

# %% [markdown]
# ## Hàm Chuyển Đổi Liên Kết Hình Ảnh Thành Liên Kết Video

# %%
def get_video_links(images: List[str]) -> List[str]:
    """
    Chuyển đổi liên kết hình ảnh thành liên kết video.
    
    :param images: Danh sách liên kết hình ảnh
    :return: Danh sách liên kết video
    """
    return [
        img.replace("thumbs", "videos").replace(".png", ".mp4?autoplay=true")
        for img in images
    ]

# %% [markdown]
# ## Hàm Thu Thập Dữ Liệu Bằng Selenium

# %%
def scrape_with_selenium(url: str, chrome_driver_path: str, wait_time: int = 10) -> List[Tuple[str, str]]:
    """
    Thu thập dữ liệu text và video từ trang web sử dụng Selenium.
    
    :param url: URL của trang web
    :param chrome_driver_path: Đường dẫn đến ChromeDriver
    :param wait_time: Thời gian chờ tối đa cho WebDriverWait
    :return: Danh sách các cặp (text, video URL)
    """
    driver = setup_webdriver(chrome_driver_path)
    driver.get(url)
    wait = WebDriverWait(driver, wait_time)
    all_text_video = []

    try:
        while True:
            time.sleep(2)  # Đợi nội dung tải
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            list_text, images = extract_data(soup)
            videos = get_video_links(images)
            text_video = list(zip(list_text, videos))
            all_text_video.extend(text_video)
            
            # Tìm nút chuyển trang
            buttons = driver.find_elements(By.CSS_SELECTOR, 'button.btn.mx-1.btn-sm')
            btn_info = next((btn for btn in buttons if 'btn-info' in btn.get_attribute('class')), None)
            
            if btn_info:
                btn_index = buttons.index(btn_info)
                if btn_index + 1 < len(buttons):
                    next_button = buttons[btn_index + 1]
                    next_button.click()
                    wait.until(EC.staleness_of(next_button))
                else:
                    print("Không còn trang nào để tiếp tục.")
                    break
            else:
                print("Không tìm thấy nút 'btn-info'.")
                break
    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")
    finally:
        driver.quit()
    
    return all_text_video

# %% [markdown]
# ## Hàm Xuất Dữ Liệu Ra File CSV

# %%
def export_to_csv(data: List[Tuple[str, str]], csv_file_path: str) -> None:
    """
    Xuất dữ liệu ra file CSV.
    
    :param data: Danh sách các cặp (text, video URL)
    :param csv_file_path: Đường dẫn đến file CSV
    """
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Text', 'Video'])
        writer.writerows(data)
    print(f"Đã lưu dữ liệu vào {csv_file_path}")

# %% [markdown]
# ## Hàm Xử Lý Dữ Liệu Với Pandas

# %%
def process_data(csv_file_path: str) -> pd.DataFrame:
    """
    Đọc và xử lý dữ liệu từ file CSV.
    
    :param csv_file_path: Đường dẫn đến file CSV
    :return: DataFrame đã được xử lý
    """
    df = pd.read_csv(csv_file_path)
    
    # Đổi tên cột và nối thêm phần URL
    df.rename(columns={'Video': 'Video URL'}, inplace=True)
    df['Video URL'] = 'https://qipedc.moet.gov.vn' + df['Video URL']
    
    # Thêm cột Region
    df['Region'] = df['Video URL'].apply(extract_region_from_url)
    
    # Thêm cột Label
    df['Label'] = df['Text'].str.replace(' ', '_') + df['Region'].fillna('')
    
    # Sắp xếp lại DataFrame
    df.sort_values('Text', inplace=True)
    
    return df

def extract_region_from_url(video_url: str) -> Optional[str]:
    """
    Trích xuất khu vực từ URL video.
    
    :param video_url: URL của video
    :return: Khu vực ('B', 'T', 'N') hoặc None
    """
    match = re.search(r'/videos/D\d{4}([BTN])\.mp4', video_url)
    if match:
        return match.group(1)
    return None

# %% [markdown]
# ## Hàm Lưu DataFrame Vào CSV

# %%
def save_dataframe(df: pd.DataFrame, csv_file_path: str) -> None:
    """
    Lưu DataFrame vào file CSV.
    
    :param df: DataFrame cần lưu
    :param csv_file_path: Đường dẫn đến file CSV
    """
    df.to_csv(csv_file_path, index=False, encoding='utf-8')
    print(f"Đã cập nhật dữ liệu vào {csv_file_path}")

# %% [markdown]
# ## Hàm Chính để Thực Thi Cào Dữ Liệu và Xử Lý

# %%
def main():
    # Thiết lập các tham số
    url = 'https://qipedc.moet.gov.vn/dictionary'
    chrome_driver_path = '/opt/homebrew/bin/chromedriver'  # Cập nhật đường dẫn nếu cần
    csv_file_path = '../data/raw/text_video_data.csv'
    
    # Bước 1: Thu thập dữ liệu bằng Selenium
    print("Bắt đầu thu thập dữ liệu bằng Selenium...")
    all_text_video = scrape_with_selenium(url, chrome_driver_path)
    print(f"Đã thu thập được {len(all_text_video)} cặp text-video.")
    
    # Bước 2: Xuất dữ liệu ra CSV
    export_to_csv(all_text_video, csv_file_path)
    
    # Bước 3: Đọc và xử lý dữ liệu với Pandas
    print("Đang xử lý dữ liệu với Pandas...")
    df = process_data(csv_file_path)
    print(df.head())
    
    # Bước 4: Lưu DataFrame đã xử lý vào CSV
    save_dataframe(df, csv_file_path)
    
    print("Hoàn thành quá trình cào dữ liệu và xử lý.")

# %% [markdown]
# ## Chạy Hàm Chính

# %%
if __name__ == "__main__":
    main()

# %% [markdown]
# ## Ghi Chú

# %%
# - Đảm bảo rằng bạn đã cài đặt các thư viện cần thiết:
#   ```
#   pip install asyncio requests-html beautifulsoup4 selenium pandas
#   ```
# - Cài đặt ChromeDriver phù hợp với phiên bản Chrome của bạn từ [trang chính thức](https://googlechromelabs.github.io/chrome-for-testing/).
# - Cập nhật `chrome_driver_path` trong hàm `main` nếu cần thiết.
# - Nếu bạn gặp sự cố khi chạy ChromeDriver trên macOS, hãy tham khảo [bài thảo luận này](https://discussions.apple.com/thread/250425993?sortBy=rank).