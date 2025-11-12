"""
Web scraping script for LoopNet commercial real estate portal
Использует requests/bs4 и Selenium для сбора данных
"""
import time
import yaml
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import sys
from pathlib import Path

# Добавление родительской директории в путь для импорта
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from scripts.logger_config import setup_logger
from scripts.utils import clean_text, parse_price, parse_square_feet, extract_city_state


class LoopNetScraper:
    """Класс для скрапинга данных с LoopNet"""
    
    def __init__(self, config_path=None):
        """
        Инициализация скрапера
        
        Args:
            config_path: путь к файлу конфигурации
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = setup_logger(config_path)
        self.scraping_config = self.config.get('scraping', {}).get('loopnet', {})
        self.base_url = self.scraping_config.get('base_url', 'https://www.loopnet.com')
        self.delay = self.scraping_config.get('delay_between_requests', 2)
        self.use_selenium = self.scraping_config.get('use_selenium', True)
        self.headless = self.scraping_config.get('headless', True)
        
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.properties = []
        
    def _init_driver(self):
        """Инициализация Selenium WebDriver"""
        if self.driver is None:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.logger.info("Selenium WebDriver инициализирован")
    
    def _close_driver(self):
        """Закрытие WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.logger.info("Selenium WebDriver закрыт")
    
    def scrape_search_page(self, url: str) -> list:
        """
        Скрапинг страницы поиска
        
        Args:
            url: URL страницы поиска
            
        Returns:
            список URL объектов недвижимости
        """
        self.logger.info(f"Скрапинг страницы поиска: {url}")
        
        if self.use_selenium:
            self._init_driver()
            try:
                self.driver.get(url)
                time.sleep(3)  # Ожидание загрузки
                
                # Имитация работы пользователя - прокрутка страницы
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Поиск ссылок на объекты
                property_links = []
                elements = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="/property/"], a[href*="/listing/"]')
                for elem in elements:
                    href = elem.get_attribute('href')
                    if href and href not in property_links:
                        property_links.append(href)
                
                self.logger.info(f"Найдено {len(property_links)} ссылок на объекты")
                return property_links
                
            except Exception as e:
                self.logger.error(f"Ошибка при скрапинге страницы поиска: {e}")
                return []
        else:
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                property_links = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if '/property/' in href or '/listing/' in href:
                        full_url = href if href.startswith('http') else self.base_url + href
                        if full_url not in property_links:
                            property_links.append(full_url)
                
                self.logger.info(f"Найдено {len(property_links)} ссылок на объекты")
                return property_links
                
            except Exception as e:
                self.logger.error(f"Ошибка при скрапинге страницы поиска: {e}")
                return []
    
    def scrape_property(self, url: str) -> dict:
        """
        Скрапинг страницы объекта недвижимости
        
        Args:
            url: URL объекта
            
        Returns:
            словарь с данными объекта
        """
        self.logger.debug(f"Скрапинг объекта: {url}")
        
        property_data = {
            'source': 'LoopNet',
            'url': url,
            'property_id': None,
            'title': None,
            'description': None,
            'price': None,
            'price_per_sqft': None,
            'square_feet': None,
            'property_type': None,
            'city': None,
            'state': None,
            'address': None,
            'year_built': None,
            'parking_spaces': None,
            'listing_date': None
        }
        
        try:
            if self.use_selenium:
                self._init_driver()
                self.driver.get(url)
                time.sleep(2)
                
                # Имитация работы пользователя
                self.driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(1)
                self.driver.execute_script("window.scrollTo(0, 1000);")
                time.sleep(1)
                
                # Извлечение данных
                try:
                    title_elem = self.driver.find_element(By.CSS_SELECTOR, 'h1, .property-title, [data-testid="property-title"]')
                    property_data['title'] = clean_text(title_elem.text)
                except:
                    pass
                
                try:
                    desc_elem = self.driver.find_element(By.CSS_SELECTOR, '.description, .property-description, [data-testid="description"]')
                    property_data['description'] = clean_text(desc_elem.text)
                except:
                    pass
                
                try:
                    price_elem = self.driver.find_element(By.CSS_SELECTOR, '.price, .property-price, [data-testid="price"]')
                    price_text = price_elem.text
                    property_data['price'] = parse_price(price_text)
                except:
                    pass
                
                try:
                    sqft_elem = self.driver.find_element(By.CSS_SELECTOR, '.square-feet, .sqft, [data-testid="square-feet"]')
                    sqft_text = sqft_elem.text
                    property_data['square_feet'] = parse_square_feet(sqft_text)
                    if property_data['price'] and property_data['square_feet']:
                        property_data['price_per_sqft'] = property_data['price'] / property_data['square_feet']
                except:
                    pass
                
                try:
                    location_elem = self.driver.find_element(By.CSS_SELECTOR, '.location, .address, [data-testid="address"]')
                    location_text = location_elem.text
                    property_data['address'] = clean_text(location_text)
                    city, state = extract_city_state(location_text)
                    property_data['city'] = city
                    property_data['state'] = state
                except:
                    pass
                
                # Получение HTML для дополнительного парсинга
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
            else:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Извлечение данных через BeautifulSoup
                title_elem = soup.select_one('h1, .property-title')
                if title_elem:
                    property_data['title'] = clean_text(title_elem.get_text())
                
                desc_elem = soup.select_one('.description, .property-description')
                if desc_elem:
                    property_data['description'] = clean_text(desc_elem.get_text())
                
                price_elem = soup.select_one('.price, .property-price')
                if price_elem:
                    price_text = price_elem.get_text()
                    property_data['price'] = parse_price(price_text)
                
                sqft_elem = soup.select_one('.square-feet, .sqft')
                if sqft_elem:
                    sqft_text = sqft_elem.get_text()
                    property_data['square_feet'] = parse_square_feet(sqft_text)
                    if property_data['price'] and property_data['square_feet']:
                        property_data['price_per_sqft'] = property_data['price'] / property_data['square_feet']
                
                location_elem = soup.select_one('.location, .address')
                if location_elem:
                    location_text = location_elem.get_text()
                    property_data['address'] = clean_text(location_text)
                    city, state = extract_city_state(location_text)
                    property_data['city'] = city
                    property_data['state'] = state
            
            # Дополнительное извлечение из мета-тегов
            meta_tags = soup.find_all('meta')
            for meta in meta_tags:
                prop = meta.get('property', '')
                content = meta.get('content', '')
                
                if 'og:title' in prop:
                    if not property_data['title']:
                        property_data['title'] = clean_text(content)
                elif 'og:description' in prop:
                    if not property_data['description']:
                        property_data['description'] = clean_text(content)
            
            # Извлечение ID из URL
            if '/property/' in url or '/listing/' in url:
                parts = url.split('/')
                for i, part in enumerate(parts):
                    if part in ['property', 'listing'] and i + 1 < len(parts):
                        property_data['property_id'] = parts[i + 1].split('/')[0]
                        break
            
            self.logger.debug(f"Данные объекта извлечены: {property_data['title']}")
            
        except Exception as e:
            self.logger.error(f"Ошибка при скрапинге объекта {url}: {e}")
        
        return property_data
    
    def scrape_multiple_pages(self, search_urls: list, max_properties: int = 10000) -> pd.DataFrame:
        """
        Скрапинг нескольких страниц поиска
        
        Args:
            search_urls: список URL страниц поиска
            max_properties: максимальное количество объектов для сбора
            
        Returns:
            DataFrame с собранными данными
        """
        self.logger.info(f"Начало сбора данных с LoopNet. Максимум объектов: {max_properties}")
        
        all_property_links = []
        
        # Сбор ссылок на объекты
        for search_url in search_urls:
            links = self.scrape_search_page(search_url)
            all_property_links.extend(links)
            time.sleep(self.delay)
            
            if len(all_property_links) >= max_properties:
                break
        
        # Удаление дубликатов
        all_property_links = list(set(all_property_links))[:max_properties]
        self.logger.info(f"Всего уникальных ссылок: {len(all_property_links)}")
        
        # Скрапинг объектов
        for i, link in enumerate(all_property_links, 1):
            if len(self.properties) >= max_properties:
                break
            
            property_data = self.scrape_property(link)
            if property_data.get('title') or property_data.get('description'):
                self.properties.append(property_data)
            
            if i % 10 == 0:
                self.logger.info(f"Обработано объектов: {len(self.properties)}/{len(all_property_links)}")
            
            time.sleep(self.delay)
        
        self._close_driver()
        
        df = pd.DataFrame(self.properties)
        self.logger.info(f"Сбор данных завершен. Всего собрано: {len(df)} объектов")
        
        return df


def main():
    """Основная функция для запуска скрапинга"""
    scraper = LoopNetScraper()
    
    # URL для поиска недвижимости по городам
    search_urls = [
        f"{scraper.base_url}/search/commercial-real-estate/new-york-ny",
        f"{scraper.base_url}/search/commercial-real-estate/los-angeles-ca",
        f"{scraper.base_url}/search/commercial-real-estate/chicago-il",
        f"{scraper.base_url}/search/commercial-real-estate/houston-tx",
        f"{scraper.base_url}/search/commercial-real-estate/phoenix-az",
    ]
    
    # Сбор данных
    df = scraper.scrape_multiple_pages(search_urls, max_properties=5000)
    
    # Сохранение данных
    output_path = 'data/raw/loopnet_data.csv'
    df.to_csv(output_path, index=False, encoding='utf-8')
    scraper.logger.info(f"Данные сохранены в {output_path}")


if __name__ == '__main__':
    main()

