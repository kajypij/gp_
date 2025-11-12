"""
API script for collecting transportation data from Google Maps API
Собирает данные о транспорте и доступности локаций
"""
import yaml
import pandas as pd
import requests
import time
from typing import List, Dict, Any, Tuple
import sys
from pathlib import Path

# Добавление родительской директории в путь для импорта
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from scripts.logger_config import setup_logger


class GoogleMapsAPI:
    """Класс для работы с Google Maps API"""
    
    def __init__(self, config_path=None):
        """
        Инициализация API
        
        Args:
            config_path: путь к файлу конфигурации
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = setup_logger(config_path)
        self.api_key = self.config.get('api_keys', {}).get('google_maps', '')
        self.api_config = self.config.get('api', {}).get('google_maps', {})
        self.base_url = self.api_config.get('base_url', 'https://maps.googleapis.com/maps/api')
        self.timeout = self.api_config.get('timeout', 30)
        
        if not self.api_key:
            self.logger.warning("API ключ Google Maps не найден в конфигурации")
    
    def _make_request(self, endpoint: str, params: dict) -> dict:
        """
        Выполнение запроса к API
        
        Args:
            endpoint: endpoint API
            params: параметры запроса
            
        Returns:
            ответ API в виде словаря
        """
        url = f"{self.base_url}/{endpoint}"
        params['key'] = self.api_key
        
        try:
            self.logger.info(f"Запрос к API: {endpoint}")
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK':
                self.logger.info(f"Успешный ответ от API: {endpoint}")
                return data
            else:
                self.logger.warning(f"API вернул статус: {data.get('status')}")
                return {}
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ошибка при запросе к API {endpoint}: {e}")
            return {}
    
    def geocode_address(self, address: str) -> Tuple[float, float]:
        """
        Запрос 1: Геокодирование адреса
        
        Args:
            address: адрес для геокодирования
            
        Returns:
            кортеж (широта, долгота)
        """
        self.logger.info(f"Запрос 1: Геокодирование адреса: {address}")
        
        endpoint = "geocode/json"
        params = {
            'address': address
        }
        
        data = self._make_request(endpoint, params)
        
        if data and 'results' in data and len(data['results']) > 0:
            location = data['results'][0]['geometry']['location']
            lat = location['lat']
            lng = location['lng']
            self.logger.info(f"Координаты найдены: ({lat}, {lng})")
            return (lat, lng)
        else:
            self.logger.warning(f"Не удалось найти координаты для адреса: {address}")
            return (None, None)
    
    def get_nearby_places(self, lat: float, lng: float, place_type: str = 'shopping_mall', radius: int = 5000) -> List[Dict]:
        """
        Запрос 2: Поиск ближайших мест
        
        Args:
            lat: широта
            lng: долгота
            place_type: тип места
            radius: радиус поиска в метрах
            
        Returns:
            список мест
        """
        self.logger.info(f"Запрос 2: Поиск ближайших мест типа {place_type}")
        
        endpoint = "place/nearbysearch/json"
        params = {
            'location': f"{lat},{lng}",
            'radius': radius,
            'type': place_type
        }
        
        data = self._make_request(endpoint, params)
        
        if data and 'results' in data:
            places = data['results']
            self.logger.info(f"Найдено мест: {len(places)}")
            return places
        else:
            self.logger.warning("Не удалось найти места")
            return []
    
    def get_distance_matrix(self, origins: List[str], destinations: List[str], mode: str = 'driving') -> Dict:
        """
        Запрос 3: Матрица расстояний
        
        Args:
            origins: список точек отправления
            destinations: список точек назначения
            mode: режим передвижения (driving, walking, transit)
            
        Returns:
            данные о расстояниях и времени
        """
        self.logger.info(f"Запрос 3: Матрица расстояний (режим: {mode})")
        
        endpoint = "distancematrix/json"
        params = {
            'origins': '|'.join(origins),
            'destinations': '|'.join(destinations),
            'mode': mode,
            'units': 'imperial'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and 'rows' in data:
            self.logger.info("Матрица расстояний получена")
            return data
        else:
            self.logger.warning("Не удалось получить матрицу расстояний")
            return {}
    
    def get_directions(self, origin: str, destination: str, mode: str = 'driving') -> Dict:
        """
        Запрос 4: Маршрут между точками
        
        Args:
            origin: точка отправления
            destination: точка назначения
            mode: режим передвижения
            
        Returns:
            данные о маршруте
        """
        self.logger.info(f"Запрос 4: Маршрут от {origin} до {destination}")
        
        endpoint = "directions/json"
        params = {
            'origin': origin,
            'destination': destination,
            'mode': mode
        }
        
        data = self._make_request(endpoint, params)
        
        if data and 'routes' in data and len(data['routes']) > 0:
            route = data['routes'][0]
            leg = route['legs'][0]
            distance = leg['distance']['value']  # в метрах
            duration = leg['duration']['value']  # в секундах
            
            self.logger.info(f"Маршрут найден: расстояние {distance/1000:.2f} км, время {duration/60:.2f} мин")
            return {
                'distance_meters': distance,
                'duration_seconds': duration,
                'distance_text': leg['distance']['text'],
                'duration_text': leg['duration']['text']
            }
        else:
            self.logger.warning("Не удалось найти маршрут")
            return {}
    
    def get_place_details(self, place_id: str) -> Dict:
        """
        Запрос 5: Детальная информация о месте
        
        Args:
            place_id: ID места
            
        Returns:
            детальная информация
        """
        self.logger.info(f"Запрос 5: Детальная информация о месте: {place_id}")
        
        endpoint = "place/details/json"
        params = {
            'place_id': place_id,
            'fields': 'name,rating,user_ratings_total,formatted_address,geometry,types'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and 'result' in data:
            result = data['result']
            self.logger.info(f"Информация о месте получена: {result.get('name')}")
            return result
        else:
            self.logger.warning("Не удалось получить информацию о месте")
            return {}
    
    def collect_transportation_data(self, addresses: List[str]) -> pd.DataFrame:
        """
        Сбор транспортных данных для списка адресов
        
        Args:
            addresses: список адресов
            
        Returns:
            DataFrame с транспортными данными
        """
        self.logger.info("Начало сбора транспортных данных")
        
        results = []
        
        for address in addresses:
            self.logger.info(f"Обработка адреса: {address}")
            
            # Геокодирование
            lat, lng = self.geocode_address(address)
            if lat is None or lng is None:
                continue
            
            time.sleep(0.1)  # Задержка между запросами
            
            # Поиск ближайших торговых центров
            nearby_malls = self.get_nearby_places(lat, lng, 'shopping_mall', 5000)
            mall_count = len(nearby_malls)
            
            time.sleep(0.1)
            
            # Поиск ближайших остановок транспорта
            nearby_transit = self.get_nearby_places(lat, lng, 'transit_station', 2000)
            transit_count = len(nearby_transit)
            
            time.sleep(0.1)
            
            # Матрица расстояний до ключевых точек
            key_destinations = [
                "Times Square, New York, NY",
                "Downtown Los Angeles, CA",
                "Chicago Loop, IL"
            ]
            
            distance_data = self.get_distance_matrix([address], key_destinations, 'driving')
            
            min_distance = None
            min_duration = None
            
            if distance_data and 'rows' in distance_data:
                row = distance_data['rows'][0]
                if 'elements' in row:
                    distances = []
                    durations = []
                    for elem in row['elements']:
                        if elem.get('status') == 'OK':
                            distances.append(elem['distance']['value'])
                            durations.append(elem['duration']['value'])
                    
                    if distances:
                        min_distance = min(distances) / 1000  # в км
                        min_duration = min(durations) / 60  # в минутах
            
            time.sleep(0.1)
            
            # Сохранение результатов
            result = {
                'address': address,
                'latitude': lat,
                'longitude': lng,
                'nearby_malls_count': mall_count,
                'nearby_transit_stations_count': transit_count,
                'min_distance_to_key_location_km': min_distance,
                'min_duration_to_key_location_min': min_duration
            }
            
            results.append(result)
            
            time.sleep(0.2)  # Задержка между адресами
        
        df = pd.DataFrame(results)
        self.logger.info(f"Сбор транспортных данных завершен. Всего записей: {len(df)}")
        
        return df


def main():
    """Основная функция для запуска сбора данных"""
    api = GoogleMapsAPI()
    
    # Адреса для анализа транспортной доступности
    addresses = [
        "123 Main St, New York, NY",
        "456 Broadway, Los Angeles, CA",
        "789 State St, Chicago, IL",
        "321 Main St, Houston, TX",
        "654 Central Ave, Phoenix, AZ"
    ]
    
    # Сбор данных
    df = api.collect_transportation_data(addresses)
    
    if not df.empty:
        # Сохранение данных
        output_path = 'data/raw/google_maps_data.csv'
        df.to_csv(output_path, index=False, encoding='utf-8')
        api.logger.info(f"Данные сохранены в {output_path}")
    else:
        api.logger.error("Не удалось собрать данные")


if __name__ == '__main__':
    main()

