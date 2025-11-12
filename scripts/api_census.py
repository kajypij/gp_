"""
API script for collecting demographic data from US Census Bureau
Выполняет минимум 5 различных запросов к API
"""
import yaml
import pandas as pd
import requests
from typing import List, Dict, Any
import sys
from pathlib import Path

# Добавление родительской директории в путь для импорта
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from scripts.logger_config import setup_logger


class CensusBureauAPI:
    """Класс для работы с API Census Bureau"""
    
    def __init__(self, config_path=None):
        """
        Инициализация API клиента
        
        Args:
            config_path: путь к файлу конфигурации
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = setup_logger(config_path)
        self.api_key = self.config.get('api_keys', {}).get('census_bureau', '')
        self.api_config = self.config.get('api', {}).get('census_bureau', {})
        self.base_url = self.api_config.get('base_url', 'https://api.census.gov/data')
        self.timeout = self.api_config.get('timeout', 30)
        
        if not self.api_key:
            self.logger.warning("API ключ Census Bureau не найден в конфигурации")
    
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
            self.logger.info(f"Успешный ответ от API: {endpoint}")
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Ошибка при запросе к API {endpoint}: {e}")
            return {}
    
    def get_population_by_city(self, cities: List[str], year: int = 2020) -> pd.DataFrame:
        """
        Запрос 1: Получение данных о населении по городам
        
        Args:
            cities: список городов
            year: год данных
            
        Returns:
            DataFrame с данными о населении
        """
        self.logger.info("Запрос 1: Получение данных о населении по городам")
        
        # Используем ACS 5-year estimates
        endpoint = f"{year}/acs/acs5"
        params = {
            'get': 'B01001_001E',  # Total population
            'for': 'place:*',
            'in': 'state:*'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            df['B01001_001E'] = pd.to_numeric(df['B01001_001E'], errors='coerce')
            df = df.rename(columns={'B01001_001E': 'population'})
            return df
        else:
            self.logger.warning("Не удалось получить данные о населении")
            return pd.DataFrame()
    
    def get_median_household_income(self, cities: List[str], year: int = 2020) -> pd.DataFrame:
        """
        Запрос 2: Получение медианного дохода домохозяйств
        
        Args:
            cities: список городов
            year: год данных
            
        Returns:
            DataFrame с данными о доходах
        """
        self.logger.info("Запрос 2: Получение медианного дохода домохозяйств")
        
        endpoint = f"{year}/acs/acs5"
        params = {
            'get': 'B19013_001E',  # Median household income
            'for': 'place:*',
            'in': 'state:*'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            df['B19013_001E'] = pd.to_numeric(df['B19013_001E'], errors='coerce')
            df = df.rename(columns={'B19013_001E': 'median_household_income'})
            return df
        else:
            self.logger.warning("Не удалось получить данные о доходах")
            return pd.DataFrame()
    
    def get_employment_data(self, cities: List[str], year: int = 2020) -> pd.DataFrame:
        """
        Запрос 3: Получение данных о занятости
        
        Args:
            cities: список городов
            year: год данных
            
        Returns:
            DataFrame с данными о занятости
        """
        self.logger.info("Запрос 3: Получение данных о занятости")
        
        endpoint = f"{year}/acs/acs5"
        params = {
            'get': 'B23025_002E,B23025_003E,B23025_004E,B23025_005E',  # Employment status
            'for': 'place:*',
            'in': 'state:*'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            for col in ['B23025_002E', 'B23025_003E', 'B23025_004E', 'B23025_005E']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.rename(columns={
                'B23025_002E': 'in_labor_force',
                'B23025_003E': 'civilian_labor_force',
                'B23025_004E': 'employed',
                'B23025_005E': 'unemployed'
            })
            if 'employed' in df.columns and 'in_labor_force' in df.columns:
                df['employment_rate'] = df['employed'] / df['in_labor_force'] * 100
            return df
        else:
            self.logger.warning("Не удалось получить данные о занятости")
            return pd.DataFrame()
    
    def get_housing_data(self, cities: List[str], year: int = 2020) -> pd.DataFrame:
        """
        Запрос 4: Получение данных о жилье
        
        Args:
            cities: список городов
            year: год данных
            
        Returns:
            DataFrame с данными о жилье
        """
        self.logger.info("Запрос 4: Получение данных о жилье")
        
        endpoint = f"{year}/acs/acs5"
        params = {
            'get': 'B25001_001E,B25002_001E,B25002_002E,B25002_003E',  # Housing units
            'for': 'place:*',
            'in': 'state:*'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            for col in ['B25001_001E', 'B25002_001E', 'B25002_002E', 'B25002_003E']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.rename(columns={
                'B25001_001E': 'total_housing_units',
                'B25002_001E': 'total_occupied',
                'B25002_002E': 'occupied',
                'B25002_003E': 'vacant'
            })
            if 'total_housing_units' in df.columns and 'vacant' in df.columns:
                df['vacancy_rate'] = df['vacant'] / df['total_housing_units'] * 100
            return df
        else:
            self.logger.warning("Не удалось получить данные о жилье")
            return pd.DataFrame()
    
    def get_education_data(self, cities: List[str], year: int = 2020) -> pd.DataFrame:
        """
        Запрос 5: Получение данных об образовании
        
        Args:
            cities: список городов
            year: год данных
            
        Returns:
            DataFrame с данными об образовании
        """
        self.logger.info("Запрос 5: Получение данных об образовании")
        
        endpoint = f"{year}/acs/acs5"
        params = {
            'get': 'B15003_001E,B15003_022E,B15003_023E,B15003_024E,B15003_025E',  # Educational attainment
            'for': 'place:*',
            'in': 'state:*'
        }
        
        data = self._make_request(endpoint, params)
        
        if data and len(data) > 1:
            df = pd.DataFrame(data[1:], columns=data[0])
            for col in df.columns:
                if col not in ['state', 'place']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df.rename(columns={
                'B15003_001E': 'total_population_25plus',
                'B15003_022E': 'bachelors_degree',
                'B15003_023E': 'masters_degree',
                'B15003_024E': 'professional_degree',
                'B15003_025E': 'doctorate_degree'
            })
            if 'total_population_25plus' in df.columns:
                for col in ['bachelors_degree', 'masters_degree', 'professional_degree', 'doctorate_degree']:
                    if col in df.columns:
                        df[f'{col}_rate'] = df[col] / df['total_population_25plus'] * 100
            return df
        else:
            self.logger.warning("Не удалось получить данные об образовании")
            return pd.DataFrame()
    
    def collect_all_data(self, cities: List[str], year: int = 2020) -> pd.DataFrame:
        """
        Сбор всех данных через различные запросы к API
        
        Args:
            cities: список городов
            year: год данных
            
        Returns:
            объединенный DataFrame со всеми данными
        """
        self.logger.info("Начало сбора всех данных через Census Bureau API")
        
        # Выполнение всех 5+ запросов
        dfs = []
        
        # Запрос 1: Население
        df_pop = self.get_population_by_city(cities, year)
        if not df_pop.empty:
            dfs.append(('population', df_pop))
        
        # Запрос 2: Доходы
        df_income = self.get_median_household_income(cities, year)
        if not df_income.empty:
            dfs.append(('income', df_income))
        
        # Запрос 3: Занятость
        df_employment = self.get_employment_data(cities, year)
        if not df_employment.empty:
            dfs.append(('employment', df_employment))
        
        # Запрос 4: Жилье
        df_housing = self.get_housing_data(cities, year)
        if not df_housing.empty:
            dfs.append(('housing', df_housing))
        
        # Запрос 5: Образование
        df_education = self.get_education_data(cities, year)
        if not df_education.empty:
            dfs.append(('education', df_education))
        
        # Объединение данных
        if dfs:
            merged_df = dfs[0][1]
            for name, df in dfs[1:]:
                if 'state' in df.columns and 'place' in df.columns:
                    merged_df = merged_df.merge(
                        df,
                        on=['state', 'place'],
                        how='outer',
                        suffixes=('', f'_{name}')
                    )
            
            self.logger.info(f"Сбор данных завершен. Всего записей: {len(merged_df)}")
            return merged_df
        else:
            self.logger.warning("Не удалось собрать данные")
            return pd.DataFrame()


def main():
    """Основная функция для запуска сбора данных"""
    api = CensusBureauAPI()
    
    # Список городов для анализа
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
    
    # Сбор данных
    df = api.collect_all_data(cities, year=2020)
    
    if not df.empty:
        # Сохранение данных
        output_path = 'data/raw/census_data.csv'
        df.to_csv(output_path, index=False, encoding='utf-8')
        api.logger.info(f"Данные сохранены в {output_path}")
    else:
        api.logger.error("Не удалось собрать данные")


if __name__ == '__main__':
    main()

