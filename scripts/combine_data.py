"""
Script for combining data from all sources into a single dataset
Объединяет данные из веб-скрапинга и API в единый датасет
"""
import pandas as pd
import yaml
import os
import sys
from pathlib import Path
import numpy as np

# Добавление родительской директории в путь для импорта
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from scripts.logger_config import setup_logger
from scripts.utils import clean_text, fips_to_state_abbr


class DataCombiner:
    """Класс для объединения данных из различных источников"""
    
    def __init__(self, config_path=None):
        """
        Инициализация комбайнера данных
        
        Args:
            config_path: путь к файлу конфигурации
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config.yaml'
        
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.logger = setup_logger(config_path)
        self.data_dir = 'data/raw'
        self.output_dir = 'data/processed'
        
        # Создание директории для обработанных данных
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def load_scraping_data(self) -> pd.DataFrame:
        """
        Загрузка данных из веб-скрапинга
        
        Returns:
            DataFrame с данными из скрапинга
        """
        self.logger.info("Загрузка данных из веб-скрапинга")
        
        dfs = []
        
        # Загрузка данных Crexi
        crexi_path = os.path.join(self.data_dir, 'crexi_data.csv')
        if os.path.exists(crexi_path):
            df_crexi = pd.read_csv(crexi_path)
            self.logger.info(f"Загружено данных из Crexi: {len(df_crexi)} записей")
            dfs.append(df_crexi)
        else:
            self.logger.warning(f"Файл {crexi_path} не найден")
        
        # Загрузка данных LoopNet
        loopnet_path = os.path.join(self.data_dir, 'loopnet_data.csv')
        if os.path.exists(loopnet_path):
            df_loopnet = pd.read_csv(loopnet_path)
            self.logger.info(f"Загружено данных из LoopNet: {len(df_loopnet)} записей")
            dfs.append(df_loopnet)
        else:
            self.logger.warning(f"Файл {loopnet_path} не найден")
        
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            self.logger.info(f"Всего данных из скрапинга: {len(combined_df)} записей")
            return combined_df
        else:
            self.logger.warning("Не удалось загрузить данные из скрапинга")
            return pd.DataFrame()
    
    def load_api_data(self) -> tuple:
        """
        Загрузка данных из API
        
        Returns:
            кортеж (DataFrame с данными Census, DataFrame с данными Google Maps)
        """
        self.logger.info("Загрузка данных из API")
        
        # Загрузка данных Census Bureau
        census_path = os.path.join(self.data_dir, 'census_data.csv')
        df_census = pd.DataFrame()
        if os.path.exists(census_path):
            df_census = pd.read_csv(census_path)
            self.logger.info(f"Загружено данных из Census Bureau: {len(df_census)} записей")
        else:
            self.logger.warning(f"Файл {census_path} не найден")
        
        # Загрузка данных Google Maps
        google_maps_path = os.path.join(self.data_dir, 'google_maps_data.csv')
        df_google_maps = pd.DataFrame()
        if os.path.exists(google_maps_path):
            df_google_maps = pd.read_csv(google_maps_path)
            self.logger.info(f"Загружено данных из Google Maps: {len(df_google_maps)} записей")
        else:
            self.logger.warning(f"Файл {google_maps_path} не найден")
        
        return df_census, df_google_maps
    
    def enrich_with_census_data(self, df: pd.DataFrame, df_census: pd.DataFrame) -> pd.DataFrame:
        """
        Обогащение данных демографической информацией
        
        Args:
            df: основной DataFrame
            df_census: DataFrame с данными Census
            
        Returns:
            обогащенный DataFrame
        """
        if df_census.empty:
            self.logger.warning("Нет данных Census для обогащения")
            return df
        
        self.logger.info("Обогащение данных демографической информацией")
        
        # Приведение FIPS-кодов к почтовым аббревиатурам
        df_census_state = df_census.copy()
        df_census_state['state_fips'] = (
            df_census_state['state']
            .apply(lambda x: f"{int(x):02d}" if pd.notna(x) else None)
        )
        df_census_state['state_abbr'] = df_census_state['state_fips'].apply(fips_to_state_abbr)
        df_census_state = df_census_state.dropna(subset=['state_abbr'])
        
        # Агрегация по штату (средние и медианы для числовых показателей)
        candidate_cols = [
            col for col in df_census_state.columns
            if col not in {'state', 'place', 'state_fips', 'state_abbr'}
        ]
        
        for col in candidate_cols:
            df_census_state[col] = pd.to_numeric(df_census_state[col], errors='coerce')
            if df_census_state[col].notna().any():
                df_census_state.loc[df_census_state[col] < 0, col] = np.nan
        
        numeric_cols = [
            col for col in candidate_cols
            if pd.api.types.is_numeric_dtype(df_census_state[col]) and df_census_state[col].notna().any()
        ]
        
        if not numeric_cols:
            self.logger.warning("В данных Census отсутствуют числовые признаки для агрегации")
            return df
        
        agg_dict = {col: ['mean', 'median'] for col in numeric_cols}
        df_state_stats = df_census_state.groupby('state_abbr').agg(agg_dict)
        df_state_stats.columns = [
            f"census_{col}_{stat}" for col, stat in df_state_stats.columns
        ]
        df_state_stats = df_state_stats.reset_index().rename(columns={'state_abbr': 'state'})
        
        df = df.merge(
            df_state_stats,
            on='state',
            how='left'
        )
        
        self.logger.info("Данные обогащены демографической информацией (уровень штата)")
        return df
    
    def enrich_with_transportation_data(self, df: pd.DataFrame, df_google_maps: pd.DataFrame) -> pd.DataFrame:
        """
        Обогащение данных транспортной информацией
        
        Args:
            df: основной DataFrame
            df_google_maps: DataFrame с данными Google Maps
            
        Returns:
            обогащенный DataFrame
        """
        if df_google_maps.empty:
            self.logger.warning("Нет данных Google Maps для обогащения")
            return df
        
        self.logger.info("Обогащение данных транспортной информацией")
        
        # Объединение по адресу
        if 'address' in df.columns and 'address' in df_google_maps.columns:
            df = df.merge(
                df_google_maps,
                on='address',
                how='left',
                suffixes=('', '_transport')
            )
        else:
            # Если адреса не совпадают, можно попробовать объединить по координатам
            if 'latitude' in df.columns and 'longitude' in df.columns:
                if 'latitude' in df_google_maps.columns and 'longitude' in df_google_maps.columns:
                    # Объединение по ближайшим координатам (упрощенный вариант)
                    self.logger.info("Объединение по координатам")
                    # Для точного объединения нужна более сложная логика
                    # Здесь упрощенный вариант
                    pass
        
        self.logger.info("Данные обогащены транспортной информацией")
        return df
    
    def clean_and_prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Очистка и подготовка данных
        
        Args:
            df: исходный DataFrame
            
        Returns:
            очищенный DataFrame
        """
        self.logger.info("Очистка и подготовка данных")
        
        # Очистка текстовых полей
        text_columns = ['title', 'description', 'address']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(clean_text)
        
        # Удаление дубликатов
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        removed = initial_count - len(df)
        if removed > 0:
            self.logger.info(f"Удалено дубликатов: {removed}")
        
        # Создание целевой переменной (цена за квадратный фут)
        if 'price_per_sqft' not in df.columns:
            if 'price' in df.columns and 'square_feet' in df.columns:
                df['price_per_sqft'] = df['price'] / df['square_feet']
        
        # Создание дополнительных признаков
        if 'description' in df.columns:
            df['description_length'] = df['description'].str.len()
            df['description_word_count'] = df['description'].str.split().str.len()
        
        self.logger.info("Данные очищены и подготовлены")
        return df
    
    def combine_all_data(self) -> pd.DataFrame:
        """
        Объединение всех данных в единый датасет
        
        Returns:
            объединенный DataFrame
        """
        self.logger.info("Начало объединения всех данных")
        
        # Загрузка данных из скрапинга
        df_scraping = self.load_scraping_data()
        
        if df_scraping.empty:
            self.logger.error("Нет данных из скрапинга для объединения")
            return pd.DataFrame()
        
        # Загрузка данных из API
        df_census, df_google_maps = self.load_api_data()
        
        # Обогащение данными Census
        if not df_census.empty:
            df_scraping = self.enrich_with_census_data(df_scraping, df_census)
        
        # Обогащение данными Google Maps
        if not df_google_maps.empty:
            df_scraping = self.enrich_with_transportation_data(df_scraping, df_google_maps)
        
        # Очистка и подготовка
        df_final = self.clean_and_prepare_data(df_scraping)
        
        self.logger.info(f"Объединение данных завершено. Итоговый датасет: {len(df_final)} записей")
        
        return df_final
    
    def save_combined_data(self, df: pd.DataFrame, filename: str = 'combined_dataset.csv'):
        """
        Сохранение объединенного датасета
        
        Args:
            df: DataFrame для сохранения
            filename: имя файла
        """
        output_path = os.path.join(self.output_dir, filename)
        df.to_csv(output_path, index=False, encoding='utf-8')
        self.logger.info(f"Объединенный датасет сохранен в {output_path}")


def main():
    """Основная функция для запуска объединения данных"""
    combiner = DataCombiner()
    
    # Объединение всех данных
    df_combined = combiner.combine_all_data()
    
    if not df_combined.empty:
        # Сохранение
        combiner.save_combined_data(df_combined)
        combiner.logger.info(f"Итоговый датасет содержит {len(df_combined)} записей и {len(df_combined.columns)} признаков")
    else:
        combiner.logger.error("Не удалось объединить данные")


if __name__ == '__main__':
    main()

