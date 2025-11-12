"""
Main script to run the entire data collection pipeline
Главный скрипт для запуска всего пайплайна сбора данных
"""
import sys
import os
from pathlib import Path

# Добавление директории scripts в путь
scripts_dir = Path(__file__).parent / 'scripts'
sys.path.insert(0, str(scripts_dir))

import yaml
from logger_config import setup_logger


def main():
    """Главная функция для запуска всего пайплайна"""
    # Настройка логирования
    logger = setup_logger()
    logger.info("=" * 60)
    logger.info("Запуск пайплайна сбора данных о коммерческой недвижимости")
    logger.info("=" * 60)
    
    # Загрузка конфигурации
    config_path = Path('config.yaml')
    if not config_path.exists():
        logger.error(f"Файл конфигурации {config_path} не найден!")
        return
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # Проверка API ключей
    api_keys = config.get('api_keys', {})
    if not api_keys.get('census_bureau') or api_keys.get('census_bureau') == 'YOUR_CENSUS_API_KEY_HERE':
        logger.warning("API ключ Census Bureau не настроен!")
    
    if not api_keys.get('google_maps') or api_keys.get('google_maps') == 'YOUR_GOOGLE_MAPS_API_KEY_HERE':
        logger.warning("API ключ Google Maps не настроен!")
    
    # Создание необходимых директорий
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    logger.info("\nПайплайн включает следующие этапы:")
    logger.info("1. Веб-скрапинг Crexi")
    logger.info("2. Веб-скрапинг LoopNet")
    logger.info("3. Сбор данных через Census Bureau API")
    logger.info("4. Сбор данных через Google Maps API")
    logger.info("5. Объединение всех данных")
    logger.info("\nДля запуска отдельных этапов используйте соответствующие скрипты в папке scripts/")
    logger.info("\nПримеры:")
    logger.info("  python scripts/scrape_crexi.py")
    logger.info("  python scripts/scrape_loopnet.py")
    logger.info("  python scripts/api_census.py")
    logger.info("  python scripts/api_google_maps.py")
    logger.info("  python scripts/combine_data.py")
    
    logger.info("\n" + "=" * 60)
    logger.info("Пайплайн готов к использованию!")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()

