"""
Logger configuration module
Настройка логирования через config.yaml
"""
import logging
import os
import yaml
from logging.handlers import RotatingFileHandler


def setup_logger(config_path=None):
    """
    Настройка логгера на основе конфигурационного файла
    
    Args:
        config_path: путь к файлу конфигурации
        
    Returns:
        logger: настроенный объект логгера
    """
    # Загрузка конфигурации
    if config_path is None:
        from pathlib import Path
        config_path = Path(__file__).parent.parent / 'config.yaml'
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    log_config = config.get('logging', {})
    
    # Проверка, включено ли логирование
    if not log_config.get('enabled', True):
        logging.getLogger().disabled = True
        return logging.getLogger()
    
    # Создание директории для логов, если её нет
    log_file = log_config.get('log_file', 'logs/project.log')
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Настройка логгера
    logger = logging.getLogger('commercial_real_estate')
    logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
    
    # Очистка существующих обработчиков
    logger.handlers.clear()
    
    # Формат логирования
    formatter = logging.Formatter(log_config.get('format', 
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    # Обработчик для файла
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Обработчик для консоли
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

