"""
Utility functions for data processing
"""
import re
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional


FIPS_TO_STATE = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY"
}


def clean_text(text: str) -> str:
    """
    Очистка текста от мусора с использованием регулярных выражений
    
    Args:
        text: исходный текст
        
    Returns:
        очищенный текст
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    
    # Удаление лишних пробелов
    text = re.sub(r'\s+', ' ', text)
    
    # Удаление специальных символов, но сохранение букв, цифр и основных знаков препинания
    text = re.sub(r'[^\w\s.,!?;:()\-\'"]', '', text)
    
    # Удаление множественных пробелов
    text = re.sub(r' +', ' ', text)
    
    # Удаление пробелов в начале и конце
    text = text.strip()
    
    return text


def parse_price(price_str: str) -> float:
    """
    Парсинг цены из строки
    
    Args:
        price_str: строка с ценой
        
    Returns:
        цена в виде числа
    """
    if pd.isna(price_str) or price_str is None:
        return np.nan
    
    price_str = str(price_str)
    
    # Удаление символов валюты и пробелов
    price_str = re.sub(r'[^\d.,]', '', price_str)
    
    # Замена запятой на точку
    price_str = price_str.replace(',', '')
    
    try:
        return float(price_str)
    except ValueError:
        return np.nan


def parse_square_feet(sqft_str: str) -> float:
    """
    Парсинг площади из строки
    
    Args:
        sqft_str: строка с площадью
        
    Returns:
        площадь в квадратных футах
    """
    if pd.isna(sqft_str) or sqft_str is None:
        return np.nan
    
    sqft_str = str(sqft_str)
    
    # Извлечение числа
    match = re.search(r'([\d,]+)', sqft_str)
    if match:
        num_str = match.group(1).replace(',', '')
        try:
            return float(num_str)
        except ValueError:
            return np.nan
    
    return np.nan


def extract_city_state(location: str) -> tuple:
    """
    Извлечение города и штата из строки локации
    
    Args:
        location: строка с локацией
        
    Returns:
        кортеж (город, штат)
    """
    if pd.isna(location) or location is None:
        return (None, None)
    
    location = str(location).strip()
    
    # Паттерн для извлечения города и штата
    match = re.search(r'([^,]+),\s*([A-Z]{2})', location)
    if match:
        city = match.group(1).strip()
        state = match.group(2).strip()
        return (city, state)
    
    return (None, None)


def fips_to_state_abbr(fips_code: Optional[str]) -> Optional[str]:
    """
    Конвертация двухзначного FIPS-кода штата в почтовую аббревиатуру.

    Args:
        fips_code: строка или число с кодом FIPS

    Returns:
        Почтовая аббревиатура штата (например, 'NY') или None, если код не найден.
    """
    if fips_code is None:
        return None

    if isinstance(fips_code, (int, float)):
        if pd.isna(fips_code):
            return None
        fips_str = f"{int(fips_code):02d}"
    else:
        fips_str = str(fips_code).strip()
        if len(fips_str) == 1:
            fips_str = f"0{fips_str}"

    return FIPS_TO_STATE.get(fips_str)

