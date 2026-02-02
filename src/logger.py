"""
Настройка логирования для всего приложения.
"""

import logging
import sys
from src.constants import LOCAL_LOG_FILE, LOG_ENCODING, LOG_LEVEL


def setup_logging():
    """
    Настраивает систему логирования для всего приложения.

    Returns:
        Настроенный логгер с именем 'books_parser'.

    Configuration:
        - Уровень логирования: LOG_LEVEL из констант
        - Формат: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        - Обработчики: файл (LOCAL_LOG_FILE) и консоль
        - Кодировка: LOG_ENCODING из констант

    Note:
        Настраивает корневой логгер, поэтому все дочерние логгеры
        автоматически получат эту конфигурацию.
    """
    # Создаем форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Обработчик для файла
    file_handler = logging.FileHandler(LOCAL_LOG_FILE, encoding=LOG_ENCODING)
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(formatter)

    # Обработчик для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(formatter)

    # Настраиваем корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)

    # Удаляем существующие обработчики
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Добавляем наши обработчики
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Настраиваем логгер для нашего приложения
    app_logger = logging.getLogger('books_parser')
    app_logger.setLevel(LOG_LEVEL)

    return app_logger
