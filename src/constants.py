import os
import logging

# ========== КОНСТАНТЫ ==========

LOG_LEVEL = logging.INFO
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES_DIR = os.path.join(BASE_DIR, 'files')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# Названия файлов
LOG_FILE = 'books_parser.log'
CSV_ALL_BOOKS = 'all_books.csv'
CSV_CLEANED_BOOKS = 'books_cleaned.csv'
CSV_FILTERED_BOOKS = 'filtered_books.csv'
CSV_BOOK_STATS = 'book_statistics.csv'
CSV_DATA_QUALITY_REPORT = 'data_quality_report.csv'
CSV_DEFAULT = 'books.csv'  # для функции по умолчанию


# Локальные файлы (с путями)
LOCAL_LOG_FILE = os.path.join(LOGS_DIR, LOG_FILE)
LOCAL_CSV_ALL_BOOKS = os.path.join(FILES_DIR, CSV_ALL_BOOKS)
LOCAL_CSV_CLEANED_BOOKS = os.path.join(FILES_DIR, CSV_CLEANED_BOOKS)
LOCAL_CSV_FILTERED_BOOKS = os.path.join(FILES_DIR, CSV_FILTERED_BOOKS)
LOCAL_CSV_BOOK_STATS = os.path.join(FILES_DIR, CSV_BOOK_STATS)
LOCAL_CSV_DATA_QUALITY_REPORT = os.path.join(FILES_DIR,
                                             CSV_DATA_QUALITY_REPORT)
LOCAL_CSV_DEFAULT = os.path.join(FILES_DIR, CSV_DEFAULT)

# Количество потоков парсинга
MAX_WORKERS_VALUE = 20

# Настройки парсинга
BASE_URL = "books.toscrape.com"
MIN_DELAY = 1.0
MAX_DELAY = 3.0
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10


CUSTOM_CONFIG = {
    'headers': {
            'User-Agent': 'Mozilla/5.0 \
            (Windows NT 10.0; Win64; x64) \
            AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,\
            application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        },
    'min_delay': MIN_DELAY,
    'max_delay': MAX_DELAY,
    'max_retries': MAX_RETRIES,
    'timeout': REQUEST_TIMEOUT,
    'base_url': f"https://{BASE_URL}"
}

# Рейтинги
RATING_MAP = {
    'One': 1, 'Two': 2, 'Three': 3,
    'Four': 4, 'Five': 5, 'Zero': 0
}
DEFAULT_RATING = 0

# Фильтрация
MIN_RATING_FILTER = 4
MAX_PRICE_FILTER = 50.0

# Анализ данных
PRICE_OUTLIER_MULTIPLIER = 1.5

# Файлы
DEFAULT_ENCODING = 'utf-8-sig'
LOG_ENCODING = 'utf-8'

# S3
S3_REGION_DEFAULT = "ru-7"
S3_PREFIX_RAW = 'raw_data/'
S3_PREFIX_CLEANED = 'cleaned_data/'
S3_PREFIX_STATS = 'statistics/'
S3_PREFIX_FILTERED = 'filtered_data/'
S3_PREFIX_ANALYSIS = 'analysis_results/'

# Статусы
STOCK_IN_STOCK = 'In stock'

# Content Types
CONTENT_TYPE_CSV = 'text/csv; charset=utf-8'
CONTENT_TYPE_OCTET = 'application/octet-stream'

# Размеры файлов
KB_DIVIDER = 1024

# Лимиты для отображения
MAX_TITLE_LENGTH_PREVIEW = 40
MAX_TITLE_LENGTH_FILTERED = 35
