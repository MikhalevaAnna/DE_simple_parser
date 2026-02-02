"""
Парсер сайта books.toscrape.com с сохранением в S3 Selectel бакет.
"""
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from urllib.parse import urljoin
from typing import List, Dict, Optional, Tuple, Any
import os
from dotenv import load_dotenv

# Импортируем из созданных модулей
from src.constants import (CUSTOM_CONFIG, LOG_ENCODING,
                           RATING_MAP, DEFAULT_RATING,
                           LOCAL_CSV_DEFAULT, DEFAULT_ENCODING,
                           MAX_WORKERS_VALUE, BASE_URL,
                           LOCAL_CSV_ALL_BOOKS, CSV_ALL_BOOKS,
                           LOGS_DIR, FILES_DIR, BASE_DIR)
from src.s3_interaction import initialize_s3, save_data_to_s3
from src.data_analyze import analyze_and_save_stats, filter_and_save_books
from src.logger import setup_logging

load_dotenv()
logger = setup_logging()

# ========== КОНФИГУРАЦИЯ ==========


def get_scraper_config() -> Dict[str, Any]:
    """
    Возвращает кастомную конфигурацию для парсера с настройками запросов.

    Returns:
        Словарь с конфигурацией парсера
    """
    return CUSTOM_CONFIG


# ========== ОСНОВНЫЕ ФУНКЦИИ ПАРСЕРА ==========

def make_request(url: str,
                 config: Dict[str, Any],
                 session: requests.Session
                 ) -> Optional[requests.Response]:
    """
    Выполняет HTTP-запрос с обработкой ошибок и повторными попытками.

    Args:
        url: URL для запроса.
        config: Конфигурация парсера из get_scraper_config().
        session: Сессия requests для сохранения соединений.

    Returns:
        Response объект при успешном запросе,
        None при ошибках после всех попыток.
    """

    for attempt in range(config['max_retries'] + 1):
        try:
            logger.debug(f"Запрос к {url} (попытка {attempt + 1})")
            response = session.get(url, timeout=config['timeout'])

            if response.encoding is None or response.encoding == 'ISO-8859-1':
                response.encoding = LOG_ENCODING

            # Проверяем статус код
            if response.status_code == 200:
                return response
            elif 500 <= response.status_code < 600:
                logger.warning(f"Серверная ошибка {response.status_code} для {url}")
            else:
                logger.warning(f"Клиентская ошибка {response.status_code} для {url}")
                return None  # Не повторяем для клиентских ошибок

        except requests.exceptions.ConnectionError as e:
            logger.error(f"Ошибка соединения для {url}: {e}")

            # Детализируем ошибку соединения
            if "Name or service not known" in str(e):
                logger.error(f"DNS ошибка: не удалось разрешить имя хоста для {url}")
            elif "timed out" in str(e).lower():
                logger.error(f"Таймаут соединения для {url}")
            elif "refused" in str(e).lower():
                logger.error(f"Соединение отклонено для {url} (порт закрыт)")

        except requests.exceptions.Timeout as e:
            logger.error(f"Таймаут запроса для {url}: {e}")

        except requests.exceptions.TooManyRedirects as e:
            logger.error(f"Слишком много редиректов для {url}: {e}")
            return None  # Не повторяем

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP ошибка для {url}: {e}")

        except Exception as e:
            logger.error(f"Неожиданная ошибка запроса к {url}: {e}")

        # Задержка перед повторной попыткой
        if attempt < config['max_retries']:
            delay = config['min_delay'] * (2 ** attempt)  # Экспоненциальная задержка
            delay = min(delay, config['max_delay'] * 3)
            logger.info(f"Повторная попытка через {delay:.2f} секунд...")
            time.sleep(delay)

    logger.error(f"Не удалось выполнить запрос к {url} после всех попыток")
    return None


def parse_book_card(card: BeautifulSoup, base_url: str
                    ) -> Optional[Dict[str, Any]]:
    """
    Парсит информацию о книге из HTML-карточки на странице каталога.

    Args:
        card: BeautifulSoup объект карточки книги.
        base_url: Базовый URL сайта для формирования абсолютных ссылок.

    Returns:
        Словарь с базовой информацией о книге или None при ошибке:
        {
            'title': str - название книги,
            'price': float - цена в фунтах,
            'rating': int - рейтинг от 0 до 5,
            'stock': int - количество в наличии,
            'in_stock': bool - наличие в магазине,
            'url': str - URL детальной страницы
        }
    """
    try:
        book_info = {}

        # Извлекаем название
        title_elem = card.find('h3').find('a')
        book_info['title'] = title_elem.get('title',
                                            title_elem.text.strip())

        # Извлекаем цену
        price_elem = card.find('p', class_='price_color')
        price_text = price_elem.text.strip() if price_elem else ''
        price_match = re.search(r'[\d.]+', price_text)
        book_info['price'] = (float(price_match.group())
                              if price_match
                              else 0.0)

        # Извлекаем рейтинг
        rating_elem = card.find('p', class_='star-rating')
        rating_classes = (rating_elem.get('class', [])
                          if rating_elem
                          else [])

        for cls in rating_classes:
            if cls in RATING_MAP:
                book_info['rating'] = RATING_MAP[cls]
                break
        else:
            book_info['rating'] = DEFAULT_RATING

        # Ищем элемент с наличием книг
        stock_elem = card.find('p', class_='instock availability')

        if not stock_elem:
            # Альтернативный поиск для страницы карточки товара
            stock_elem = card.find('p', class_='availability')

        stock_text = stock_elem.text.strip() if stock_elem else ''

        # Пытаемся найти число в тексте (например In stock (19 available))
        stock_match = re.search(r'\((\d+)\s+available\)',
                                stock_text, re.IGNORECASE)

        if not stock_match:
            # Альтернативный поиск: просто ищем любое число
            stock_match = re.search(r'(\d+)', stock_text)

        book_info['stock'] = int(stock_match.group(1)) if stock_match else 0

        # Определяем, есть ли книга в наличии
        book_info['in_stock'] = bool(
            stock_elem and
            ('instock' in stock_elem.get('class', []) or
             'available' in stock_text.lower() or
             'in stock' in stock_text.lower())
        )

        # Извлекаем ссылку
        link_elem = card.find('h3').find('a')
        href = link_elem.get('href', '') if link_elem else ''

        # Обрабатываем относительные ссылки
        if href.startswith('../../../'):
            href = 'catalogue/' + href[9:]
        elif href.startswith('../../'):
            href = 'catalogue/' + href[6:]
        elif not href.startswith('catalogue/'):
            href = 'catalogue/' + href

        book_info['url'] = urljoin(base_url, href)

        return book_info

    except Exception as e:
        logger.error(f"Ошибка парсинга карточки книги: {e}")
        return None


def parse_book_detail_page(url: str, config: Dict[str, Any],
                           session: requests.Session
                           ) -> Optional[Dict[str, Any]]:
    """
    Парсит детальную информацию со страницы отдельной книги.

    Args:
        url: URL детальной страницы книги.
        config: Конфигурация парсера.
        session: Сессия requests.

    Returns:
        Словарь с детальной информацией о книге или None при ошибке.
        Включает все поля из parse_book_card() плюс:
        {
            'upc': str - универсальный код продукта,
            'product_type': str - тип продукта,
            'price_excl_tax': float - цена без налога,
            'price_incl_tax': float - цена с налогом,
            'tax': float - сумма налога,
            'category': str - категория книги,
            'image_url': str - URL обложки книги
        }
    """

    response = make_request(url, config, session)
    if not response:
        return None

    soup = BeautifulSoup(response.content, 'lxml')

    book_details = {}

    try:
        # Название книги
        title_elem = soup.find('div',
                               class_='product_main').find('h1')
        book_details['title'] = title_elem.text.strip() if title_elem else ''

        # Цена
        price_elem = soup.find('p', class_='price_color')
        price_text = price_elem.text.strip() if price_elem else ''
        price_match = re.search(r'[\d.]+', price_text)
        book_details['price'] = (float(price_match.group())
                                 if price_match
                                 else 0.0)

        # Наличие - для детальной страницы
        availability_elem = soup.find('p',
                                      class_='instock availability')

        if not availability_elem:
            # Альтернативный поиск
            availability_elem = soup.find('p', class_='availability')

        stock_text = (availability_elem.text.strip()
                      if availability_elem
                      else '')

        # Ищем количество в скобках, например: "In stock (19 available)"
        stock_match = re.search(r'\((\d+)\s+available\)',
                                stock_text, re.IGNORECASE)

        if not stock_match:
            # Ищем просто число в тексте
            stock_match = re.search(r'(\d+)', stock_text)

        book_details['stock'] = (int(stock_match.group(1))
                                 if stock_match
                                 else 0)
        book_details['in_stock'] = bool(
            availability_elem and
            ('instock' in availability_elem.get('class', []) or
             'available' in stock_text.lower() or
             'in stock' in stock_text.lower())
        )

        # Рейтинг
        rating_elem = soup.find('p', class_='star-rating')
        rating_classes = (rating_elem.get('class', [])
                          if rating_elem
                          else [])

        for cls in rating_classes:
            if cls in RATING_MAP:
                book_details['rating'] = RATING_MAP[cls]
                break
        else:
            book_details['rating'] = DEFAULT_RATING

        # UPC и другая информация из таблицы
        table = soup.find('table', class_='table table-striped')
        if table:
            for row in table.find_all('tr'):
                header = row.find('th')
                value = row.find('td')
                if header and value:
                    header_text = header.text.strip().lower()
                    value_text = value.text.strip()
                    if 'upc' in header_text:
                        book_details['upc'] = value_text
                    elif 'product type' in header_text:
                        book_details['product_type'] = value_text
                    elif 'price (excl. tax)' in header_text.lower():
                        tax_match = re.search(r'[\d.]+', value_text)
                        book_details['price_excl_tax'] = (
                            float(tax_match.group())
                            if tax_match
                            else 0.0)
                    elif 'price (incl. tax)' in header_text.lower():
                        tax_match = re.search(r'[\d.]+', value_text)
                        book_details['price_incl_tax'] = (
                            float(tax_match.group())
                            if tax_match
                            else 0.0)
                    elif 'tax' in header_text and 'number' not in header_text:
                        tax_match = re.search(r'[\d.]+', value_text)
                        book_details['tax'] = (
                            float(tax_match.group())
                            if tax_match
                            else 0.0)

        # Категория
        breadcrumb = soup.find('ul', class_='breadcrumb')
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if len(links) >= 3:
                book_details['category'] = links[2].text.strip()

        # URL изображения
        img_elem = (soup.find('div', class_='item active').find('img')
                    if soup.find('div',
                                 class_='item active')
                    else None)
        if img_elem:
            img_src = img_elem.get('src', '')
            if img_src:
                img_src = img_src.replace('../..', '')
                book_details['image_url'] = urljoin(config['base_url'],
                                                    img_src)

        # URL текущей страницы
        book_details['url'] = url

        return book_details

    except Exception as e:
        logger.error(f"Ошибка парсинга детальной страницы {url}: {e}")
        return None


def scrape_book_detail(url: str, config: Optional[Dict[str, Any]] = None,
                       session: Optional[requests.Session] = None
                       ) -> Dict[str, Any]:
    """
    Основная функция для парсинга детальной информации о книге.

    Args:
        url: URL книги (может быть относительным или абсолютным).
        config: Конфигурация парсера (создается если не передана).
        session: Сессия requests (создается если не передана).

    Returns:
        Словарь с информацией о книге (пустой при ошибке).
    """

    # Инициализация конфигурации и сессии
    if not config:
        config = get_scraper_config()

    if not session:
        session = requests.Session()
        session.headers.update(config['headers'])

    # Если URL не полный, формируем его
    if not url.startswith('http'):
        if url.startswith('catalogue/'):
            url = f"{config['base_url']}/{url}"
        else:
            url = f"{config['base_url']}/catalogue/{url}"

    logger.debug(f"Парсинг детальной страницы: {url}")

    # Используем функцию parse_book_detail_page, которую мы уже создали
    book_details = parse_book_detail_page(url, config, session)

    return book_details or {}


def scrape_page(url: str, config: Dict[str, Any],
                session: requests.Session
                ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Парсит одну страницу каталога книг.

    Args:
        url: URL страницы каталога.
        config: Конфигурация парсера.
        session: Сессия requests.

    Returns:
        Кортеж (books, next_url):
        - books: список словарей с информацией о книгах на странице
        - next_url: URL следующей страницы или None если это последняя страница

    Workflow:
        1. Загружает страницу через make_request()
        2. Находит все карточки книг
        3. Парсит каждую карточку через parse_book_card()
        4. Ищет ссылку на следующую страницу
    """

    response = make_request(url, config, session)
    if not response:
        return [], None

    soup = BeautifulSoup(response.content, 'lxml')
    books = []

    # Ищем все карточки книг
    book_cards = soup.find_all('article', class_='product_pod')

    if not book_cards:
        # Альтернативный поиск
        book_cards = soup.select('li.col-xs-6 article')

    # Парсим каждую карточку
    for card in book_cards:
        book_info = parse_book_card(card, config['base_url'])
        if book_info:
            books.append(book_info)

    # Ищем следующую страницу
    next_link = soup.find('li', class_='next')
    next_url = None

    if next_link:
        next_a = next_link.find('a')
        if next_a:
            next_href = next_a.get('href', '')
            if next_href:
                # Формируем полный URL следующей страницы
                if next_href.startswith('catalogue/'):
                    next_url = urljoin(config['base_url'], next_href)
                else:
                    # Если это относительная ссылка от текущей страницы
                    next_url = urljoin(url, next_href)

    return books, next_url


# ========== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ==========

def save_data_locally(books_data: List[Dict[str, Any]],
                      filename: str = LOCAL_CSV_DEFAULT
                      ) -> pd.DataFrame:
    """
    Сохраняет собранные данные в локальный CSV файл.

    Args:
        books_data: Список словарей с данными книг.
        filename: Путь к файлу для сохранения.

    Returns:
        DataFrame с сохраненными данными (пустой если данных нет).
    """
    if not books_data:
        logger.warning("Нет данных для сохранения")
        return pd.DataFrame()

    df = pd.DataFrame(books_data)
    df.to_csv(filename, index=False, encoding=DEFAULT_ENCODING)

    logger.info(f"Данные сохранены локально в {filename}")

    return df


def scrape_all_pages(config: Dict[str, Any], max_pages: Optional[int] = None,
                     get_detailed: bool = False
                     ) -> List[Dict[str, Any]]:
    """
    Парсит все страницы каталога книг.

    Args:
        config: Конфигурация парсера.
        max_pages: Максимальное количество страниц для парсинга (None = все).
        get_detailed: Если True - парсит детальную информацию для каждой книги.

    Returns:
        Список словарей с информацией о всех книгах.

    Workflow при get_detailed=True:
        1. Собирает все URL книг со всех страниц
        2. Запускает параллельный парсинг детальных страниц
        3. Объединяет базовую и детальную информацию

    Workflow при get_detailed=False:
        1. Последовательно парсит все страницы
        2. Собирает только базовую информацию
    """

    session = requests.Session()
    session.headers.update(config['headers'])

    books_data = []
    current_url = f"{config['base_url']}/catalogue/page-1.html"
    page_num = 1

    # Собираем все URL детальных страниц
    if get_detailed:
        all_books = []
        detail_urls = []

        # Сначала соберем все URL
        while current_url:
            if max_pages and page_num > max_pages:
                break

            books, next_url = scrape_page(current_url, config, session)
            all_books.extend(books)
            detail_urls.extend([book['url'] for book in books])

            logger.info(f"Страница {page_num}: {len(books)} книг")

            if next_url:
                current_url = next_url
                page_num += 1
                time.sleep(0.1)  # Минимальная задержка
            else:
                current_url = None

        # Параллельный парсинг детальных страниц
        logger.info(f"Параллельный парсинг "
                    f"{len(detail_urls)} детальных страниц...")

        # Используем словарь для быстрого доступа
        book_dict = {book['url']: book for book in all_books}

        # Увеличиваем количество потоков
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=MAX_WORKERS_VALUE) as executor:
            # Запускаем все задачи
            future_to_url = {
                executor.submit(scrape_book_detail,
                                url, config): url
                for url in detail_urls
            }

            # Обрабатываем результаты
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    detailed_info = future.result(timeout=10)
                    if detailed_info and url in book_dict:
                        book_dict[url].update(detailed_info)
                except Exception as e:
                    logger.error(f"Ошибка для {url}: {e}")

        books_data = list(book_dict.values())
    else:
        # Базовый парсинг без деталей
        while current_url:
            if max_pages and page_num > max_pages:
                break

            books, next_url = scrape_page(current_url, config, session)
            books_data.extend(books)

            logger.info(f"Страница {page_num}: {len(books)} книг")

            if next_url:
                current_url = next_url
                page_num += 1
                time.sleep(0.1)  # Минимальная задержка
            else:
                current_url = None

    logger.info(f"Парсинг завершен. Всего книг: {len(books_data)}")
    return books_data


def full_scrape() -> None:
    """
    Выполняет полный цикл парсинга с сохранением и анализом данных.

    Workflow:
        1. Инициализирует подключение к S3
        2. Проверяет доступность сайта
        3. Парсит все книги с детальной информацией
        4. Сохраняет данные локально
        5. Загружает данные в S3
        6. Выполняет анализ данных
        7. Фильтрует и сохраняет результаты
    """
    logger.info("Запуск полного парсинга с сохранением в S3...")

    # Инициализируем S3
    s3_client, s3_config = initialize_s3()
    scraper_config = get_scraper_config()

    # Проверяем подключение к сайту
    test_url = f"https://{BASE_URL}/catalogue/page-1.html"
    session = requests.Session()
    session.headers.update(scraper_config['headers'])

    response = make_request(test_url, scraper_config, session)
    if not response:
        logger.error("❌ Не удалось подключиться к сайту")
        return

    get_detailed = True  # По умолчанию получаем детальную информацию

    # Запускаем парсинг с выбранными настройками
    books_data = scrape_all_pages(scraper_config, get_detailed=get_detailed)

    if books_data:
        logger.info(f"Успешно собрано {len(books_data)} книг")

        # Сохраняем локально
        filename = LOCAL_CSV_ALL_BOOKS
        if get_detailed:
            filename = LOCAL_CSV_ALL_BOOKS.replace('.csv', '_detailed.csv')

        df = save_data_locally(books_data, filename)

        # Сохраняем в S3
        if s3_client and s3_config:
            s3_filename = CSV_ALL_BOOKS
            if get_detailed:
                s3_filename = CSV_ALL_BOOKS.replace('.csv', '_detailed.csv')

            s3_key = save_data_to_s3(s3_client,
                                     books_data,
                                     s3_config['bucket_name'],
                                     s3_filename)

            if s3_key:
                # Анализируем и сохраняем статистику
                analyze_and_save_stats(df, s3_client, s3_config['bucket_name'])

                # Фильтруем и сохраняем
                filter_and_save_books(df,
                                      s3_client=s3_client,
                                      bucket_name=s3_config['bucket_name'])

                logger.info("Парсинг завершен успешно!")
                logger.info("Анализ полученных данных проведен успешно!")
                logger.info("Данные успешно сохранены в S3!")
                logger.info(f"Собрано книг: {len(books_data)}")
            else:
                logger.info("Данные не были сохранены в S3, "
                            "но парсинг прошел успешно")
    else:
        logger.error("❌ Не удалось собрать данные")


def main() -> None:
    """
    Точка входа в приложение. Выполняет настройку и запуск парсера.

    Workflow:
        1. Создает необходимые директории
        2. Настраивает логирование
        3. Выводит информацию о проекте и требованиях
        4. Запускает полный парсинг через full_scrape()
    """
    # Настройка логирования перед началом работы
    os.makedirs(LOGS_DIR, exist_ok=True)
    os.makedirs(FILES_DIR, exist_ok=True)

    endpoint = os.getenv('SELECTEL_ENDPOINT')
    logger.info("=" * 60)
    logger.info(f"📚 ПАРСЕР КНИГ - {BASE_URL}")
    logger.info("🔄 Сохранение в S3 Selectel")
    logger.info("=" * 60)

    logger.info("\n📁 Структура проекта:")
    logger.info(f"  📂 {BASE_DIR}/")
    logger.info(f"    ├── 📁 {os.path.basename(FILES_DIR)}/    "
                f"# CSV файлы с данными")
    logger.info(f"    ├── 📁 {os.path.basename(LOGS_DIR)}/     "
                f"# Лог файлы")
    logger.info("    ├── 📁 src/      # Исходный код")
    logger.info(f"    └── 📄 {os.path.basename(__file__)}  "
                f"# Основной скрипт")

    logger.info("\n📦 Требуемые зависимости:")
    logger.info("  pip install -r requirements.txt")
    logger.info("\n🔑 Настройка файла .env (пример):")
    logger.info("  SELECTEL_ACCESS_KEY=ваш_access_key")
    logger.info("  SELECTEL_SECRET_KEY=ваш_secret_key")
    logger.info("  SELECTEL_BUCKET=de-books")
    logger.info(f"  SELECTEL_ENDPOINT={endpoint}")
    full_scrape()


if __name__ == "__main__":
    main()
