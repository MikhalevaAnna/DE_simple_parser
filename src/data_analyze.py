"""
Функции для анализа данных и генерации отчетов.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Any
from src.constants import (LOCAL_CSV_CLEANED_BOOKS, DEFAULT_ENCODING,
                           S3_PREFIX_CLEANED, CSV_CLEANED_BOOKS,
                           BASE_URL, PRICE_OUTLIER_MULTIPLIER,
                           LOCAL_CSV_BOOK_STATS, S3_PREFIX_STATS,
                           CSV_BOOK_STATS, LOCAL_CSV_DATA_QUALITY_REPORT,
                           S3_PREFIX_ANALYSIS, CSV_DATA_QUALITY_REPORT,
                           MIN_RATING_FILTER, LOCAL_CSV_FILTERED_BOOKS,
                           S3_PREFIX_FILTERED, CSV_FILTERED_BOOKS,
                           MAX_PRICE_FILTER)
from src.s3_interaction import upload_df_to_s3, upload_file_to_s3

logger = logging.getLogger(__name__)


def analyze_and_save_stats(df: pd.DataFrame,
                           s3_client: Optional[Any] = None,
                           bucket_name: Optional[str] = None
                           ) -> Dict[str, Any]:
    """
    Выполняет комплексный анализ данных книг и сохраняет результаты.

    Args:
        df: DataFrame с данными книг.
        s3_client: Клиент S3 для сохранения результатов (опционально).
        bucket_name: Имя бакета для сохранения (опционально).

    Returns:
        Словарь с результатами анализа:
        {
            'total_books': int,
            'missing_values': int,
            'total_duplicates': int,
            'cleaned_records': int,
            'removed_duplicates': int,
            'price_statistics': Dict,
            'rating_distribution': Dict,
            'in_stock': Dict,
            'stock_statistics': Dict,
            'price_outliers': int
        }

    Workflow:
        1. Анализ пропущенных значений
        2. Удаление дубликатов
        3. Статистический анализ цен
        4. Анализ распределения рейтингов
        5. Анализ наличия книг
        6. Сохранение агрегированных данных
        7. Генерация отчета о качестве данных
    """

    if df.empty:
        logger.warning("Нет данных для анализа")
        return {}

    results = {}

    # Основная статистика
    results['total_books'] = len(df)
    logger.info(f"Всего книг: {results['total_books']}")

    # 1. Проверка на пропущенные значения
    missing = df.isnull().sum()
    missing_records = 0

    for col, count in missing.items():
        if count > 0:
            logger.warning(f"  {col}: {count} пропущенных значений")
            missing_records += count

    results['missing_values'] = int(missing_records)

    # 2. Проверка на дубликаты
    total_duplicates = df.duplicated().sum()
    results['total_duplicates'] = int(total_duplicates)

    # Если есть дубликаты, удаляем их
    if total_duplicates > 0:
        # Сохраняем исходное количество записей
        original_count = len(df)
        # Удаляем полные дубликаты
        df_no_duplicates = df.drop_duplicates()

        removed_count = original_count - len(df_no_duplicates)
        logger.info(f"  Удалено {removed_count} дубликатов")
        logger.info(f"  Осталось {len(df_no_duplicates)} уникальных записей")

        # Сохраняем очищенные данные
        if removed_count > 0:
            # Сохраняем локально (используем константы из constants.py)

            df_no_duplicates.to_csv(LOCAL_CSV_CLEANED_BOOKS,
                                    index=False,
                                    encoding=DEFAULT_ENCODING)

            # Сохраняем в S3
            if s3_client and bucket_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                s3_key = f"{S3_PREFIX_CLEANED}{timestamp}_{CSV_CLEANED_BOOKS}"

                metadata = {
                    'source': BASE_URL,
                    'records_count': str(len(df_no_duplicates)),
                    'duplicates_removed': str(removed_count),
                    'cleaned_at': datetime.now().isoformat()
                }

                upload_df_to_s3(s3_client,
                                df_no_duplicates,
                                bucket_name, s3_key,
                                metadata)

        # Заменяем исходный DataFrame на очищенный для дальнейшего анализа
        df = df_no_duplicates
        results['cleaned_records'] = len(df)
        results['removed_duplicates'] = removed_count

    # Статистика по ценам
    if 'price' in df.columns:
        price_stats = {
            'mean': df['price'].mean(),
            'median': df['price'].median(),
            'min': df['price'].min(),
            'max': df['price'].max(),
            'std': df['price'].std()
        }

        # Проверка аномальных цен
        price_q1 = df['price'].quantile(0.25)
        price_q3 = df['price'].quantile(0.75)
        price_iqr = price_q3 - price_q1
        price_lower_bound = price_q1 - PRICE_OUTLIER_MULTIPLIER * price_iqr
        price_upper_bound = price_q3 + PRICE_OUTLIER_MULTIPLIER * price_iqr

        price_outliers = df[(df['price'] < price_lower_bound)
                            | (df['price'] > price_upper_bound)]
        if len(price_outliers) > 0:
            results['price_outliers'] = len(price_outliers)

        results['price_statistics'] = {k: float(v) for k, v
                                       in price_stats.items()}

    # Статистика по рейтингам
    if 'rating' in df.columns:
        rating_counts = df['rating'].value_counts().sort_index()
        rating_distribution = {}

        for rating, count in rating_counts.items():
            percentage = (count / len(df)) * 100
            rating_distribution[int(rating)] = {
                'count': int(count),
                'percentage': float(percentage)
            }

        results['rating_distribution'] = rating_distribution

    # Статистика по наличию
    if 'in_stock' in df.columns:
        in_stock = df['in_stock'].sum()
        percentage = (in_stock / len(df)) * 100
        results['in_stock'] = {
            'count': int(in_stock),
            'percentage': float(percentage)
        }
    if 'stock' in df.columns:
        total_stock = df['stock'].sum()
        avg_stock = df['stock'].mean()
        results['stock_statistics'] = {
            'total': int(total_stock),
            'average': float(avg_stock)
        }

    # Сохраняем агрегированные данные по рейтингам
    if 'rating' in df.columns and 'price' in df.columns:
        agg_data = df.groupby('rating').agg({
            'price': ['mean', 'median', 'min', 'max', 'count', 'std'],
            'in_stock': 'sum',
            'stock': 'sum'
        }).round(2)

        # Переименовываем колонки для лучшей читаемости
        agg_data.columns = ['price_mean', 'price_median', 'price_min',
                            'price_max', 'count', 'price_std',
                            'in_stock_sum', 'stock_sum']

        # Сохраняем локально
        agg_data.to_csv(LOCAL_CSV_BOOK_STATS,
                        encoding=DEFAULT_ENCODING)

        # Сохраняем в S3
        if s3_client and bucket_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{S3_PREFIX_STATS}{timestamp}_{CSV_BOOK_STATS}"

            metadata = {
                'source': BASE_URL,
                'analysis_type': 'aggregated_statistics',
                'analyzed_at': datetime.now().isoformat(),
                'unique_records': str(len(df))
            }

            upload_file_to_s3(s3_client, LOCAL_CSV_BOOK_STATS,
                              bucket_name, s3_key, metadata)

    # Создаем DataFrame с результатами анализа
    analysis_data = []

    # Общая информация
    analysis_data.append({
        'category': 'Общая информация',
        'metric': 'Всего записей',
        'value': results['total_books']
    })

    if 'cleaned_records' in results:
        analysis_data.append({
            'category': 'Общая информация',
            'metric': 'Уникальных записей после очистки',
            'value': results['cleaned_records']
        })

    # Качество данных
    analysis_data.append({
        'category': 'Качество данных',
        'metric': 'Пропущенные значения',
        'value': results.get('missing_values', 0)
    })

    if 'total_duplicates' in results:
        analysis_data.append({
            'category': 'Качество данных',
            'metric': 'Полных дубликатов',
            'value': results['total_duplicates']
        })

    if 'removed_duplicates' in results:
        analysis_data.append({
            'category': 'Качество данных',
            'metric': 'Удалено дубликатов',
            'value': results['removed_duplicates']
        })

    if 'price_outliers' in results:
        analysis_data.append({
            'category': 'Качество данных',
            'metric': 'Выбросы по цене',
            'value': results['price_outliers']
        })

    # Статистика по ценам
    if 'price_statistics' in results:
        price_stats = results['price_statistics']
        analysis_data.append({
            'category': 'Статистика по ценам',
            'metric': 'Средняя цена (£)',
            'value': f"{price_stats['mean']:.2f}"
        })
        analysis_data.append({
            'category': 'Статистика по ценам',
            'metric': 'Минимальная цена (£)',
            'value': f"{price_stats['min']:.2f}"
        })
        analysis_data.append({
            'category': 'Статистика по ценам',
            'metric': 'Максимальная цена (£)',
            'value': f"{price_stats['max']:.2f}"
        })

    # Статистика по наличию
    if 'in_stock' in results:
        in_stock_stats = results['in_stock']
        analysis_data.append({
            'category': 'Статистика по наличию',
            'metric': 'Книг в наличии',
            'value': f"{in_stock_stats['count']}"
        })

    if 'stock_statistics' in results:
        stock_stats = results['stock_statistics']
        analysis_data.append({
            'category': 'Статистика по наличию',
            'metric': 'Всего экземпляров в наличии',
            'value': stock_stats['total']
        })
        analysis_data.append({
            'category': 'Статистика по наличию',
            'metric': 'Среднее количество на книгу',
            'value': f"{stock_stats['average']:.1f}"
        })

    # Статистика по рейтингам
    if 'rating_distribution' in results:
        rating_stats = results['rating_distribution']
        for rating, stats in rating_stats.items():
            analysis_data.append({
                'category': 'Распределение по рейтингам',
                'metric': f'Рейтинг {rating}',
                'value': f"{stats['count']} книг ({stats['percentage']:.1f}%)"
            })

    # Создаем DataFrame и сохраняем в CSV
    analysis_df = pd.DataFrame(analysis_data)

    analysis_df.to_csv(LOCAL_CSV_DATA_QUALITY_REPORT,
                       index=False,
                       encoding=DEFAULT_ENCODING)

    # Сохраняем в S3
    if s3_client and bucket_name:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_PREFIX_ANALYSIS}{timestamp}_{CSV_DATA_QUALITY_REPORT}"

        metadata = {
            'source': BASE_URL,
            'analysis_type': CSV_DATA_QUALITY_REPORT.split('.')[0],
            'analyzed_at': datetime.now().isoformat(),
            'total_records': str(results['total_books'])
        }

        if 'cleaned_records' in results:
            metadata['unique_records'] = str(results['cleaned_records'])

        upload_file_to_s3(s3_client,
                          LOCAL_CSV_DATA_QUALITY_REPORT,
                          bucket_name, s3_key, metadata)

    return results


def filter_and_save_books(df: pd.DataFrame,
                          min_rating: int = MIN_RATING_FILTER,
                          max_price: float = MAX_PRICE_FILTER,
                          s3_client: Optional[Any] = None,
                          bucket_name: Optional[str] = None) -> pd.DataFrame:
    """
    Фильтрует книги по рейтингу и цене с сохранением результатов.

    Args:
        df: DataFrame с данными книг.
        min_rating: Минимальный рейтинг для фильтрации.
        max_price: Максимальная цена для фильтрации.
        min_rating, max_price - (по умолчанию из констант).
        s3_client: Клиент S3 для сохранения результатов (опционально).
        bucket_name: Имя бакета для сохранения (опционально).

    Returns:
        Отфильтрованный DataFrame.

    Note:
        Сохраняет результаты локально в CSV и при необходимости в S3.
        Имя файла в S3 включает timestamp и параметры фильтрации.
    """

    # Применяем фильтры
    filtered = df.copy()

    if 'rating' in filtered.columns:
        filtered = filtered[filtered['rating'] >= min_rating]

    if 'price' in filtered.columns:
        filtered = filtered[filtered['price'] <= max_price]

    logger.info(f"Фильтрация: рейтинг >= {min_rating}, цена <= £{max_price}")
    logger.info(f"Результат: {len(filtered)} из {len(df)} книг")

    if len(filtered) > 0:
        # Сохраняем локально
        filtered.to_csv(LOCAL_CSV_FILTERED_BOOKS,
                        index=False,
                        encoding=DEFAULT_ENCODING)

        # Сохраняем в S3
        if s3_client and bucket_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_key = f"{S3_PREFIX_FILTERED}{timestamp}_{CSV_FILTERED_BOOKS}"

            metadata = {
                'source': BASE_URL,
                'records_count': str(len(filtered)),
                'filters': f'rating>={min_rating}, price<={max_price}',
                'filtered_at': datetime.now().isoformat()
            }

            upload_file_to_s3(s3_client, LOCAL_CSV_FILTERED_BOOKS,
                              bucket_name, s3_key, metadata)

    return filtered
