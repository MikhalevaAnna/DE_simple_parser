"""
Функции для работы с S3 Selectel.
"""

import io
import re
import logging
import boto3
import botocore
from botocore.exceptions import ClientError
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
import pandas as pd
import os
from src.constants import (S3_REGION_DEFAULT, MAX_RETRIES,
                           REQUEST_TIMEOUT, DEFAULT_ENCODING,
                           CONTENT_TYPE_CSV, CONTENT_TYPE_OCTET,
                           S3_PREFIX_RAW, BASE_URL)


logger = logging.getLogger(__name__)


def get_s3_config() -> Optional[Dict[str, str]]:
    """
    Получает конфигурацию для подключения к
    S3 Selectel из переменных окружения.

    Returns:
        Словарь с конфигурацией S3 или None если учетные данные не найдены.
    """
    access_key = os.getenv('SELECTEL_ACCESS_KEY')
    secret_key = os.getenv('SELECTEL_SECRET_KEY')
    bucket_name = os.getenv('SELECTEL_BUCKET')
    endpoint = os.getenv('SELECTEL_ENDPOINT')

    if not access_key or not secret_key:
        logger.error("❌ Не найдены учетные данные S3!")
        logger.error("Установите переменные окружения:")
        logger.error("  SELECTEL_ACCESS_KEY=ваш_ключ")
        logger.error("  SELECTEL_SECRET_KEY=ваш_секрет")
        return None

    # Определяем регион из endpoint
    region = S3_REGION_DEFAULT  # По умолчанию для вашего endpoint

    # Пытаемся извлечь регион из endpoint
    if endpoint and 'ru-' in endpoint:
        match = re.search(r'ru-(\d+)', endpoint)
        if match:
            region = f"ru-{match.group(1)}"

    return {
        'endpoint_url': endpoint,
        'bucket_name': bucket_name,
        'access_key': access_key,
        'secret_key': secret_key,
        'region': region
    }


def create_s3_client(s3_config: Dict[str, str]) -> Optional[Any]:
    """
    Создает клиент boto3 для работы с S3 Selectel.

    Args:
        s3_config: Конфигурация S3 из функции get_s3_config().

    Returns:
        Объект клиента S3 boto3 или None в случае ошибки.
    """
    try:
        session = boto3.Session(
            aws_access_key_id=s3_config['access_key'],
            aws_secret_access_key=s3_config['secret_key'],
            region_name=s3_config['region']
        )

        s3_client = session.client(
            's3',
            endpoint_url=s3_config['endpoint_url'],
            config=botocore.client.Config(
                s3={'addressing_style': 'virtual'},
                max_pool_connections=50,
                retries={'max_attempts': MAX_RETRIES},
                connect_timeout=REQUEST_TIMEOUT,
                read_timeout=30,
                signature_version='s3v4'
            )
        )

        logger.info("✅ S3 клиент создан")
        return s3_client

    except Exception as e:
        logger.error(f"❌ Ошибка создания S3 клиента: {e}")
        return None


def check_s3_bucket(s3_client: Any, bucket_name: str) -> bool:
    """
    Проверяет существование и доступность S3 бакета.

    Args:
        s3_client: Клиент S3 boto3.
        bucket_name: Имя бакета для проверки.

    Returns:
        True если бакет существует и доступен, False в противном случае.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"✅ Бакет '{bucket_name}' существует")
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == '404':
            logger.warning(f"⚠️  Бакет '{bucket_name}' не найден")
            return False
        elif error_code in ['400', '403', 'InvalidBucketName']:
            logger.warning(f"⚠️  Нет доступа к бакету '{bucket_name}'"
                           f" или он не существует")
            return False
        else:
            logger.error(f"❌ Ошибка при проверке бакета: {e}")
            return False
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при проверке "
                     f"бакета: {e}")
        return False


def create_s3_bucket(s3_client: Any,
                     bucket_name: str,
                     region: str = S3_REGION_DEFAULT) -> bool:
    """
    Создает новый бакет в S3 Selectel с указанным регионом.

    Args:
        s3_client: Клиент S3 boto3.
        bucket_name: Имя создаваемого бакета.
        region: Регион для создания бакета (по умолчанию из констант).

    Returns:
        True если бакет создан или уже существует, False при ошибке.
    """
    try:
        # Для Selectel создаем бакет с указанием региона
        create_bucket_config = {
            'LocationConstraint': region
        }

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=create_bucket_config
        )

        logger.info(f"✅ Бакет '{bucket_name}' создан в регионе {region}")
        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == 'BucketAlreadyOwnedByYou':
            logger.info(f"✅ Бакет '{bucket_name}' уже существует")
            return True
        elif error_code == 'BucketAlreadyExists':
            logger.error(f"❌ Бакет '{bucket_name}' уже существует "
                         f"у другого пользователя")
            return False
        else:
            logger.error(f"❌ Ошибка при создании бакета: {e}")
            return False


def list_available_buckets(s3_client: Any,
                           region: str = S3_REGION_DEFAULT) -> List[str]:
    """
    Получает список всех доступных бакетов у текущего пользователя.

    Args:
        s3_client: Клиент S3 boto3.
        region: Регион для отображения в логах.

    Returns:
        Список имен доступных бакетов.
    """
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        if buckets:
            logger.info(f"📁 Доступные бакеты (регион {region}):")
            for bucket in buckets:
                logger.info(f"  - {bucket}")
        else:
            logger.info("📁 Нет доступных бакетов")

        return buckets

    except ClientError as e:
        error_code = e.response['Error']['Code']

        if (error_code == 'AuthorizationHeaderMalformed'
                and 'AuthorizationHeaderMalformed' in str(e)
                or 'region' in str(e).lower()):
            logger.error(f"❌ Ошибка региона: {e}")
            logger.error("Проверьте правильность региона в настройках")
        else:
            logger.error(f"❌ Ошибка получения списка бакетов: {e}")
        return []


def upload_df_to_s3(s3_client: Any, df: pd.DataFrame,
                    bucket_name: str,
                    s3_key: str,
                    metadata: Optional[Dict[str, str]] = None) -> bool:
    """
    Загружает DataFrame Pandas в S3 как CSV файл.

    Args:
        s3_client: Клиент S3 boto3.
        df: DataFrame для загрузки.
        bucket_name: Имя целевого бакета.
        s3_key: Ключ (путь) для файла в S3.
        metadata: Дополнительные метаданные для файла.

    Returns:
        True при успешной загрузке, False при ошибке.
    """
    try:
        # Конвертируем DataFrame в CSV в памяти
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding=DEFAULT_ENCODING)

        # Подготовка метаданных
        if metadata is None:
            metadata = {}

        # Загружаем в S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType=CONTENT_TYPE_CSV,
            Metadata=metadata
        )

        logger.info(f"✅ Файл загружен в S3: "
                    f"s3://{bucket_name}/{s3_key}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки в S3: {e}")
        return False


def upload_file_to_s3(s3_client: Any,
                      local_path: str,
                      bucket_name: str,
                      s3_key: str,
                      metadata: Optional[Dict[str, str]] = None) -> bool:
    """
        Загружает локальный файл в S3.

    Args:
        s3_client: Клиент S3 boto3.
        local_path: Путь к локальному файлу.
        bucket_name: Имя целевого бакета.
        s3_key: Ключ (путь) для файла в S3.
        metadata: Дополнительные метаданные для файла.

    Returns:
        True при успешной загрузке, False при ошибке.
    """
    try:
        # Определяем Content-Type
        ext = os.path.splitext(local_path)[1].lower()
        content_types = {
            '.csv': 'text/csv'
        }
        content_type = content_types.get(ext, CONTENT_TYPE_OCTET)

        with open(local_path, 'rb') as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=f,
                ContentType=content_type,
                Metadata=metadata or {}
            )

        logger.info(f"✅ Файл '{local_path}' загружен в S3: {s3_key}")
        return True

    except Exception as e:
        logger.error(f"❌ Ошибка загрузки файла в S3: {e}")
        return False


def list_s3_files(s3_client: Any,
                  bucket_name: str,
                  prefix: str = "") -> List[Dict[str, Any]]:
    """
    Получает список файлов в S3 бакете с возможностью фильтрации по префиксу.

    Args:
        s3_client: Клиент S3 boto3.
        bucket_name: Имя бакета.
        prefix: Префикс для фильтрации файлов (опционально).

    Returns:
        Список словарей с информацией о файлах:
        [
            {
                'key': 'путь/к/файлу.csv',
                'size': 1024,  # в байтах
                'last_modified': datetime.datetime
            }
        ]
    """
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )

        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })

        return files

    except Exception as e:
        logger.error(f"❌ Ошибка получения списка файлов: {e}")
        return []


def initialize_s3() -> Tuple[Optional[Any], Optional[Dict[str, str]]]:
    """
    Инициализирует подключение к S3 Selectel с проверкой и созданием бакета.

    Returns:
        Кортеж (s3_client, s3_config) или (None, None) при ошибке.
    """
    s3_config = get_s3_config()

    if not s3_config:
        return None, None

    s3_client = create_s3_client(s3_config)
    if not s3_client:
        logger.error("❌ Конфигурация S3 не найдена")
        return None, None

    # Проверяем наш бакет
    bucket_name = s3_config['bucket_name']
    bucket_exists = check_s3_bucket(s3_client, bucket_name)

    if not bucket_exists:
        if not create_s3_bucket(s3_client, bucket_name, s3_config['region']):
            logger.error(f"❌ Не удалось создать бакет '{bucket_name}'")
            return None, None

    return s3_client, s3_config


def save_data_to_s3(s3_client: Any, books_data: List[Dict[str, Any]],
                    bucket_name: str, s3_filename: str) -> Optional[str]:
    """
    Сохраняет собранные данные книг в S3 с автоматическим именованием файла.

    Args:
        s3_client: Клиент S3 boto3.
        books_data: Список словарей с данными книг.
        bucket_name: Имя целевого бакета.
        s3_filename: Базовое имя файла.

    Returns:
        S3 ключ загруженного файла или None при ошибке.
    """

    if not books_data:
        logger.warning("Нет данных для сохранения в S3")
        return None

    df = pd.DataFrame(books_data)

    # Создаем уникальное имя файла с timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{S3_PREFIX_RAW}{timestamp}_{s3_filename}"

    # Метаданные для файла
    metadata = {
        'source': BASE_URL,
        'records_count': str(len(df)),
        'scraped_at': datetime.now().isoformat(),
        'file_type': 'csv'
    }

    # Загружаем в S3
    success = upload_df_to_s3(s3_client, df, bucket_name, s3_key, metadata)

    if success:
        logger.info(f"✅ Основные данные сохранены в S3: {s3_key}")
        return s3_key
    else:
        logger.error("❌ Не удалось сохранить основные данные в S3")
        return None
