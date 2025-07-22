from datetime import datetime

BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"
LINK_CLASS = "accordeon-inner__item-title link xls"
LINK_PATTERN = r"/upload/reports/oil_xls/oil_xls_(\d{8})"
PAGE_PARAM = "?page=page-{}"
DATE_FORMAT = "%Y%m%d"
DEFAULT_CUTOFF_DATE = datetime(2025, 7, 18)
NUM_CONSUMERS_LINK = 3
CONSUMER_TIMEOUT = 5
REQUEST_DELAY = 0.3
METRIC_TON_UNIT = "Единица измерения: Метрическая тонна"
ITOGO = "Итого:"
COLUMN_NAMES = {
    "Код\nИнструмента": {"type": "str"},
    "Наименование\nИнструмента": {"type": "str"},
    "Базис\nпоставки": {"type": "str"},
    "Объем\nДоговоров\nв единицах\nизмерения": {"type": "int"},
    "Обьем\nДоговоров,\nруб.": {"type": "int"},
    "Количество\nДоговоров,\nшт.": {"type": "int"},
}
FIELD_NAMES = {
    "exchange_product_id": "Код\nИнструмента",
    "exchange_product_name": "Наименование\nИнструмента",
    "delivery_basis_name": "Базис\nпоставки",
    "volume": "Объем\nДоговоров\nв единицах\nизмерения",
    "total": "Обьем\nДоговоров,\nруб.",
    "count": "Количество\nДоговоров,\nшт.",
}
