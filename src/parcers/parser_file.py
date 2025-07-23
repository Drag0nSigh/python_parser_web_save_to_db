import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import xlrd

from src.constante import COLUMN_NAMES, DATE_FORMAT, FIELD_NAMES, ITOGO, METRIC_TON_UNIT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileParser:
    """Класс для парсинга файлов."""

    @staticmethod
    def process_headers(row: List[Any]) -> Dict[str, int]:
        """Парсинг строки заголовков и определение индексов столбцов.

        Args:
            row: Строка данных из XLS-файла.

        Returns:
            Dict[str, int]: Словарь с названиями столбцов и их индексами, или пустой словарь, если заголовки не найдены.
        """
        column_indices = {}
        for col_idx, value in enumerate(row):
            if isinstance(value, str) and value.strip() in COLUMN_NAMES:
                column_indices[value.strip()] = col_idx
        if all(col in column_indices for col in COLUMN_NAMES):
            return column_indices
        logger.error(f"Не найдены все необходимые заголовки в строке: {row}")
        return {}

    async def parse_file(
        self, file_content: Optional[bytes], date_file: datetime
    ) -> List[Dict[str, Any]]:
        """Парсинг содержимого XLS-файла и извлечение данных из таблицы с единицей измерения 'Метрическая тонна'.

        Args:
            file_content: Содержимое файла в байтах или None, если файл не скачан.
            date_file: Дата файла

        Returns:
            List[Dict[str, Any]]: Список словарей с данными для сохранения в базу данных.
        """
        if not self.not_file_content(file_content, date_file):
            return []

        try:
            if not self.checking_html_file(file_content, date_file):
                return []

            # Чтение XLS-файла с помощью xlrd
            data = self.xls_to_list_data(file_content, date_file)
            # Инициализация результата
            result = []
            in_metric_ton_section = False
            skip_next_row = False
            column_indices = {}

            # Проход по строкам для поиска нужной секции
            for index, row in enumerate(data):
                # Проверяем начало секции 'Метрическая тонна'
                if (
                    len(row) > 1
                    and isinstance(row[1], str)
                    and METRIC_TON_UNIT in row[1]
                ):
                    in_metric_ton_section = True
                    continue

                # Пропускаем строку подзаголовков
                if skip_next_row:
                    skip_next_row = False
                    continue

                # Проверяем заголовки
                if not column_indices and in_metric_ton_section:
                    column_indices = self.process_headers(row)
                    if column_indices:
                        skip_next_row = (
                            True  # Пропускаем следующую строку (подзаголовки)
                        )
                    continue

                # Проверяем конец секции
                if (
                    len(row) > 1
                    and isinstance(row[1], str)
                    and (METRIC_TON_UNIT in row[1] or ITOGO in row[1])
                ):
                    in_metric_ton_section = False
                    continue

                # Обрабатываем строки в нужной секции
                if (
                    in_metric_ton_section
                    and row[1]
                    and isinstance(row[1], str)
                    and len(row[1]) > 3
                ):
                    try:
                        # Проверяем наличие всех необходимых столбцов
                        if not all(col in column_indices for col in COLUMN_NAMES):
                            logger.error(
                                f"Недостаточно столбцов для обработки строки {index}: {row}"
                            )
                            continue

                        result.extend(
                            self.valid_row_in_dict_for_db(
                                row, column_indices, date_file
                            )
                        )
                    except (ValueError, IndexError) as e:
                        logger.error(f"Ошибка при обработке строки {index}: {e}")
                        continue

            logger.info(
                f"Извлечено {len(result)} записей для даты {date_file.strftime(DATE_FORMAT)}"
            )
            result = self.add_new_key_in_dict_for_db(result)
            return result

        except Exception as e:
            logger.error(
                f"Общая ошибка при парсинге файла для даты {date_file.strftime(DATE_FORMAT)}: {e}"
            )
            return []

    @staticmethod
    def not_file_content(file_content: Optional[bytes], date_file: datetime) -> bool:
        """Проверка наличия содержимого файла.

        Args:
            file_content: Содержимое файла в байтах или None, если файл не скачан.
            date_file: Дата, связанная с файлом.

        Returns:
            bool: True, если файл содержит данные, False, если файл отсутствует.
        """
        if file_content is None:
            logger.info(
                f"Пропуск парсинга, файл не скачан для даты: {date_file.strftime(DATE_FORMAT)}"
            )
            return False
        return True

    @staticmethod
    def checking_html_file(file_content: Optional[bytes], date_file: datetime) -> bool:
        """Проверка, является ли файл HTML.

        Args:
            file_content: Содержимое файла в байтах.
            date_file: Дата, связанная с файлом.

        Returns:
            bool: True, если файл не является HTML, False, если файл является HTML.
        """
        is_html = file_content.startswith(b"<!DOCTYPE") or file_content.startswith(
            b"<html"
        )
        if is_html:
            logger.error(
                f"Файл для даты {date_file.strftime(DATE_FORMAT)} является HTML, а не XLS"
            )
            return False
        return True

    @staticmethod
    def xls_to_list_data(
        file_content: Optional[bytes], date_file: datetime
    ) -> List[List[str]]:
        """Чтение XLS-файла и преобразование его в список строк.

        Args:
            file_content: Содержимое XLS-файла в байтах.
            date_file: Дата, связанная с файлом.

        Returns:
            List[List[str]]: Список строк данных из XLS-файла или пустой список при ошибке.

        Raises:
            xlrd.XLRDError: Если файл не является корректным XLS-файлом.
        """
        try:
            workbook = xlrd.open_workbook(file_contents=file_content)
            sheet = workbook.sheet_by_index(0)
            data = []
            for row_idx in range(sheet.nrows):
                row = sheet.row_values(row_idx)
                data.append(row)
            logger.info(
                f"Файл для даты {date_file.strftime(DATE_FORMAT)} успешно прочитан как XLS"
            )
            return data
        except xlrd.XLRDError as xlrd_err:
            logger.error(
                f"Ошибка при чтении XLS для даты {date_file.strftime(DATE_FORMAT)}: {xlrd_err}"
            )
            return []

    @staticmethod
    def valid_row_in_dict_for_db(
        row: List[str], column_indices: Dict[str, int], date_bulletin: datetime
    ) -> List[Dict[str, Any]]:
        """Преобразование строки данных в список словарей для базы данных.

        Args:
            row: Строка данных из XLS-файла.
            column_indices: Словарь с названиями столбцов и их индексами.
            date_bulletin: Дата билютеня

        Returns:
            List[Dict[str, Any]]: Список словарей с данными, если строка валидна и содержит договоры,
            иначе пустой список.
        """
        result = []
        count_col = FIELD_NAMES["count"]
        data_col = "date"
        try:
            count = (
                int(float(row[column_indices[count_col]]))
                if len(row) > column_indices[count_col]
                and row[column_indices[count_col]]
                else 0
            )
        except (ValueError, TypeError):
            count = 0
        if count > 0:
            record = {}
            for field_key, col_name in FIELD_NAMES.items():
                col_idx = column_indices[col_name]
                col_type = COLUMN_NAMES[col_name]["type"]
                if col_type == "int":
                    try:
                        value = (
                            int(float(row[col_idx]))
                            if len(row) > col_idx and row[col_idx]
                            else 0
                        )
                    except (ValueError, TypeError):
                        value = 0
                else:
                    value = (
                        str(row[col_idx]).strip()
                        if len(row) > col_idx and row[col_idx]
                        else ""
                    )
                record[field_key] = value
                record[data_col] = date_bulletin
            result.append(record)
        return result

    @staticmethod
    def add_new_key_in_dict_for_db(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for one_entry in data:
            one_entry["oil_id"] = one_entry.get("exchange_product_id", "")[:4]
            one_entry["delivery_basis_id"] = one_entry.get("exchange_product_id", "")[
                4:7
            ]
            one_entry["delivery_type_id"] = one_entry.get("delivery_basis_id", "")[-1]
            result.append(one_entry)
        return result
