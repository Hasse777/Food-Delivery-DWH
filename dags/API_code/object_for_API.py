import requests as r
from datetime import datetime
from typing import List, Dict


class YandexAPI:
    """
    Класс для работы с предаставленным API в проекте.
    """
    BASE_URL = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    GET_METHODS = {
        'restaurant': '/restaurants',
        'couriers': '/couriers',
        'deliveries': '/deliveries'
    }

    def __init__(self, *, nickname: str, cohort: str, api_key: str) -> None:
        self._headers = {
            'X-Nickname': nickname,
            'X-Cohort': cohort,
            'X-API-KEY': api_key
        }

    def get_couriers(self, *, sort_field: str = '_id', sort_direction: str = 'asc', limit: int = 50, offset: int = 0) -> List[Dict[str, str]]:
        """
        Метод возвращает список курьеров.

        Args:
            sort_field: Возможные значения [_id or name]. Параметр определяет
            поле, по которому будет применяться сортировка.

            sort_direction: Возможные значения [asc - по возрастанию or desk - по убыванию].
            Порядок сортировки.

            limit: Максимальное количество записей, которые будут возвращены в ответе.

            offset: Указатель на запись с которой нужно начать возвращать список.
        """
        try:
            params = {
                'sort_field': sort_field,
                'sort_direction': sort_direction,
                'limit': limit,
                'offset': offset
            }
            url = self.BASE_URL + self.GET_METHODS['couriers']
            response = r.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            raise RuntimeError(f'Не удалось получить список курьеров: {e}')

    def get_deliveries(self, *, sort_field: str = 'date', sort_direction: str = 'asc', limit: int = 50, offset: int = 0, restaurant_id: str = '', from_date: str = '2022-01-01 00:00:00', to_date: str = '') -> List[Dict[str, str]]:
        """
        Метод возвращает список совершённых доставок.

        Args:
            sort_field: Возможные значения [_id or date]. Параметр определяет
            поле, по которому будет применяться сортировка.

            sort_direction: Возможные значения [asc - по возрастанию or desk - по убыванию].
            Порядок сортировки.

            limit: Максимальное количество записей, которые будут возвращены в ответе.

            offset: Указатель на запись с которой нужно начать возвращать список.

            restaurant_id: id искомого ресторана. Если не указано, то метод
            вернет данные по всем доступным ресторанам в БД.

            from_date: Дата, с которой начинается выборка данных.

            to_data: Дата, на которой заканчивается выборка данных.
        """
        try:
            if not to_date:
                to_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            params = {
                'sort_field': sort_field,
                'sort_direction': sort_direction,
                'limit': limit,
                'offset': offset,
                'restaurant_id': restaurant_id,
                'from': from_date,
                'to': to_date
            }
            url = self.BASE_URL + self.GET_METHODS['deliveries']
            response = r.get(url=url, headers=self._headers, params=params)
            response.raise_for_status()
            return response.json()

        except Exception as e:
            raise RuntimeError(f'Не удалось получить список совершенных доставок: {e}')
