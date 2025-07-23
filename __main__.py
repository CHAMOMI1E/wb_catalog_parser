import asyncio
import logging
from typing import Any, Optional
import aiohttp
import json
import xlsxwriter
import time
import functools

MAIN_MENU_URL = "https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v3.json"
SEARCH_URL = "https://search.wb.ru/exactmatch/sng/common/v14/search"

SEARCH_PARAMS = {
    "ab_testing": "false",
    "appType": "1",
    "curr": "rub",
    "dest": "-59202",
    "hide_dtype": "13",
    "lang": "ru",
    "resultset": "filters",
    "spp": "30",
    "suppressSpellcheck": "false",
}


class ParserBase:
    async def __aenter__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def _request(self, method: str, url: str, **kwargs: Any) -> Optional[dict]:
        if not self._session:
            raise RuntimeError(
                "Сессия не инициализирована. Используйте контекстный менеджер async with"
            )
        for attempt in range(1, 6):
            print(f"Запрос по ссылке {url}")
            async with self._session.request(method, url, **kwargs) as response:
                if response.status in (200, 203):
                    return await response.json(content_type=None)
                else:
                    logging.warning(
                        f"Ошибка при запросе к API: {response.status} - {await response.json()}"
                    )
                    await asyncio.sleep(30 * attempt)
                    continue
        else:
            logging.error("Превышено максимальное количество попыток")
            return None


class CategoryParser(ParserBase):
    async def get_categories(self):
        return await self._request("GET", MAIN_MENU_URL)

    async def get_category_items(self, search_query: str) -> list[dict]:
        params = SEARCH_PARAMS.copy()
        params["query"] = search_query
        res = await self._request("GET", SEARCH_URL, params=params)
        result = []
        if res:
            filters = res.get("data", {}).get("filters", [])
            for f in filters:
                if f.get("name") == "Категория":
                    for i in f.get("items", []):
                        result.append({
                            "id": i.get("id"),
                            "name": i.get("name"),
                            "level": 99,
                            "parent": search_query
                        })
        return result

    async def walk_categories(self, nodes, level=1, parent=None, root=None, result=None):
        if result is None:
            result = []
        tasks = []
        for node in nodes:
            current_root = root if root is not None else node.get("name")
            entry = {
                "id": node.get("id"),
                "name": node.get("name"),
                "level": level,
                "parent": parent,
                "root": current_root
            }
            result.append(entry)
            childs = node.get("childs")
            if childs:
                tasks.append(self.walk_categories(childs, level + 1, node.get("name"), current_root, result))
            if node.get("searchQuery"):
                tasks.append(self._add_search_items(node, result, current_root))
        if tasks:
            await asyncio.gather(*tasks)
        return result

    async def _add_search_items(self, node, result, root):
        items = await self.get_category_items(node["searchQuery"])
        for item in items:
            item["parent"] = node.get("name")
            item["root"] = root
        result.extend(items)


class ExcelExporter:
    def __init__(self, filename: str = "result.xlsx"):
        self.filename = filename


    def export(self, sheets: dict[str, list[dict]]):
        if not sheets:
            return
        workbook = xlsxwriter.Workbook(self.filename)
        for sheet_name, categories in sheets.items():
            ws_name = sheet_name
            worksheet = workbook.add_worksheet(ws_name)
            worksheet.write_row(0, 0, ["ID", "Название", "Уровень", "Родитель"])
            for row_num, row in enumerate(categories, start=1):
                worksheet.write_row(
                    row_num, 0, [row.get("id"), row.get("name"), row.get("level"), row.get("parent")]
                )
            worksheet.set_column(0, 0, 12)
            worksheet.set_column(1, 1, 45)
            worksheet.set_column(2, 2, 22)
            worksheet.set_column(3, 3, 45)
        print(f"Excel-файл успешно создан: {self.filename}")
        workbook.close()


def measure_time(func):
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"Функция {func.__name__} заняла {elapsed:.3f} сек.")
        return result

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"Функция {func.__name__} заняла {elapsed:.3f} сек.")
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


@measure_time
async def main():
    async with CategoryParser() as parser:
        categories = await parser.get_categories()
        refactored = await parser.walk_categories(categories)
        sheets = {}
        for cat in refactored:
            root_name = cat["root"]
            sheets.setdefault(root_name, []).append(cat)
        exporter = ExcelExporter()
        exporter.export(sheets)


if __name__ == "__main__":
    asyncio.run(main())
