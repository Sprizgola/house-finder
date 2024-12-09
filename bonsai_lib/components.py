import re
import sqlite3
import logging
from typing import Any, Dict, List, Optional, Union

from haystack import component
from haystack.dataclasses import Document, ByteStream, ChatMessage
from haystack.components.converters.utils import normalize_metadata
from haystack.components.fetchers import LinkContentFetcher
from bs4 import BeautifulSoup, Tag

from bonsai_lib.macros import REAL_ESTATE_STATUS, FLOOR_MAP, PROVINCE_MAP


@component
class SubitoItParser():
    
    @component.output_types(documents=List[Document])
    def run(self, sources: List[Union[str, ByteStream]], meta: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None, **kwargs):
        
        documents = []

        meta_list = normalize_metadata(meta=meta, sources_count=len(sources))

        for source, metadata in zip(sources, meta_list):

            soup = BeautifulSoup(source.data, 'html.parser')

            request_url = source.meta["url"]

            re_status = None

            for status in REAL_ESTATE_STATUS:

                if status in request_url:

                    re_status = status

                    break
            
            if not re_status:
                raise Exception("Could not find real estate status in request url")

            product_list_items = soup.find_all('div', class_=re.compile(r'item-card'))

            for product in product_list_items:
                
                # Initialize variables
                house = {"mq": "NOT-FOUND", "n_rooms": "NOT-FOUND", "n_bathrooms": "NOT-FOUND", "floor": "NOT-FOUND"}

                house["status"] = REAL_ESTATE_STATUS[re_status]

                house["title"] = product.find('h2').string

                specs = product.find('div', class_=re.compile(r'BigCard-module_additional-info')).contents
                for spec in specs:
                    if spec.string.endswith("mq"):
                        house["mq"] = spec.string
                    elif "Local" in spec.string:
                        n_rooms = spec.string.split(" ")[0]
                        house["n_rooms"] = n_rooms
                    elif "Bagn" in spec.string:
                        n_bathrooms = spec.string.split(" ")[0]
                        house["n_bathrooms"] = n_bathrooms
                    elif "Piano" in spec.string or any(x in spec.string for x in FLOOR_MAP):
                        floor = spec.string
                        if floor in FLOOR_MAP:
                            house["floor"] = FLOOR_MAP[floor]
                        else:
                            house["floor"] = floor.split(" ")[0].replace("°", "")
                    else:
                        print("UNKNOWN SPEC: " + spec.string)
                        continue
                try:
                    price = product.find('p',class_=re.compile(r'price')).contents[0]
                    # check if the span tag exists
                    price_soup = BeautifulSoup(price, 'html.parser')
                    if type(price_soup) == Tag:
                        continue

                    price = int(price.replace('.','')[:-2])
                except:
                    price = "NOT-FOUND"
                
                house["price"] = price

                link = product.find('a').get('href')
                sold = product.find('span',re.compile(r'item-sold-badge'))
                
                house["link"] = link
                house["sold"] = True if sold else False

                try:
                    location = product.find('span',re.compile(r'town')).string + product.find('span', re.compile(r'city')).string

                    location_regex = re.compile(r"(.*) ([\W]+)")
                    city = location_regex.search(location).groups()[0]
                    province = location.split("(")[1].replace(")", "")
                    house["city"] = city
                    house["province"] = PROVINCE_MAP[province]

                except:
                    house["city"] = "NOT-FOUND"
                    house["province"] = "NOT-FOUND"

                # Check if it managed by real estate agency
                is_real_estate_agency = False

                for span in product.find_all("span"):
                    if span.string == "Agenzia":
                        is_real_estate_agency = True

                house["is_real_estate_agency"] = is_real_estate_agency

                document = Document(content=house["title"], meta={**house, **metadata})
                documents.append(document)

        return {"documents": documents}


@component
class SQLWriter:
    
    def __init__(self, dbname: str) -> None:

        self._dbname = dbname   
        self.connection = sqlite3.connect(self._dbname)

    def _insert_query(self, table_name: str, table_schema: Dict[str, str], **kwargs):

        insert_query = f"INSERT INTO {table_name} ({', '.join(table_schema.keys())}) VALUES ({', '.join(['?'] * len(table_schema))})"       
        

        return insert_query

    @component.output_types(rows_written=int)
    def run(self, documents: List[Document], table_name: str, table_schema: Dict[str, str], create_table: bool = False,  **kwargs):
        
        insert_query = self._insert_query(table_name, table_schema)

        data = []

        for doc in documents:
            
            tmp = {"content": doc.content, **doc.meta}
            
            values = []
            
            for col in table_schema:
                values.append(tmp[col])

            data.append(tuple(values))
            
        cursor = self.connection.cursor()

        # Check if table exists
  
        res = cursor.execute(
                f"""SELECT name FROM sqlite_master  WHERE type='table'
                AND name='{table_name}'; """
            ).fetchall()

        if not res:
            if not create_table:
                raise Exception(f"Table {table_name} not found")

            cursor.execute(f"CREATE TABLE {table_name} ({', '.join([f'{k} {v}' for k, v in table_schema.items()])})")

        cursor.executemany(insert_query, data)
        self.connection.commit()
    
        return {"rows_written": len(data)}


@component
class SQLValidator:

    @component.output_types(query_to_validate=str, entities=str)
    def run(self, replies: List[str]):
        if 'DONE' in replies[0]:
            return {"query": replies[0].replace('DONE', '')}
        else:
            logging.info("Reflecting on query\n", replies[0])
            return {"query_to_validate": replies[0]}


@component
class SQLQuery:

    def __init__(self, dbname: str) -> None:

        self._dbname = dbname   
        self.connection = sqlite3.connect(self._dbname)

    @component.output_types(results=List[str], queries=List[str])
    def run(self, queries: List[str]):
        results = []

        cursor = self.connection.cursor()

        for query in queries:
            
            result = cursor.execute(query)
            
            results.extend(result.fetchall())

        return {"results": results, "queries": queries}
    


@component
class SQLQueryParser:

    @component.output_types(replies=List[str])
    def run(self, replies: Union[List[str], List[ChatMessage]]):
        results = []

        # regex = re.compile(r"\[SQL](.*?)\[\\SQL]")
        regex = re.compile(r"(.*?)```")

        for query in replies:
            
            if isinstance(query, ChatMessage):
                query = query.content

            query = query.replace("\n", " ")

            try:
                parsed_query = regex.search(query).groups()[0].lower()
            except AttributeError:
                return {"replies": [""]}
            
            # Add LOWERCASE cast to VARCHAR columns

            tmp = []

            conditions = parsed_query.split("where")[1]

            for condition in conditions.split("and"):

                col, val = condition.split("=")

                if "'" in val or '"' in val:
                    col = "lower({})".format(col.strip(" "))

                tmp.append("{} = {}".format(col.strip(), val.strip()))

            parsed_query = parsed_query.replace(conditions, " {}".format(" AND ".join(tmp))).strip()

            results.append(parsed_query)

        return {"replies": results}