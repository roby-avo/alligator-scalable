import os
import asyncio
import aiohttp
from URLs import URLs

headers = {
    'accept': 'application/json'
}

class LamAPI():
    def __init__(self, LAMAPI_HOST, LAMAPI_HOST_PORT, client_key,  response_format="json") -> None:
        self.format = response_format
        base_url = LAMAPI_HOST
        if LAMAPI_HOST_PORT is not None:
            base_url = f"{LAMAPI_HOST}:{LAMAPI_HOST_PORT}/"
        self._url = URLs(base_url, response_format=response_format)
        self.client_key = client_key

    async def __to_format(self, response):
        if self.format == "json":
            try:
                result = await response.json()
                for kg in ["wikidata", "dbpedia"]:
                    if kg in result:
                        result = result[kg]
                        break
                return result
            except:
                return {}
        else:
            raise Exception("Sorry, Invalid format!") 

    async def __submit_get(self, url, params):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                return await self.__to_format(response)

    async def __submit_post(self, url, params, json):
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, params=params, json=json) as response:
                return await self.__to_format(response)

    async def literal_recognizer(self, column):
        json_data = {'json': column}
        params = {'token': self.client_key}
        result = await self.__submit_post(self._url.literal_recognizer_url(), params, json_data)
        freq_data = {}
        for cell in result:
            item = result[cell]
            if item["datatype"] == "STRING" and item["datatype"] == item["classification"]:
                datatype = "ENTITY"
            else:
                datatype = item["classification"]  
            if datatype not in freq_data:
                freq_data[datatype] = 0
            freq_data[datatype] += 1   

        return freq_data
        

    async def labels(self, entitites, kg="wikidata"):
        params = {
            'token': self.client_key,
            'kg': kg
        }
        json_data = {
            'json': entitites
        }
        result = await self.__submit_post(self._url.entities_labels(), params, json_data)
        return result

    async def objects(self, entitites, kg="wikidata"):
        params = {
            'token': self.client_key,
            'kg': kg
        }
        json_data = {
            'json': entitites
        }
        result = await self.__submit_post(self._url.entities_objects_url(), params, json_data)
        return result

    async def predicates(self, entitites, kg="wikidata"):
        params = {
            'token': self.client_key,
            'kg': kg
        }
        json_data = {
            'json': entitites
        }
        result = await self.__submit_post(self._url.entities_predicates_url(), params, json_data)
        return result

    async def types(self, entitites, kg="wikidata"):
        params = {
            'token': self.client_key,
            'kg': kg
        }
        json_data = {
            'json': entitites
        }
        result = await self.__submit_post(self._url.entities_types_url(), params, json_data)
        return result

    async def literals(self, entitites, kg="wikidata"):
        params = {
            'token': self.client_key,
            'kg': kg
        }
        json_data = {
            'json': entitites
        }
        result = await self.__submit_post(self._url.entities_literals_url(), params, json_data)
        return result

    async def lookup(self, string, ngrams=False, fuzzy=False, types=None, kg="wikidata", limit=100):
        params = {
            'token': self.client_key,
            'name': string,
            'ngrams': ngrams,
            'fuzzy': fuzzy,
            'types': types,
            'kg': kg,
            'limit': limit
        }
        result = await self.__submit_get(self._url.lookup_url(), params)
        if len(result) > 1:
            result = {"wikidata": result}
        return result
