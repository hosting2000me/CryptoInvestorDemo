import pytest
pytest.importorskip("asyncpg")

import asyncpg
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text
from typing import Union, Dict, List, Any, Optional
from sqlalchemy.engine.result import Result
from sqlalchemy.ext.asyncio import AsyncEngine

class SQLAlchemyDataStore:
    # Просто для сокращения сигнатур вызова. Можем принимать List Of Dictionaries (для batch операций)
    QueryParams = Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]
    def __init__(self, db_url: str):
        self.db_url: str = db_url
        self.engine: AsyncEngine = create_async_engine(
                    db_url,  
                    pool_pre_ping=True,     # Проверяет соединение перед использованием
                    pool_recycle=3600,      # Пересоздает соединения каждый час
                    )
    
    async def disconnect(self) -> None:
        # This method is created for testing purposes and is currently unused
        if self.engine:
            await self.engine.dispose()
    
    async def execute(self, query: str, params: QueryParams = None) -> Result:
        # This method is created for testing purposes and is currently unused
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            await conn.commit()
            return result
    
    async def select(self, query: str, params: QueryParams = None) -> List[Any]:
        """
        Выполняет SELECT запрос и возвращает все результаты через fetchall
        
        Args:
            query: SQL запрос
            **params: параметры для запроса
            
        Returns:
            Результаты запроса через fetchall
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            return result.mappings().fetchall()
    
    async def fetch_one(self, query: str, params: QueryParams = None) -> Optional[Dict[str, Any]]:
        """
        Выполняет SQL запрос и возвращает первую запись через fetchone
        
        Args:
            query: SQL запрос
            params: параметры для запроса
            
        Returns:
            Первая запись результата или None
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            row = result.mappings().fetchone()
            return dict(row) if row else None
    
    async def transaction(self):
        conn = await self.engine.connect()
        trans = await conn.begin()

        class Tx:
            def __init__(self, conn, trans):
                self.conn = conn
                self.trans = trans

            # просто выполнить SQL, вернуть raw result
            async def execute(self, query: str, params=None):
                return await self.conn.execute(text(query), params or {})

            # получить все строки
            async def select(self, query: str, params=None):
                result = await self.conn.execute(text(query), params or {})
                return result.mappings().fetchall()

            # получить одну строку
            async def fetch_one(self, query: str, params=None):
                result = await self.conn.execute(text(query), params or {})
                return result.mappings().fetchone()                

            async def commit(self):
                await self.trans.commit()
                await self.conn.close()

            async def rollback(self):
                await self.trans.rollback()
                await self.conn.close()

        return Tx(conn, trans)
    
    async def execute_in_transaction(self, queries_and_params: List[tuple]) -> List[Any]:
        """
        Выполняет несколько запросов в одной транзакции
        
        Args:
            queries_and_params: список кортежей (query, params)
            
        Returns:
            Список результатов для каждого запроса
        """
        async with self.engine.connect() as conn:
            async with conn.begin():
                results = []
                for query, params in queries_and_params:
                    result = await conn.execute(text(query), params or {})
                    results.append(result)
                return results
