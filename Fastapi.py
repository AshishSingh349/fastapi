from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg
import pandas as pd
from typing import List

app = FastAPI()

# Define a list of database URLs with identifiable names
DATABASE_URLS = {
    "EDGE": "postgresql://postgres:TmeicIndia9@10.29.70.101:5432/TMEDGEDB",
    "VIB": "postgresql://postgres:postgres@10.29.70.102:5432/TMVIBDB",
    "TMDB": "postgresql://postgres:TmeicIndia9@10.29.70.101:5432/TMDB",
    "CSTMGR": "postgresql://postgres:TmeicIndia9@10.29.70.101:5432/CST_MGR",

    # Add more database URLs as needed
}

async def get_database(db_url):
    pool = await asyncpg.create_pool(db_url)
    return pool

class DynamicTable(BaseModel):
    table_name: str

class DynamicData(BaseModel):
    data: dict


@app.post("/create/{db_name}/{schema_name}/{table_name}", response_model=DynamicData)
async def create_data(db_name: str, schema_name: str,table_name: str, data: DynamicData):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        columns = ", ".join(data.data.keys())
        values = ", ".join(["$"+str(i+1) for i in range(len(data.data.values()))])
        query = f""" INSERT INTO "{schema_name}"."{table_name}" ({columns}) VALUES ({values}) RETURNING *; """
        row = await conn.fetchrow(query, *data.data.values())
        if not row:
            raise HTTPException(status_code=400, detail="Failed to insert data")
        return DynamicData(data=dict(row))

@app.get("/readall/{db_name}/{schema_name}/{table_name}")
async def read_all_data(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f""" SELECT * FROM "{schema_name}"."{table_name}"; """
        try:
            rows = await conn.fetch(query)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        return [DynamicData(data=dict(row)) for row in rows]
 

# For get_PieceData_AsDF below function added.
@app.get("/read_table_data/{db_name}/{schema_name}/{table_name}")
async def read_table_data(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""SELECT * FROM "{schema_name}"."{table_name}";"""
        try:
            rows = await conn.fetch(query)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        return rows
    
@app.get("/read_column_list/{db_name}/{schema_name}/{table_name}")
async def read_column_list(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""SELECT column_name from information_schema.Columns where table_schema = '{schema_name}' and table_name = '{table_name}';"""
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        return [row for row in rows]
    

@app.get("/read_table_primary_key/{db_name}/{schema_name}/{table_name}")
async def get_Table_primary_key_list(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""select ccu.column_name from information_schema.table_constraints as tc
                    join information_schema.constraint_column_usage as ccu on tc.constraint_name = ccu.constraint_name and tc.table_schema =ccu.table_schema
                    where tc.table_schema = '{schema_name}' and constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';"""
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        return rows

####################################################################################################################
##                                READ TABLE COLUMNS COMMENT
####################################################################################################################
@app.get("/col_comment/{db_name}/{schema_name}/{table_name}")
async def read_column_comment(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""SELECT jsonb_object_agg(column_name, column_comment) AS column_comments
                FROM 
                (
                    SELECT a.attname AS column_name, d.description AS column_comment
                    FROM pg_attribute AS a
                    LEFT JOIN pg_description AS d ON a.attrelid = d.objoid AND a.attnum = d.objsubid
                    JOIN pg_class AS c ON a.attrelid = c.oid
                    JOIN pg_namespace AS n ON c.relnamespace = n.oid
                    WHERE n.nspname = '{schema_name}' AND c.relname = '{table_name}'  AND a.attnum > 0 AND NOT a.attisdropped
                ) AS subquery;"""
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        return [row for row in rows]
        


####################################################################################################################
##                                READ TABLE LIST IN SCHEMA
####################################################################################################################
@app.get("/table_list/{db_name}/{schema_name}")
async def get_all_table_names(db_name: str, schema_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""SELECT table_name FROM information_schema.tables
      where table_schema = '{schema_name}' AND table_type='BASE TABLE'
      ORDER BY table_name;"""
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        return [row['table_name'] for row in rows]
    
####################################################################################################################
##                                READ TABLE LIST IN SCHEMA BY PARTIAL STRING
####################################################################################################################
@app.get("/cond_table_list/{db_name}/{schema_name}/{table_name_partial_string}")
async def get_alike_table_names(db_name: str, schema_name: str,table_name_partial_string: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""SELECT table_name FROM information_schema.tables
                where table_schema = '{schema_name}' AND table_type='BASE TABLE'  AND table_name like '{table_name_partial_string}'
                ORDER BY table_name;"""
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        return [row['table_name'] for row in rows]
    

####################################################################################################################
##                                READ COLUMN LIST IN TABLE
####################################################################################################################
@app.get("/col_list/{db_name}/{schema_name}/{table_name}")
async def get_table_column_list(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""  select column_name from information_schema.Columns where table_schema = '{schema_name}' and table_name = '{table_name}';   """
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        return [row['column_name'] for row in rows]
    
####################################################################################################################
##                                READ TABLE COLUMN INFO LIST
####################################################################################################################
@app.get("/col_info/{db_name}/{schema_name}/{table_name}")
async def get_column_information(db_name: str, schema_name: str,table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""  select column_name,data_type,character_maximum_length from information_schema.columns where table_schema = '{schema_name}' and table_name = '{table_name}';   """
        try:
            rows = await conn.fetch(query)
            print(rows)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")
        # return [DynamicData(data=dict(row)) for row in rows]
        # return [(row['column_name'],row['data_type'],row['character_maximum_length']) for row in rows]
        return [row for row in rows]








class TableDataResponse(BaseModel):
    rows: List[DynamicData]
    tab_rows: List[DynamicData]

@app.get("/read_table_primary_key1/{db_name}/{schema_name}/{table_name}")
async def get_Table_primary_key_list1(db_name: str, schema_name: str, table_name: str):
    if db_name not in DATABASE_URLS:
        raise HTTPException(status_code=400, detail="Database not found")

    db_url = DATABASE_URLS[db_name]
    pool = await get_database(db_url)
    async with pool.acquire() as conn:
        query = f"""select ccu.column_name from information_schema.table_constraints as tc
                    join information_schema.constraint_column_usage as ccu on tc.constraint_name = ccu.constraint_name and tc.table_schema =ccu.table_schema
                    where tc.table_schema = '{schema_name}' and constraint_type = 'PRIMARY KEY' and tc.table_name = '{table_name}';"""
        table_query = f"""SELECT * FROM "{schema_name}"."{table_name}";"""
        try:
            rows = await conn.fetch(query)
            tab_rows = await conn.fetch(table_query)
        except asyncpg.exceptions.DataError as e:
            raise HTTPException(status_code=400, detail="Data error: " + str(e))
        if not rows:
            raise HTTPException(status_code=404, detail="No data found in the table")

        # Create a response object with both sets of data
        response = TableDataResponse(
            rows=[DynamicData(data=dict(row)) for row in rows],
            tab_rows=[DynamicData(data=dict(row)) for row in tab_rows]
        )
        return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
