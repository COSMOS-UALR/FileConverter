from tqdm import tqdm
import pandas as pd
import numpy as np
import aiomysql
import asyncio
import pymysql
import os

def main():
    conn = Connection()
    '''
    Copy this code for each query you want to run. Just replace fname, query, and data

    conn.add_task(Query(
        fname="",
        query=""" """,
        data=None
    ))
    
    '''
    conn.add_task(Query(
        fname="ausi_dod_related_videos",
        query="""SELECT video_id, parent_video, crawled_time 
        FROM australian_dod.related_videos;""",
        data=None
    ))
    # conn.add_task(Query(
    #     fname="ausi_dod_videos_daily",
    #     query="""SELECT * FROM australian_dod.videos_daily;""",
    #     data=None
    # ))

    conn.run(debug=False)


class Query:
    def __init__(self, fname, query, data=None, group=""):
        self.fname = fname
        self.query = query
        self.data = data
        self.group = group


class Connection:
    def __init__(self, export_dir="export"):
        self.pool = None
        self.tasks = []
        self.export_dir = export_dir

    def add_task(self, query: Query):
        query.query = " ".join(query.query.split())
        t = self.create_task(query)
        self.tasks.append(t)

    async def create_task(self, query: Query):
        results = await self.exectue(query.query, query.data)
        if len(results) > 1000000: #chunking files too large
            df = pd.DataFrame(results)
            num_chunks = round(len(results) / 1000000)
            chunks = np.array_split(df, num_chunks)
            c_count = 1
            for chunk in tqdm(chunks, desc=query.fname+" chunking"): 
                self.save_xlsx(chunk, query.fname + '_chunk_{}'.format(c_count))
                c_count += 1
            del df 
        elif results: self.save_xlsx(results, query.fname)
        else: return

    def run(self, debug=False):
        asyncio.run(self._run_all(), debug=debug)

    async def _run_all(self):
        self.pool = await aiomysql.create_pool(host='144.167.35.221', port=3306,
                                      user='db_exporter', password='Cosmos1', maxsize=20)
        [await f for f in tqdm(asyncio.as_completed(self.tasks),
            total=len(self.tasks), desc="SQL")]
        self.pool.close()
        await self.pool.wait_closed()

    def save_xlsx(self, data, fname, loc="export", cols=[]):
        df = pd.DataFrame(data)
        if cols: df.columns = cols
        path = os.path.join(os.getcwd(), loc+"\\"+fname+".xlsx")
        with pd.ExcelWriter(path, engine='xlsxwriter', options={'strings_to_urls': False}) as writer: 
            df.to_excel(writer, header=True, index=False, encoding='utf-8', na_rep='None')
        del df
        return fname

    async def exectue(self, query, data):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if data: await cur.execute(query, data)
                    else: await cur.execute(query)
                    records = await cur.fetchall()
            conn.close()
            return records
        except (AttributeError, pymysql.err.OperationalError):
            #asyncio.exceptions.IncompleteReadError: X bytes read on a total of Y expected bytes
            print("\nFailed to recieve all the data from the db. Re-running the query as blocking.")
            return self.block_execute(query, data)

    def block_execute(self, query, data):
        connection = pymysql.connect(host='144.167.35.221', user='db_exporter', 
            password='Cosmos1', cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            if data: cursor.execute(query, data)
            else: cursor.execute(query)
            records = cursor.fetchall()
        connection.close()
        return records

if __name__ == "__main__":
    main()