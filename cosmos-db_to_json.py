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
    #Copy this code for each query you want to run. Just replace fname, query, and data

    conn.add_task(Query(
        fname="",
        query=""" """,
        data=None
    ))
    
    '''
    conn.add_task(Query(
        fname="ausi_covid_comments",
        query="""SELECT *
            FROM australian_dod.comments
            left join australian_dod.videos
            on videos.video_id = comments.video_id
            where match (video_title, transcript, description)
            against ("+australia +(covid china chinese virus wuhan coronavirus government narrative global virus power cover shift countries pandemic health)" in boolean mode)
            where published_date between "2019-07-01 00:00:00" and "2020-12-31 00:00:00";""",
        data=None
    ))
#     conn.add_task(Query(
#         fname="ausi_dod_video_commenter_network",
#         query="""SELECT video_id, commenter_id
# FROM australian_dod.comments
# where published_date between "2019-07-01 00:00:00" and "2020-12-31 00:00:00"; """,
#         data=None
#     ))

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
        if results: self.save_json(results, query.fname)
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

    def save_json(self, data, fname, loc="export", cols=[]):
        df = pd.DataFrame(data)
        path = os.path.join(os.getcwd(), loc+"\\"+fname+".json")
        df.to_json(path)
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