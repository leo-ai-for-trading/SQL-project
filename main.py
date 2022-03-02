from re import sub
from psaw import PushshiftAPI
import datetime as dt
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
cursor.execute("""
    SELECT * FROM stock
""")
rows = cursor.fetchall()

stocks = {}
for row in rows:
    stocks['$' + row['symbol']] = row['id']

api = PushshiftAPI()

start_epoch=int(dt.datetime(2022, 2, 28).timestamp())

submissions = api.search_submissions(after=start_epoch,
                            subreddit='wallstreetbets',
                            filter=['url','author', 'title', 'subreddit'])
for submission in submissions:
    #print(submission)
    words = submission.title.split()
    #filter results by $ sign
    cashtags = list(set(filter(lambda word: word.lower().startswith('$'),words)))

    if len(cashtags) > 0:
        print(cashtags)

        cursor.execute("""
            INSERT INTO mention (dt, stock_id, message,source, url)
        
        
        """)