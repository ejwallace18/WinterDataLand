# Winter Dataland Top Christmas Songs Project

# import dependencies
import pandas as pd
import sqlalchemy 
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

# Clean data sets
# read in csv file for Christmas Carols dataset
df = pd.read_csv('ChristmasCarols.csv')
carol_df = pd.DataFrame(df)
carol_df.head()

# remove "" from song titles
carol_df["Title"] =carol_df['Title'].str.replace(r"\"","")
# check data set
carol_df.head()

# remove "" from Artist Names
carol_df["Artist"] =carol_df['Artist'].str.replace(r"\"","")

# drop unwanted columns
carol_df.drop(columns=['Additional information', 'Year'])

# read in csv file for Top 100 Billboard songs dataset
Top100_df = pd.read_csv('Top_100.csv')
Top100_df.head()

#drop unwanted column
Top100_df.drop(columns= ['url'])

# rename columns
Top100_df = Top100_df.rename(columns={'Week Position': 'week_position', 
                                      'Performer': 'Artist',
                                      'Song': 'Title',
                                     'Previous Week Position': 'previous_week',
                                    'Peak Position': 'peak_position',
                                    'Weeks on Chart': 'total_weeks'})
Top100_df.head()

# merge table of top_100 and carol merged on song and artist
merged_xmas = pd.merge(carol_df, Top100_df,  on=['Title', 'Artist'], how='left')
merged_xmas

# establish connection with postgres
engine = create_engine(f"postgresql://{username}:{password}@localhost:5432/Top_Songs")
connection = engine.connect()

# create carol table in PostGres and upload data
import csv
from io import StringIO
def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

carol_df.to_sql('carols', engine, method=psql_insert_copy)

# create top 100 table in PostGres and upload data
Top100_df.to_sql('top_100', engine, method=psql_insert_copy)







