import psycopg2
import boto3

#importing pandas for printing the ouput using pandas dataframe
import pandas as pd


s3 = boto3.client(
    's3',
    aws_access_key_id="AKIAVQ4WJJRVN7AIY7HQ", 
    aws_secret_access_key=" 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", 
    region_name="ap-south-1"
    )

#connection creation with redshift 
con=psycopg2.connect(
    dbname= 'dev', 
    host='cluster-1.ccbx4r1ighpu.ap-south-1.redshift.amazonaws.com', 
    port= '5439', 
    user= 'db_user', 
    password= 'Mohana2002'
    )

#Joining two different data into single table with specifies columns using join and store the output in another table
create="""CREATE TABLE Output_table5 AS
          SELECT music_data.artist_familiarity, 
               music_data.artist_hotttnesss, 
               music_data.artist_latitude, 
               music_data.artist_longitude, 
               music_data.artist_name, 
               music_data.artist_id,
               music_data.artist_terms, 
               music_data.artist_terms_freq, 
               music_data.song_id, 
               log_data.song_name, 
               log_data.song_popularity, 
               log_data.song_duration, 
               log_data.play_count
           FROM music_data
           JOIN log_data ON music_data.song_id = log_data.song_id
        ;"""

#cursor creation
cur = con.cursor()
cur.execute(create)

#query execution using cursor
cur.execute('select * from Output_table5;')

#print the query output using pandas dataframe
output1 = pd.DataFrame(cur.fetchall(),columns=[desc[0] for desc in cur.description])
print(output1)

#sorting the table with most popular song in ascending order
f_query='''SELECT o.song_id, 
                  o.artist_id, 
                  o.artist_name, 
                  o.song_name, 
                  o.song_duration, 
                  o.play_count, 
                  o.song_popularity
            FROM output_table5 o
            JOIN (SELECT song_id, MAX(song_popularity) AS max_popularity
            FROM output_table5
            GROUP BY song_id) m
            ON o.song_id = m.song_id AND o.song_popularity = m.max_popularity
            ORDER BY o.song_popularity DESC
        ;'''
#query execution using cursor
cur.execute(f_query)

#print the query output using pandas dataframe
output2 = pd.DataFrame(cur.fetchall(),columns=[desc[0] for desc in cur.description])
print(output2)

#close the connection
con.commit()
