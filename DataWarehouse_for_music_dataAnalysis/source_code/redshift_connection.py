import psycopg2
import boto3

s3 = boto3.client(
    's3',
    aws_access_key_id="*KIAVQ4WJJRVN7AIY7HQ", 
    aws_secret_access_key=" 9L2ovZm3T19vaLb7DbPMOXWL*rV9dCQVHrvJvU4R", 
    region_name="ap-south-1"
    )

#connection creation with redshift database using psycopg2
con=psycopg2.connect(
    dbname= 'dev', 
    host='cluster-1.ccbx4r1ighpu.ap-south-1.redshift.amazonaws.com', 
    port= '5439', 
    user= 'db_user', 
    password= 'Mohana2002'
    )

#table creation1

create="""create table music_data
(
    artist_familiarity varchar(500),
    artist_hotttnesss varchar(300),	
    artist_id varchar(300) primary key,
    artist_latitude varchar(300),	
    artist_longitude varchar(300),	
    artist_name varchar(300),	
    artist_terms varchar(300),	
    artist_terms_freq varchar(300),	
    release_id varchar(300),	
    song_id varchar(300)
);"""

#table creation2
table1="""create table log_data
(
    song_name varchar(955),	
    song_popularity	varchar(955),
    song_id	varchar(955) not null primary key,
    song_duration	varchar(955),
    play_count	varchar(955),
    artist_id varchar(955) not null
);"""

#Load data from S3 into redshift table1 using copy command 

copy_command="""copy music_data
    from 's3://my-music-data-bucket/musicdata_source/music_data.csv' 
    iam_role 'arn:aws:iam::379882916970:role/I_am_The_Role'
    DELIMITER ','
    IGNOREHEADER 1
    
;"""

#Load data from S3 into redshift table2 using copy command 

copy_command1="""copy log_data
    from 's3://my-music-data-bucket/musicdata_source/log_data.csv' 
    iam_role 'arn:aws:iam::379882916970:role/I_am_The_Role'
    DELIMITER ','
    IGNOREHEADER 1
;"""


#Opening a cursor and run the create & copy query

cur = con.cursor()
cur.execute(create)
cur.execute(copy_command)
cur.execute(table1)
cur.execute(copy_command1)
con.commit()

#Close the cursor and the connection

cur.close()
con.close()

#print statement to verify the successful execution

print("Executed Successfully")
