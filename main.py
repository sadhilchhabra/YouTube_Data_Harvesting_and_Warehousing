import mysql.connector as sql
from googleapiclient.discovery import build
from streamlit_option_menu import option_menu
import pymongo
import pandas as pd
import streamlit as st
import psycopg2

# -------------------------------This is the configuration page for our Streamlit Application---------------------------
st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing",
    page_icon="▶️",
    layout="wide"
)

# -------------------------------This is the sidebar in a Streamlit application, helps in navigation--------------------
with st.sidebar:
    selected = option_menu("Main Menu", ["Extract", "Migrate", "View"],
                           icons=["house", "gear", "tools"],
                           styles={"nav-link": {"font": "sans serif", "font-size": "20px", "text-align": "centre"},
                                   "nav-link-selected": {"font": "sans serif", "background-color": "#088F8F"},
                                   "icon": {"font-size": "20px"}
                                   }
                           )

# --------------Connecting with MongoDB Atlas Cluster and Creating a new database named 'youtubeData'---------------
client = pymongo.MongoClient('<Client string>')
db = client['youtubeData']
mycoll1 = db["channel_details"]
mycoll2 = db["video_details"]
mycoll3 = db["comments_details"]

# -----------------------------------------Connecting with PostgreSQL Workbench Database------------------------------------


mydb = psycopg2.connect("dbname=<DB name> user=postgres password=<Password goes here>")

# If buffered is True , the cursor fetches all rows from the server after an operation is executed.
cursor1 = mydb.cursor()

# ----------------------------------------------Connecting with YouTube API------------------------------------------
youtube = build('youtube', 'v3', developerKey="<Developer Key>")


# -------------------Here we are extracting the channel details with the help of YouTube Channel id-------------------
def extract_channel(chid):
    cdata = []
    result1 = youtube.channels().list(part='snippet,contentDetails,statistics', id=chid).execute()

    for i in range(len(result1['items'])):
        channel_details = dict(Channel_id=chid[i],
                               Channel_name=result1['items'][i]['snippet']['title'],
                               Playlist_id=result1['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                               Subscribers=int(result1['items'][i]['statistics']['subscriberCount']),
                               Views=int(result1['items'][i]['statistics']['viewCount']),
                               Total_videos=int(result1['items'][i]['statistics']['videoCount']),
                               Description=result1['items'][i]['snippet']['description'],
                               )
        cdata.append(channel_details)
    return cdata


# ------------------------------Here we are transforming the Duration field to make it more readable-------------------
def modify_duration(duration):
    duration_str = ""
    hours = 0
    minutes = 0
    seconds = 0
    duration = duration[2:]  # This removes 'PT' prefix from duration

    # This checks if hours, minutes, seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.index("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index + 1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index + 1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # This formats the duration string
    if hours > 0:
        duration_str += f"{hours}h "
    if minutes > 0:
        duration_str += f"{minutes}m "
    if seconds > 0:
        duration_str += f"{seconds}s"

    return duration_str.strip()


# ---------------------------Here we are extracting the video ids with the help of YouTube Channel id-------------------
def extract_channelvideo(chid):
    video_ids = []
    result2 = youtube.channels().list(id=chid, part='contentDetails').execute()
    playlist_id = result2['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        # The maxResults parameter specifies the maximum number of items that should be returned in the result
        result2 = youtube.playlistItems().list(playlistId=playlist_id, part='snippet', pageToken=next_page_token).execute()
        for i in range(len(result2['items'])):
            video_ids.append(result2['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = result2.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# -----------------------------Here we are extracting the video details using video ids---------------------------------
def extract_video(cvids):
    vdata = []
    for i in range(0, len(cvids), 10):
        # List slicing first 10 video ids and so on and then joining them and
        # then extracting video details of them respectively.
        response3 = youtube.videos().list(part="snippet,contentDetails,statistics", id=','.join(cvids[i:i + 10]))
        result3 = response3.execute()
        for videos in result3['items']:
            video_details = dict(Channel_name=videos['snippet']['channelTitle'],
                                 Channel_id=videos['snippet']['channelId'],
                                 Video_id=videos['id'],
                                 Title=videos['snippet']['title'],
                                 Tags=videos['snippet'].get('tags'),
                                 Thumbnail=videos['snippet']['thumbnails']['default']['url'],
                                 Description=videos['snippet']['description'],
                                 Published_date=videos['snippet']['publishedAt'],
                                 Duration=modify_duration(videos['contentDetails']['duration']),
                                 Views=int(videos['statistics']['viewCount']),
                                 Likes=int(videos['statistics'].get('likeCount', 0)),
                                 Comments=int(videos['statistics'].get('commentCount', 0)),
                                 Favorite_count=int(videos['statistics']['favoriteCount']),
                                 Caption_status=videos['contentDetails']['caption']
                                 )
            vdata.append(video_details)
    return vdata


# ----------------------------Here we are extracting the comment details using video ids--------------------------------
def extract_comment(v_id):
    comdata = []
    try:
        next_page_token = None
        while True:
            result4 = youtube.commentThreads().list(part="snippet,replies", videoId=v_id, maxResults=100,
                                                    pageToken=next_page_token).execute()
            for coments in result4['items']:
                comment_details = dict(Comment_id=coments['id'],
                                       Video_id=coments['snippet']['videoId'],
                                       Comment_text=coments['snippet']['topLevelComment']['snippet']['textDisplay'],
                                       Comment_author=coments['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                       Comment_posted_date=coments['snippet']['topLevelComment']['snippet']['publishedAt'],
                                       )
                comdata.append(comment_details)
            next_page_token = result4.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comdata


# ----------------------------------------------------------------------------------------------------------------

# Here we are extracting our Data form Youtube API and uploading that data to MongoDB Atlas Database Cluster
if selected == "Extract":
    st.write("### Enter a YouTube Channel id -")
    cid = st.text_input("Channel id").split(',')

    if cid and st.button("Extract Data from API"):
        ch_details = extract_channel(cid)
        st.write(f'### Channel Data Extracted Successfully')
        st.write(ch_details)

    if st.button("Upload Data to MongoDB Atlas"):
        with st.spinner('Uploading....'):
            ch_details = extract_channel(cid)
            vi_ids = extract_channelvideo(cid)
            vid_details = extract_video(vi_ids)

            def comments():
                com_d = []
                for i in vi_ids:
                    com_d = com_d + extract_comment(i)
                return com_d

            comm_details = comments()
            mycoll1.insert_many(ch_details)
            mycoll2.insert_many(vid_details)
            mycoll3.insert_many(comm_details)
            st.success("Data Uploaded Successfully")


# Here we are migrating the data to PostgreSQL Database
if selected == "Migrate":
    st.write("### Data Migration from MongoDB Atlas to PostgreSQL")

    # Here we are extracting the channel details from MongoDB Atlas Cluster
    def youtube_channel_names():
        channelname = []
        for i in db.channel_details.find():
            channelname.append(i['Channel_name'])
        return channelname

    ch_names = youtube_channel_names()
    user_inp = st.selectbox("Select the channel for data migration :", options=ch_names)

    def create_psql_tables():
        commands = (
        """

        CREATE TABLE channels(
            Channel_id VARCHAR(255),
            Channel_name VARCHAR(255),
            Playlist_id VARCHAR(255),
            Subscribers INT,
            Views INT,
            Total_videos INT,
            Description TEXT,
            PRIMARY KEY(Channel_id)
        );


        CREATE TABLE videos(
            Channel_name VARCHAR(255),
            Channel_id VARCHAR(255),
            Video_id VARCHAR(255),
            Title TEXT,
            Tags TEXT,
            Thumbnail TEXT,
            Description TEXT,
            Published_date TEXT,
            Duration VARCHAR(255),
            Views INT,
            Likes INT,
            Comments INT,
            Favorite_count INT,
            Caption_status TEXT,
            PRIMARY KEY(Video_id)
        );


        CREATE TABLE comments(
            Comment_id VARCHAR(255),
            Video_id VARCHAR(255),
            Comment_text TEXT,
            Comment_author TEXT,
            Comment_posted_date TEXT,
            PRIMARY KEY(Comment_id)
        );
        """)
        cursor1.execute(commands)
        mydb.commit()

    def migrate_data_to_channels():
        mycoll = db['channel_details']
        query1 = """INSERT INTO channels(
                Channel_id,
                Channel_name,
                Playlist_id,
                Subscribers,
                Views,
                Total_videos,
                Description) VALUES(%s,%s,%s,%s,%s,%s,%s)"""

        response = mycoll.find({"Channel_name": user_inp}, {'_id': 0})
        for i in response:
            cursor1.execute(query1, tuple(i.values()))
            mydb.commit()


    def migrate_data_to_videos():
        mycoll1 = db["video_details"]
        query2 = """INSERT INTO videos(
                Channel_name,
                Channel_id,
                Video_id,
                Title,
                Tags,
                Thumbnail,
                Description,
                Published_date,
                Duration,
                Views,
                Likes,
                Comments,
                Favorite_count,
                Caption_status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        response = mycoll1.find({"Channel_name": user_inp}, {'_id': 0})
        for i in response:
            cursor1.execute(query2, tuple(i.values()))
            mydb.commit()


    def migrate_data_to_comments():
        mycoll1 = db["video_details"]
        mycoll2 = db["comments_details"]
        query3 = """INSERT INTO comments(
                Comment_id,
                Video_id,
                Comment_text,
                Comment_author,
                Comment_posted_date) VALUES(%s,%s,%s,%s,%s)"""

        resp = mycoll1.find({"Channel_name": user_inp}, {'_id': 0})
        for vid in resp:
            subresponse = mycoll2.find({'Video_id': vid['Video_id']}, {'_id': 0})
            for i in subresponse:
                cursor1.execute(query3, tuple(i.values()))
                mydb.commit()


    if st.button("Migrate Data to MySQL"):
        try:
            with st.spinner('Migrating....'):
                create_psql_tables()
                migrate_data_to_channels()
                migrate_data_to_videos()
                migrate_data_to_comments()
                st.success("Data Migration Successful")
        except:
            st.error("Error, Data Already Migrated")


# Here we are querying the data from MySQL Database
if selected == "View":
    st.write("## :yellow[Select a question to get the Query Results]")
    questions = st.selectbox('Questions',
                             ['- What are the names of all the videos and their corresponding channels?',
                              '- Which channels have the most number of videos, and how many videos do they have?',
                              '- What are the top 10 most viewed videos and their respective channels?',
                              '- How many comments were made on each video, and what are their corresponding video names?',
                              '- Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '- What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '- What is the total number of views for each channel, and what are their corresponding channel names?',
                              '- What are the names of all the channels that have published videos in the year 2022?',
                              '- Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '- What are the names of all the videos and their corresponding channels?':
        cursor1.execute("""SELECT Title AS Video_name, Channel_name FROM videos ORDER BY Channel_name""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- Which channels have the most number of videos, and how many videos do they have?':
        cursor1.execute("""SELECT Channel_name, Total_videos FROM channels ORDER BY Total_videos DESC""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- What are the top 10 most viewed videos and their respective channels?':
        cursor1.execute("""SELECT Title AS Video_name, Views, Channel_name FROM videos
                            ORDER BY Views DESC LIMIT 10""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- How many comments were made on each video, and what are their corresponding video names?':
        cursor1.execute("""SELECT Comments, Title AS Video_name FROM videos""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor1.execute("""SELECT Title AS Video_name, Likes, Channel_name FROM videos
                            ORDER BY Likes DESC""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor1.execute("""SELECT Likes, Title AS Video_name FROM videos""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor1.execute("""SELECT Views, Channel_name FROM channels""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- What are the names of all the channels that have published videos in the year 2022?':
        cursor1.execute("""SELECT Channel_name FROM videos
                            WHERE Published_date LIKE '2022%'
                            GROUP BY channel_name""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)

    elif questions == '- Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor1.execute("""SELECT Title AS Video_name, Comments, Channel_name FROM videos
                            ORDER BY Comments DESC""")
        # rows = cursor1.fetchall()
        # for rows in rows:
        #     st.write(rows)
        df = pd.DataFrame(cursor1.fetchall(), columns=cursor1.column_names)
        st.write(df)
