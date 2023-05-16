import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector
from datetime import datetime
import pandas as pd

def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part = 'snippet, contentDetails, statistics',
        id = channel_id
    )

    response = request.execute()

    channel = response['items'][0]
    channel_details = {
        'Channel_Name': channel['snippet']['title'],
        'Channel_ID': channel_id,
        'Description': channel['snippet']['description'],
        'Published': channel['snippet']['publishedAt'],
        'Thumbnail': channel['snippet']['thumbnails']['default']['url'],
        'Playlist_ID':channel['contentDetails']['relatedPlaylists']['uploads'],
        'video_Count': int(channel['statistics']['videoCount']),
        'view_Count': int(channel['statistics']['viewCount']),
        'Subscribers_Count': int(channel['statistics']['subscriberCount']),
    }
    return channel_details


def get_all_video_ids(youtube, playlist_id):
    video_ids = []
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            video_ids.append(video_id)

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return video_ids


# Get video details
def get_video_details(youtube, video_id):
    response = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=video_id
    ).execute()

    video = response['items'][0]

    if 'dislikeCount' in video['statistics']:
        dislike_count = video['statistics']['dislikeCount']
    else:
        dislike_count = 0

    if 'likeCount' in video['statistics']:
        like_count = video['statistics']['likeCount']
    else:
        like_count = 0

    if 'commentCount' in video['statistics']:
        comment_count = video['statistics']['commentCount']
    else:
        comment_count = 0

    if 'favoriteCount' in video['statistics']:
        favorite_count = video['statistics']['favoriteCount']
    else:
        favorite_count = 0

    video_caption = ''
    if video['contentDetails']['caption'] == 'true':
        video_caption = 'Available'
    else:
        video_caption = 'Unavailable'

    video_duration = video['contentDetails']['duration']
    video_duration = video_duration[2:]
    video_duration = video_duration.lower()
    if 'h' in video_duration:
        video_duration = video_duration.replace('h', ':')
        video_duration = video_duration.replace('m', ':')
        video_duration = video_duration.replace('s', '')
    elif 'm' in video_duration:
        video_duration = video_duration.replace('m', ':')
        video_duration = video_duration.replace('s', '')
        video_duration = '00:' + video_duration
    else:
        video_duration = video_duration.replace('s', '')
        video_duration = '00:00:' + video_duration

    video_duration = ':'.join([f'{int(d):02d}' for d in video_duration.split(':') if d != ''])

    video_details = {
        'Video_ID': video_id,
        'Video_Title': video['snippet']['title'],
        'Description': video['snippet']['description'],
        'PublishedAt': video['snippet']['publishedAt'],
        'Duration': video_duration,
        'Thumbnail': video['snippet']['thumbnails']['default']['url'],
        'Caption': video_caption,
        'View_Count': int(video['statistics']['viewCount']),
        'Like_Count': int(like_count),
        'Dislike_Count': int(dislike_count),
        'Favorite_Count': int(favorite_count),
        'Comment_Count': int(comment_count),
        'Comments': get_comments(youtube, video_id)
    }
    return video_details


# To get Comments of each video

def get_comments(youtube, video_id):
    next_page_token = None

    comments = {}

    while True:
        response = youtube.commentThreads().list(
            part='snippet,replies',
            textFormat='plainText',
            maxResults=100,
            pageToken=next_page_token,
            videoId=video_id).execute()

        for item in response['items']:
            comment_id = item['id']
            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            comment_published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']

            comment_details = {'CommentId': comment_id,
                               'Comment_Text': comment_text,
                               'Comment_author': comment_author,
                               'Comment_PublishedAt': comment_published_at}

            comments[f'Comment_{len(comments) + 1}'] = comment_details

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return comments


def get_all_video_details(youtube, video_ids):
    video_details_list = []
    for video_id in video_ids:
        video_details = get_video_details(youtube, video_id)
        video_details_list.append(video_details)

    return video_details_list


def get_channel_data():
    channel_data = {}
    channel_details = get_channel_details(youtube, channel_id)
    uploads_playlist_id = channel_details['Playlist_ID']
    video_ids = get_all_video_ids(youtube, uploads_playlist_id)
    video_details_list = get_all_video_details(youtube, video_ids)
    channel_data["Channel"] = channel_details
    for video_details in video_details_list:
        channel_data[f'Video_{len(channel_data)}'] = video_details

    return channel_data

def duration_to_seconds(duration):
    duration = str(duration)
    sec = 3600
    time_duration = 0
    for j in duration.split(":"):
        j = j.lstrip('0')
        if j.isdigit():
            s = int(j)
        else:
            s = 0
        time_duration += s * sec
        sec //= 60
    return time_duration

def to_datetime(published):
    if len(published) > 20:
        dt = datetime.strptime(published, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        dt = datetime.strptime(published, '%Y-%m-%dT%H:%M:%SZ')
    d_time = dt.strftime('%Y-%m-%d %H:%M:%S')
    return d_time




st.set_page_config(page_title='Youtube data harvesting')
st.header('Youtube data harvesting using youtube API ')
st.subheader('Data Collection through API')
channel_id = st.text_input('Enter the channel ID')

api_key="<api key>"
youtube = build("youtube","v3",developerKey = api_key)

mongo_connection = "<mongodb connection>"
database_name = "Youtube_data_harvesting"
collection_name = "channels"

client = MongoClient(mongo_connection)
mydb = client[database_name]
mycol = mydb[collection_name]

ytdb = mysql.connector.connect(host='localhost',
                                       user='root',
                                       password='<password>',
                                       database='ytdata')
cursor = ytdb.cursor()
# cursor.execute("create database ytdata")
# ytdb.commit()
#
# cursor.execute("""create table yChannel (Channel_Name varchar(255),
#                                         Channel_ID varchar(255) PRIMARY KEY,
#                                         Description TEXT,
#                                         Published_At DATETIME,
#                                         Thumbnail varchar(255),
#                                         Playlist_ID varchar(255),
#                                         Video_Count INT,
#                                         View_Count INT,
#                                         Subscribers_Count INT)""")
# ytdb.commit()
#
# cursor.execute("""create table yPlaylist (Playlist_ID varchar(255) PRIMARY KEY,
#                                          Channel_ID varchar(255),
#                                          FOREIGN KEY (Channel_ID) REFERENCES yChannel(Channel_ID))""")
# ytdb.commit()
#
# cursor.execute("""create table yVideo (Video_ID varchar(255) PRIMARY KEY,
#                                       Playlist_ID varchar(255),
#                                       FOREIGN KEY (Playlist_ID) REFERENCES yPlaylist(Playlist_ID),
#                                       Video_Title varchar(255),
#                                       Description TEXT,
#                                       Published_At DATETIME,
#                                       Duration INT,
#                                       Thumbnail varchar(255),
#                                       Caption varchar(255),
#                                       View_Count INT,
#                                       Like_Count INT,
#                                       Dislike_Count INT,
#                                       Favorite_Count INT,
#                                       Comment_Count INT)""")
# ytdb.commit()
#
# cursor.execute("""create table yComment(Comment_ID varchar(255) PRIMARY KEY,
#                                        Video_ID varchar(255),
#                                        FOREIGN KEY (Video_ID) REFERENCES yVideo(Video_ID),
#                                        Comment_Text TEXT,
#                                        Comment_Author varchar(255),
#                                        Published_At DATETIME)""")
# ytdb.commit()




if channel_id:
    channel_details = get_channel_data()
    st.write('Channel Details:', channel_details)      ## To display the fetched channel details

channel_names = mycol.distinct('Channel.Channel_Name')

if st.button('Store Data in MongoDB'):
    if channel_details['Channel']['Channel_Name'] not in channel_names:
        mycol.insert_one(channel_details)
        st.write('Data stored in MongoDB')
    channel_names = mycol.distinct('Channel.Channel_Name')     # Dropdown list of channels added to MongoDB

if channel_names != []:
    selected_channel = st.selectbox('Select a channel', channel_names)     # Dropdown selection box

### Migrating selected channel to SQL Database
if st.button('Migrate to SQL Database'):
    yt_data = mycol.find_one({'Channel.Channel_Name': selected_channel})
    ###Migrating channel details into channel table
    ch_id_query = 'select * from ychannel where Channel_ID = %s'
    ch_id_value = (yt_data['Channel']['Channel_ID'],)
    cursor.execute(ch_id_query, ch_id_value)
    if not cursor.fetchall():
        channel_query = 'insert into yChannel values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
        channel_values = (yt_data['Channel']['Channel_Name'], yt_data['Channel']['Channel_ID'],
                          yt_data['Channel']['Description'], to_datetime(yt_data['Channel']['Published']),
                          yt_data['Channel']['Thumbnail'], yt_data['Channel']['Playlist_ID'],
                          yt_data['Channel']['video_Count'], yt_data['Channel']['view_Count'],
                          yt_data['Channel']['Subscribers_Count'])
        cursor.execute(channel_query, channel_values)
        ytdb.commit()
    ###Migrating playlist details into playlist table
    pl_id_query = 'select * from yplaylist where Playlist_ID = %s'
    pl_id_value = (yt_data['Channel']['Playlist_ID'],)
    cursor.execute(pl_id_query, pl_id_value)
    if not cursor.fetchall():
        playlist_query = 'insert into yPlaylist values (%s, %s)'
        playlist_values = (yt_data['Channel']['Playlist_ID'], yt_data['Channel']['Channel_ID'])
        cursor.execute(playlist_query, playlist_values)
        ytdb.commit()

    for i in range(yt_data['Channel']['video_Count']):
        i += 1
        video = 'Video_'+str(i)
        ###Migrating video details into video table
        vd_id_query = 'select * from yvideo where Video_ID = %s'
        vd_id_value = (yt_data[video]['Video_ID'],)
        cursor.execute(vd_id_query, vd_id_value)
        if not cursor.fetchall():
            video_query = 'insert into yvideo (Video_ID, Playlist_ID, Video_Title, Description, Published_At, Duration, Thumbnail, Caption, View_Count, Like_Count, Dislike_Count, Favorite_Count, Comment_Count) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            video_values = (yt_data[video]['Video_ID'], yt_data['Channel']['Playlist_ID'], yt_data[video]['Video_Title'],
                            yt_data[video]['Description'], to_datetime(yt_data[video]['PublishedAt']),
                            duration_to_seconds(yt_data[video]['Duration']), yt_data[video]['Thumbnail'],
                            yt_data[video]['Caption'], yt_data[video]['View_Count'], yt_data[video]['Like_Count'],
                            yt_data[video]['Dislike_Count'], yt_data[video]['Favorite_Count'],
                            yt_data[video]['Comment_Count'])
            cursor.execute(video_query, video_values)
            ytdb.commit()

        for j in range(len(yt_data[video]['Comments'])):
            j += 1
            comment = 'Comment_'+str(j)
            ###Migrating comment details into comment table
            cmt_id_query = 'select * from ycomment where Comment_ID = %s'
            cmt_id_value = (yt_data[video]['Comments'][comment]['CommentId'],)
            cursor.execute(cmt_id_query, cmt_id_value)
            if not cursor.fetchall():
                comment_query = 'insert into ycomment (Comment_ID, Video_ID, Comment_Text, Comment_Author, Published_At) values(%s, %s, %s, %s, %s)'
                comment_values = (yt_data[video]['Comments'][comment]['CommentId'], yt_data[video]['Video_ID'],
                                  yt_data[video]['Comments'][comment]['Comment_Text'],
                                  yt_data[video]['Comments'][comment]['Comment_author'],
                                  to_datetime(yt_data[video]['Comments'][comment]['Comment_PublishedAt']))
                cursor.execute(comment_query, comment_values)
                ytdb.commit()

    st.write('Data migrated to SQL Database')

###Performing SQL queries over the migrated data
query_list = ['Names of all the videos and their corresponding channels','Channel with most number of videos and its video count',
              'Top 10 most viewed videos with their channel name', 'Number of comments on each video with channel name',
              'Videos with highest number of likes with channel name', 'Number of likes and dislikes of each video', 'Total views of each channel',
              'Names of all the channels that have published videos in the year 2022', 'Average duration of videos in each channel',
              'Videos with most comments with channel name']

selected_query = st.selectbox('Select a query',query_list)

if st.button('Find'):
    if selected_query == 'Names of all the videos and their corresponding channels':
        cursor.execute('select yvideo.Video_Title, ychannel.Channel_Name from yvideo  join ychannel on yvideo.Playlist_ID = ychannel.Playlist_ID')
        result = cursor.fetchall()
        df = pd.DataFrame.from_records(result,columns=['Video_Title', 'Channel_Name'])
        st.dataframe(df)

    elif selected_query == 'Channel with most number of videos and its video count':
        query = 'select Channel_name, Video_Count from ychannel ORDER BY Video_Count DESC LIMIT 3'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Top 10 most viewed videos with their channel name':
        query = 'select yvideo.Video_Title, ychannel.Channel_name from yvideo join ychannel ORDER BY yvideo.View_Count DESC LIMIT 10'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Number of comments on each video with channel name':
        query = 'select yvideo.Video_Title, yvideo.Comment_Count, ychannel.Channel_Name from yvideo  join ychannel on yvideo.Playlist_ID = ychannel.Playlist_ID'
        df = pd.read_sql_query(query,ytdb)
        st.dataframe(df)

    elif selected_query == 'Videos with highest number of likes with channel name':
        query = 'select yvideo.Video_Title, yvideo.Like_Count, ychannel.Channel_name from yvideo join ychannel on yvideo.Playlist_ID = ychannel.Playlist_ID ORDER BY yvideo.Like_Count DESC LIMIT 10'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Number of likes and dislikes of each video':
        query = 'select yvideo.Video_Title, yvideo.Like_Count, yvideo.Dislike_Count from yvideo'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Total views of each channel':
        query = 'select ychannel.Channel_Name, ychannel.View_Count from ychannel'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Names of all the channels that have published videos in the year 2022':
        query = 'select distinct c.Channel_Name from ychannel c join yvideo v on c.Playlist_ID = v.Playlist_ID where year(v.Published_At)=2022'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Average duration of videos in each channel':
        query = 'select c.Channel_Name, AVG(v.Duration) as avg_duration_seconds from ychannel c join yvideo v on c.Playlist_ID = v.Playlist_ID GROUP BY c.Channel_Name'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)

    elif selected_query == 'Videos with most comments with channel name':
        query = 'select yvideo.Video_Title, yvideo.Comment_Count, ychannel.Channel_Name from yvideo  join ychannel on yvideo.Playlist_ID = ychannel.Playlist_ID ORDER BY yvideo.Comment_Count DESC LIMIT 10'
        df = pd.read_sql_query(query, ytdb)
        st.dataframe(df)