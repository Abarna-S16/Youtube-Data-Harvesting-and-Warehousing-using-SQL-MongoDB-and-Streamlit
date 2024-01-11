
# Youtube API libraries
import googleapiclient.discovery
from googleapiclient.discovery import build

# File Handling libraries
import re

# Data Analysis libraries
import pandas as pd

# SQL libraries
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine,text
import pymysql

# Data Visualization
import plotly.express as px

#User Interface (Web App)
import streamlit as st
from streamlit_option_menu import option_menu


#------------------------------------------------Page Setup-------------------------------------------------------------#

st.set_page_config(layout='wide')
with st.sidebar:
  choice = option_menu(None, ["Home","Channel Data to MongoDB","SQL Warehouse","Channel Data Analysis and Visualization"],
                           icons=["house-door-fill","database-fill-up","database-fill-check","bar-chart-fill"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "25px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "25px"},
                                   "container" : {"max-width": "9000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})

         #--------Navigating between left tabs--------#
if choice=="Home":

  st.image('/content/Home.png')

if choice=="Channel Data to MongoDB":

  #---------------------------Data Collection and Data Warehousing----------------------#

  #-------Function call to get channel data---------#
  col1,col2=st.columns(2)
  with col1:
    st.title('Data Collection')
    st.subheader('Enter Channel Id')
    channel_id = st.text_input('Channel Id')
    data=st.button('Get Data')

    # Define Session state to Get data button
    if "Get_state" not in st.session_state:
        st.session_state.Get_state = False
    if data or st.session_state.Get_state:
      st.session_state.Get_state = True
      with st.spinner('Fetching the Channel Data...'):

        #-----Getting access from youtube API-----#
        api_key="AIzaSyD0EgysaAJ7tGN175s8pUgx6leIbpuLeOc"
        youtube=build("youtube","v3",developerKey=api_key)

        #-----A function to get channel data-----#
        def get_channel_data(youtube,channel_id):
          channel_request = youtube.channels().list(
              part = 'snippet,statistics,contentDetails',
              id =channel_id)
          channel_response=channel_request.execute()
          return channel_response
        channel_data=get_channel_data(youtube,channel_id)

        #----------Extract necessary information from channel data------------#
        channel_name=channel_data['items'][0]['snippet']['title']
        channel_Id=channel_data['items'][0]['id']
        channel_videos=channel_data['items'][0]['statistics']['videoCount']
        subscription_count=channel_data['items'][0]['statistics']['subscriberCount']
        channel_views=channel_data['items'][0]['statistics']['viewCount']
        channel_description=channel_data['items'][0]['snippet']['description']
        playlist_id=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        #--------Framing dictionary for MongoDB Documents--------#
        channel={
            "Channel_Details":{
                "Channel_Id":channel_Id,
                "Channel_Name":channel_name,
                "Channel_Videos":channel_videos,
                "Channel_Views":channel_views,
                "No_of_Subscribers":subscription_count,
                "Playlist_Id":playlist_id,
                "Channel_Description":channel_description

            }
        }

        #-----------A Function to get video ids from the playlists-------------#
        def get_video_ids(youtube,playlist_id):
          video_id=[]
          next_page_token=None
          while True:
            video_request=youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            video_response=video_request.execute()

            for item in video_response['items']:
              video_id.append(item['contentDetails']['videoId'])
            next_page_token=video_response.get('nextPageToken')
            if not next_page_token:
              break
          return video_id

        #-------Function call to get video ids-----------#
        video_ids=get_video_ids(youtube,playlist_id)


        #-------A Function to get Comment details-----------#
        def get_video_comments(youtube, video_id,no_of_comments):
          comment_request = youtube.commentThreads().list(
              part='snippet',
              maxResults=no_of_comments,
              textFormat="plainText",
              videoId=video_id)
          comment_response = comment_request.execute()
          return comment_response

        #--------A Function to convert duartion---------#
        def convert_duration(duration):
          regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
          match = re.match(regex, duration)
          if not match:
              return '00:00:00'
          hours, minutes, seconds = match.groups()
          hours = int(hours[:-1]) if hours else 0
          minutes = int(minutes[:-1]) if minutes else 0
          seconds = int(seconds[:-1]) if seconds else 0
          total_seconds = hours * 3600 + minutes * 60 + seconds
          return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))

        #--------A function to get video data--------------#
        def get_video_data(youtube,video_ids):
          video_data=[]
          for video_id in video_ids:
            video_request=youtube.videos().list(
                part='contentDetails,snippet,statistics',
                id=video_id
            )
            video_response=video_request.execute()
            video = video_response['items'][0]

            # Check for comments if available and include them
            try:
                video['comments'] = get_video_comments(youtube, video_id,no_of_comments=2)
            except:
                video['comments'] = None

            # Transform duration format
            duration = video.get('contentDetails', {}).get('duration', 'Not Available')
            if duration != 'Not Available':
                duration = convert_duration(duration)
            video['contentDetails']['duration'] = duration

            video_data.append(video)

          return video_data

        #--------Function call to get video and comment data from all videos of the channel--------#
        video_data = get_video_data(youtube, video_ids)

        #--------Extracting necessary details of video and comment data for MongoDB Document---------------#
        videos_and_comments={}
        for i,video in enumerate(video_data):
          video_id=video['id']
          video_name=video['snippet']['title']
          video_description=video['snippet']['description']
          video_tags=video['snippet'].get('tags',[])
          published_at=video['snippet']['publishedAt']
          view_count=video['statistics']['viewCount']
          like_count=video['statistics'].get('likeCount',0)
          dislike_count=video['statistics'].get('dislikeCount',0)
          favorite_count=video['statistics'].get('favoriteCount',0)
          comment_count=video['statistics'].get('commentCount',0)
          duration=video['contentDetails'].get('duration','Not Available')
          thumbnail=video['snippet']['thumbnails']['high']['url']
          caption_status = video.get('contentDetails', {}).get('caption', 'Not Available')
          comments = 'None'

          # if comments available
          if video['comments'] is not None:
              comments = {}
              for index, comment in enumerate(video['comments']['items']):
                  comment_id = comment['id']
                  comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                  comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                  comment_published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                  comments[f"Comment_Id_{index + 1}"] = {
                      'Comment_Id': comment_id,
                      'Comment_Text': comment_text,
                      'Comment_Author': comment_author,
                      'Comment_PublishedAt': comment_published_at
                  }

        #------------Framing extracted video and comments data into a dictionary for MongoDB document--------------#
          videos_and_comments[f"Video_{i + 1}"] = {
              'Video_Id': video_id,
              'Video_Name': video_name,
              'Video_Description': video_description,
              'Tags': video_tags,
              'PublishedAt': published_at,
              'View_Count': view_count,
              'Like_Count': like_count,
              'Dislike_Count': dislike_count,
              'Favorite_Count': favorite_count,
              'Comment_Count': comment_count,
              'Duration': duration,
              'Thumbnail': thumbnail,
              'Caption_Status': caption_status,
              'Comments': comments
          }
        video_and_comment={
            "Videos":videos_and_comments
        }

        #-------- Merge two dictionaries (channel and videos_and_comments) in a single final dictionary
        def merge_final_dict(channel , video_and_comment):
          res = {**channel, **video_and_comment}
          return res

        final_dictionary = merge_final_dict(channel,video_and_comment)

        st.json(final_dictionary)
      st.success('Done!')
    #---------------------------------------------End of Get Data---------------------------------------------------------#

  with col2:

    st.title('Data Warehousing')
    st.subheader('The channel data is ready to be uploaded in MongoDB')
    #---------------MongoDB Atlas connection-------------#
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    import urllib
    encoded_password=urllib.parse.quote_plus("sweetdevil")       #------------to parse special characters ------------------
    uri = f"mongodb+srv://abarna:{encoded_password}@cluster0.x7sznmp.mongodb.net/?retryWrites=true&w=majority"

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    database=client['Youtube']
    channels=database['Channels']
    mongodb=st.button('Upload to MongoDB')

    # Define Session state to Upload to MongoDB button
    if 'upload_mongodb' not in st.session_state:
        st.session_state_upload_mongodb = False
    if mongodb or st.session_state_upload_mongodb:
      st.session_state_upload_mongodb = True

      #-------------insert documents in MongoDB-----------#
      final_data = {
                  'Channel_Name': channel_name,
                  "Channel_data":final_dictionary
                  }
      upload=channels.replace_one({"Channel_Id":channel_Id}, final_data, upsert=True)
      st.success('Uploaded Successfully')
      client.close()

    #-----------------------------------End of Upload to MongoDB------------------------------------------------#

if choice=="SQL Warehouse":
  st.title("Data Migration to MySQL")
  # Connect to the MongoDB server
  #---------------MongoDB Atlas connection-------------#
  from pymongo.mongo_client import MongoClient
  from pymongo.server_api import ServerApi
  import urllib
  encoded_password=urllib.parse.quote_plus("sweetdevil")       #------------to parse special characters ------------------#
  uri = f"mongodb+srv://abarna:{encoded_password}@cluster0.x7sznmp.mongodb.net/?retryWrites=true&w=majority"

  # Create a new client and connect to the server
  client = MongoClient(uri, server_api=ServerApi('1'))

  # Send a ping to confirm a successful connection
  try:
      client.admin.command('ping')
      print("Pinged your deployment. You successfully connected to MongoDB!")
  except Exception as e:
      print(e)

  database=client['Youtube']
  channels=database['Channels']

  # Collect all document names and give them
  available_channels = []
  for document in channels.find():
      available_channels.append(document["Channel_Name"])
  st.write("Select the channel name which you want to migrate to SQL from the dropdown below")
  channel_name_for_sql = st.selectbox(label='Select a Channel name', options = available_channels, key='available_channels')
  migrate = st.button('Migrate to MySQL')

    # Define Session state to Migrate to MySQL button
  if 'migrate_sql' not in st.session_state:
      st.session_state_migrate_sql = False
  if migrate or st.session_state_migrate_sql:
      st.session_state_migrate_sql = True
      with st.spinner('Migrating to SQL...'):
        # Retrieve the document with the specified name
        document = channels.find_one({"Channel_Name": channel_name_for_sql})
        client.close()

        #-------------------------- Data Conversion from document to necessary DataFrames (Channel,Playlist,Comment,Video)------------------#

        # ----Channel Dataframe-----#
        channel_dict = {
                    "Channel_Name": document['Channel_Name'],
                    "Channel_Id": document['Channel_data']['Channel_Details']['Channel_Id'],
                    "Video_Count": document['Channel_data']['Channel_Details']['Channel_Videos'],
                    "Subscriber_Count": document['Channel_data']['Channel_Details']['No_of_Subscribers'],
                    "Channel_Views": document['Channel_data']['Channel_Details']['Channel_Views'],
                    "Channel_Description": document['Channel_data']['Channel_Details']['Channel_Description'],
                    "Playlist_Id": document['Channel_data']['Channel_Details']['Playlist_Id']
                    }

        channel_df = pd.DataFrame.from_dict(channel_dict,orient='index').T

        #-------------A function to retrieve playlist data--------------#

        #-----Getting access from youtube API-----#
        api_key="AIzaSyD0EgysaAJ7tGN175s8pUgx6leIbpuLeOc"
        youtube=build("youtube","v3",developerKey=api_key)
        channel_id=document['Channel_data']['Channel_Details']['Channel_Id']
        def get_playlist_data(youtube,channel_id):
          playlists=[]
          next_page_token=None
          while True:
            playlist_request=youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            playlist_response=playlist_request.execute()

            for item in playlist_response['items']:
              playlists.append({
                "Channel_Id":item['snippet']['channelId'],
                "Playlist_Id": item['id'],
                "title":item['snippet']['title']})
            next_page_token=playlist_response.get('nextPageToken')
            if not next_page_token:
              break
          return playlists

        #----------Function call to get playlist data---------------#
        playlist_data=get_playlist_data(youtube,channel_id)

        # ----Playlist Dataframe-----#

        playlist_df=pd.DataFrame(playlist_data)

        #-----Video Dataframe------#
        videos_dict = []
        final_range=int(document['Channel_data']['Channel_Details']['Channel_Videos'])
        for i in range(1,final_range):
          video_dict={
              'Video_Id': document['Channel_data']['Videos'][f"Video_{i}"]['Video_Id'],
              'Channel_Id':document['Channel_data']['Channel_Details']['Channel_Id'],
              'Video_Name': document['Channel_data']['Videos'][f"Video_{i}"]['Video_Name'],
              'Video_Description': document['Channel_data']['Videos'][f"Video_{i}"]['Video_Description'],
              'Published_date': document['Channel_data']['Videos'][f"Video_{i}"]['PublishedAt'],
              'View_Count': document['Channel_data']['Videos'][f"Video_{i}"]['View_Count'],
              'Like_Count': document['Channel_data']['Videos'][f"Video_{i}"]['Like_Count'],
              'Dislike_Count': document['Channel_data']['Videos'][f"Video_{i}"]['Dislike_Count'],
              'Favorite_Count': document['Channel_data']['Videos'][f"Video_{i}"]['Favorite_Count'],
              'Comment_Count': document['Channel_data']['Videos'][f"Video_{i}"]['Comment_Count'],
              'Duration': document['Channel_data']['Videos'][f"Video_{i}"]['Duration'],
              'Thumbnail': document['Channel_data']['Videos'][f"Video_{i}"]['Thumbnail'],
              'Caption_Status': document['Channel_data']['Videos'][f"Video_{i}"]['Caption_Status']
              }
          videos_dict.append(video_dict)
        video_df = pd.DataFrame(videos_dict)

        #------Comment Dataframe------#
        comment_dicts = []
        for i in range(1,final_range):
            comments_access = document['Channel_data']['Videos'][f"Video_{i}"]['Comments']
            if comments_access == 'Unavailable' or ('Comment_Id_1' not in comments_access or 'Comment_Id_2' not in comments_access) :
                comment_dict = {
                    'Video_Id': 'Unavailable',
                    'Comment_Id': 'Unavailable',
                    'Comment_Text': 'Unavailable',
                    'Comment_Author':'Unavailable',
                    'Comment_Published_date': 'Unavailable',
                    }
                comment_dicts.append(comment_dict)

            else:
                for j in range(1,3):
                    comment_dict = {
                    'Video_Id': document['Channel_data']['Videos'][f"Video_{i}"]['Video_Id'],
                    'Comment_Id': document['Channel_data']['Videos'][f"Video_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Id'],
                    'Comment_Text': document['Channel_data']['Videos'][f"Video_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Text'],
                    'Comment_Author': document['Channel_data']['Videos'][f"Video_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_Author'],
                    'Comment_Published_date': document['Channel_data']['Videos'][f"Video_{i}"]['Comments'][f"Comment_Id_{j}"]['Comment_PublishedAt'],
                    }
                    comment_dicts.append(comment_dict)
        comment_df = pd.DataFrame(comment_dicts)

        #-------------------------Data Migration to MySQL-------------------------#
        #credentials to connect to the database
        host='0.tcp.in.ngrok.io'
        port=18720
        user='root'
        password='abarna16'
        charset='utf8'
        database='testdb'

        #--------------------Connection from Google colab to local MySql Server----------------#
        engine=create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        con=engine.connect()

        # Channel data to SQL
        channel_df.to_sql('Channel', engine, if_exists='append', index=True, index_label=None,
                        dtype = {"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                "Video_Count": sqlalchemy.types.INT,
                                "Subscriber_Count": sqlalchemy.types.BigInteger,
                                "Channel_Views": sqlalchemy.types.BigInteger,
                                "Channel_Description": sqlalchemy.types.TEXT,
                                "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),})
        # Playlist data to SQL
        playlist_df.to_sql('Playlist', engine, if_exists='append', index=True, index_label=None,
                          dtype={"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                  "Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                                  "playlist_Name":sqlalchemy.types.VARCHAR(length=225),})
        # Comment data to SQL
        comment_df.to_sql('Comment', engine, if_exists='append', index=True,index_label=None,
                                dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                        'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                        'Comment_Text': sqlalchemy.types.TEXT,
                                        'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                        'Comment_Published_date': sqlalchemy.types.String(length=50),})
        # Video data to SQL
        video_df.to_sql('Video', engine, if_exists='append', index=True,index_label=None,
                          dtype = {'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                  'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                                  'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                                  'Video_Description': sqlalchemy.types.TEXT,
                                  'Published_date': sqlalchemy.types.String(length=50),
                                  'View_Count': sqlalchemy.types.BigInteger,
                                  'Like_Count': sqlalchemy.types.BigInteger,
                                  'Dislike_Count': sqlalchemy.types.INT,
                                  'Favorite_Count': sqlalchemy.types.INT,
                                  'Comment_Count': sqlalchemy.types.INT,
                                  'Duration': sqlalchemy.types.VARCHAR(length=1024),
                                  'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                                  'Caption_Status': sqlalchemy.types.VARCHAR(length=225),})

      st.success('Migrated Successfully !')
      client.close()

if choice=="Channel Data Analysis and Visualization":
  st.title("Channels Data Analysis")
  #-----------------------Channel Data Analysis------------------------#

  # Connection and cursor for query
  conn = pymysql.connect(
      host='0.tcp.in.ngrok.io',
      port=18720,
      user='root',
      password='abarna16',
      charset='utf8',
      database='testdb'
          )
  cursor = conn.cursor()
  #--------Showing available channels in MySql-----------#
  st.subheader("The available channels for analysis")
  cursor.execute("SELECT Channel_Name FROM Channel;")
  result=cursor.fetchall()
  df=pd.DataFrame(result,columns=['Channels']).reset_index(drop=True)
  df.index +=1
  st.dataframe(df)

  analysis_question = st.selectbox(label='Select a Query',
    options=('1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'), key = 'collection_question')

  # Query 1
  if analysis_question == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute("SELECT Channel.Channel_Name ,Video.Video_name from Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id;")
    result_1 = cursor.fetchall()
    df1 = pd.DataFrame(result_1, columns=['Channel Name','Video Name']).reset_index(drop=True)
    df1.index += 1
    st.dataframe(df1)

  # Query 2
  elif analysis_question =='2. Which channels have the most number of videos, and how many videos do they have?':
    cursor.execute("SELECT Channel_Name, Video_Count FROM Channel ORDER BY Video_Count DESC;")
    result_2 = cursor.fetchall()
    df2 = pd.DataFrame(result_2,columns=['Channel Name','Video Count']).reset_index(drop=True)
    df2.index += 1
    st.dataframe(df2)
    fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
    fig_vc.update_traces(textfont_size=16,marker_color='#C80101')
    fig_vc.update_layout(title_font_color='#000000 ',title_font=dict(size=25))
    st.plotly_chart(fig_vc,use_container_width=True)

  # Query 3
  elif analysis_question == '3. What are the top 10 most viewed videos and their respective channels?':
    cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.View_Count FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id ORDER BY Video.View_Count DESC LIMIT 10;")
    result_3 = cursor.fetchall()
    df3 = pd.DataFrame(result_3,columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
    df3.index += 1
    st.dataframe(df3)
    fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
    fig_topvc.update_traces(textfont_size=16,marker_color='#C80101')
    fig_topvc.update_layout(title_font_color='#000000 ',title_font=dict(size=25))
    st.plotly_chart(fig_topvc,use_container_width=True)

  # Query 4
  elif analysis_question == '4. How many comments were made on each video, and what are their corresponding video names?':
    cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Comment_Count FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id;")
    result_4 = cursor.fetchall()
    df4 = pd.DataFrame(result_4,columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
    df4.index += 1
    st.dataframe(df4)

  # Query 5
  elif analysis_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Like_Count FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id ORDER BY Video.Like_Count DESC;")
    result_5= cursor.fetchall()
    df5 = pd.DataFrame(result_5,columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
    df5.index += 1
    st.dataframe(df5)

  # Query 6
  elif analysis_question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    cursor.execute("SELECT Video.Video_Name, Video.Like_Count, Video.Dislike_Count FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id ORDER BY Video.Like_Count DESC;")
    result_6= cursor.fetchall()
    df6 = pd.DataFrame(result_6,columns=['Video Name', 'Like count','Dislike count']).reset_index(drop=True)
    df6.index += 1
    st.dataframe(df6)

  # Query 7
  elif analysis_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    cursor.execute("SELECT Channel_Name, Channel_Views FROM Channel ORDER BY Channel_Views DESC;")
    result_7= cursor.fetchall()
    df7 = pd.DataFrame(result_7,columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
    df7.index += 1
    st.dataframe(df7)

    fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s', title="Total number of views", )
    fig_topview.update_traces(textfont_size=16,marker_color='#C80101')
    fig_topview.update_layout(title_font_color='#000000 ',title_font=dict(size=25))
    st.plotly_chart(fig_topview,use_container_width=True)

  # Query 8
  elif analysis_question == '8. What are the names of all the channels that have published videos in the year 2022?':
    cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Published_date FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
    result_8= cursor.fetchall()
    df8 = pd.DataFrame(result_8,columns=['Channel Name','Video Name', 'Published in 2022']).reset_index(drop=True)
    df8.index += 1
    st.dataframe(df8)

  # Query 9
  elif analysis_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cursor.execute("SELECT Channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(Video.Duration)))), '%H:%i:%s') AS Duration FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id GROUP by Channel_Name ORDER BY duration DESC ;")
    result_9= cursor.fetchall()
    df9 = pd.DataFrame(result_9,columns=['Channel Name','Average duration of videos']).reset_index(drop=True)
    df9.index += 1
    st.dataframe(df9)

  # Query 10
  elif analysis_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cursor.execute("SELECT Channel.Channel_Name, Video.Video_Name, Video.Comment_Count FROM Channel JOIN Video ON Channel.Channel_Id = Video.Channel_Id ORDER BY Video.Comment_Count DESC;")
    result_10= cursor.fetchall()
    df10 = pd.DataFrame(result_10,columns=['Channel Name','Video Name', 'Number of comments']).reset_index(drop=True)
    df10.index += 1
    st.dataframe(df10)

  conn.close()

  #---------------------------------------------------------------------*******_END_*******------------------------------------------------------------------------------#


