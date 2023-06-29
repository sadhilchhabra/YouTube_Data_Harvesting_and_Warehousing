# YouTube_Data_Harvesting_and_Warehousing

The purpose of this project is to create an intuitive Streamlit app that pulls data about a YouTube channel from the Google API, stores it in a MongoDB database, moves it to a SQL data warehouse, and then lets users perform searches for channel details and join tables to view the data.
#### Link : <a href="https://www.linkedin.com/posts/activity-7080057017985359873-U-32?utm_source=share&utm_medium=member_desktop" target="_blank">LinkedIn Post / Working Model Video</a>
<br/>

## Prerequisites
1. **Python** -- Programming Language
2. **pymongo** -- Python Framework that helps in connecting with MongoDB
3. **pandas** -- Python Library for Data Visualization
4. **streamlit** -- Python framework to rapidly build and share beautiful machine learning and data science web apps
5. **google-api-python-client** -- Python Library that offers simple, flexible access to many Google APIs.
6. **mysql-connector-python** -- Python Library that enables Python programs to access MySQL databases

<br/>
   
## Project Setup
1. Firstly install all the required extensions in the requirements.txt
   
   > pip install -r requirements.txt

2. Now one need setup a Google Cloud Project on <a href="https://console.developers.google.com/" target="_blank">Google Cloud Console</a>, and then enable the Youtube API v3, after that generate the credentials and copy the api_key. Now below is the Python code to use that API.

   ```
   youtube = build('youtube', 'v3', developerKey="your api_key goes here")
   ```

3. After that one need to create a Database Cluster in MongoDB Atlas on <a href="https://www.mongodb.com/atlas/database" target="_blank">MongoDB Atlas</a>, and then create the required user (that can access that database cluster) for that database cluster and at last get your connection link for that database cluster. Now below is the Python code to connect to that Database Cluster.

   ```
   client = pymongo.MongoClient("your connection id goes here")
   db = client["youtubeData"]
   ```

4. After that one need to create a MySQL Database in there local system. Now below is the Python code to connect to that SQL Database.

    ```
    hostname = "your host name goes here"
    database = "your database name goes here"
    username = "your username goes here"
    pwd = "your password goes here"
  
    mydb = sql.connect(host=hostname, user=username, password=pwd, database=database)
                       
    cursor1 = mydb.cursor()
    ```

5. To run the application

    > streamlit run main.py

<br/>

## Project Workflow
1. Enter a YouTube channel ID in the input field and click the "Extract Data From API" button.
   
2. The app will retrieve the channel details like  Channel_id, Channel_name Playlist_id, Subscribers, Views, Total_videos, Description and so on.

3. Now to upload the data to MongoDB Atlas Database Cluster, click the "Upload data to MongoDB Atlas" button. The app will show a success message after the data is been uploaded successfully.
   
4. After the data gets uploded to MongoDB Atlas, now from the sidebar select the Migrate tab.  Now select the channel whose data you want to migrate to the SQL database from the dropdown menu.
   
5. Then click the "Migrate Data to SQL" button to migrate the selected channel data to SQL Database.

6. The app will display a success message once the data has been migrated.
   
7. Now from the sidebar select the View tab and browse through the dropdown menu and select the required statement.
    
8. According to the selected statement the data will be queried from the SQL Database  and will be displayed here on the screen in the streamlit application.
