# **Project Overview**

In this project, I developed an ETL pipeline for *Sparkify*, a growing music streaming startup. Sparkify is scaling its user base and song database and aims to transition its data processes to the cloud. Their current data resides in Amazon S3, comprising JSON logs of user activity on the app and JSON metadata on the songs available in their platform.

As the data engineer, I designed an ETL pipeline to extract data from S3, stage it in Amazon Redshift, and transform it into dimensional tables. This setup enables the analytics team to gain insights into user listening patterns and song preferences.

# **Database Schema Design**

## **Staging Tables**
1. **staging_songs**: This table stores data extracted from the JSON metadata files about songs in the app.  
   **Columns**: `num_songs`, `artist_id`, `artist_latitude`, `artist_longitude`, `artist_location`, `artist_name`, `song_id`, `title`, `duration`, `year`.

2. **staging_events**: This table holds data from JSON logs on user activity.  
   **Columns**: `artist`, `auth`, `firstName`, `gender`, `itemInSession`, `lastName`, `length`, `level`, `location`, `method`, `page`, `registration`, `sessionId`, `song`, `status`, `ts`, `userAgent`, `userId`.

## **Fact Table**
- **songplay**: This table captures information related to each songplay event, providing data points essential for tracking song interactions.  
  **Columns**: `user_id`, `song_id`, `artist_id`, `start_time`, `session_id`, `item_in_session`, `length`, `auth`, `level`, `method`, `location`, `page`, `user_agent`, `registration`, `status`.

## **Dimension Tables**
1. **user**: This table stores user-specific information.  
   **Columns**: `user_id`, `first_name`, `last_name`, `gender`, `level`.

2. **songs**: This table holds song-specific details.  
   **Columns**: `song_id`, `artist_id`, `title`, `year`, `duration`.

3. **artists**: This table stores artist information.  
   **Columns**: `artist_id`, `name`, `num_songs`, `location`, `latitude`, `longitude`.

4. **time**: This table provides time-related data that links to the songplay events, which can be derived from the `ts` column in `staging_events`.  
   **Columns**: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`.

# **Example Analytical Queries**

In this project, I’ve designed example queries that allow Sparkify to explore user engagement and song popularity. These queries serve as foundational analyses rather than highly optimized solutions, providing Sparkify’s analytics team with a starting point for gaining insights:

1. **Top Played Songs**  
   This query identifies the songs most frequently played by users, giving Sparkify a preliminary view of popular tracks and audience preferences.

2. **Peak Usage Hours**  
   This query examines the times of day with the highest user activity, helping to reveal when users are most engaged on the app.

These initial queries enable Sparkify to start making data-driven decisions, with potential for further tuning to maximize insight and efficiency as the platform grows.
