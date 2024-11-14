Database Schema desgin

Staging Table:
1. staging_songs: stores data extracted from JSON metadata on the songs in the app. Columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year
2. staging_events: stores data extracted from JSON logs on user activity. Columns: artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId

Fact Table:
songplay: store information about 
