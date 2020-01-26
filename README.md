What is this program?
=====================
    - This program is using two datasets that reside in S3 then store them in staging then tables.


Components:
===========
   * dwh.cfg
    ---------
        The file contains all the needed information to connect to AWS.
        These information are given the authority to conecct to database.
        
   * sql_queries.py
    ----------------
        It contains all the drop, create, copy, and insert queries
        There is a star table is builded by the queries 
            1. The fact table is songplays
            2. The dimension tables are: users, songs, artists, time
        The fact table is "SONGPLAYS" which is connected to other through foreign key as following:
            * SONGPLAYS -> songplay_id (PK)
            * SONGPLAYS -> user_id     (FK)   WITH   USERS   -> user_id    (PK)
            * SONGPLAYS -> song_id     (FK)   WITH   SONGS   -> song_id    (PK)
            * SONGPLAYS -> artist_id   (FK)   WITH   ARTISTS -> artist_id  (PK)
            * SONGPLAYS -> start_time  (FK)   WITH   TIME    -> start_time (PK)
        Where (PK) is primary key, and (FK) is foreign key.
        
   * create_schemas.py
    ------------------
        This file is connecting to database then drop all the privous schemas then create them again.
        Also, create all the tables in these schemas.
        
   * etl.py
    --------
        This file is connecting to database then load the data from datasets then store them in staging and tables.
    
    
How to run it?  
==============
    - you should run 'create_schemas.py' first to create schema and tables: 
            python create_schemas.py
    - If you created the schemas and tables then you can run 'etl.py'
            python etl.py

    