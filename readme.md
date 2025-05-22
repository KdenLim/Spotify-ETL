A data pipeline which retrieves tracks played in the last 24 hours on Spotify and stores them in a MySQL database. The pipeline is scheduled to run daily on Apache-Airflow which you can run with docker-compose (Airflow's yaml file is in the repo). Spotify's API documentation can be found [here](https://developer.spotify.com/documentation/web-api).

~

MySQL query example:
![image](https://github.com/user-attachments/assets/747d55c8-a4e7-477d-bab9-323c616d24eb)

