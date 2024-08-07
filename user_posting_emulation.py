import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
import pymysql



random.seed(100)


class AWSDBConnector:

    def __init__(self):

        pass

    def read_db_creds(self):
        '''
        Reads a credential yaml file to return a dictonary of the database credentials.
        '''
        with open('db_creds.yaml', 'r') as db_creds:
            db_creds = yaml.safe_load(db_creds)
            return db_creds
        
    def create_db_connector(self):
        cred = self.read_db_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{cred['USER']}:{cred['PASSWORD']}@{cred['HOST']}:{cred['PORT']}/{cred['DATABASE']}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)

            invoke_url_pin = "https://kpo94dagq0.execute-api.us-east-1.amazonaws.com/test/topics/0affcdd81315.pin"

            pin_data = json.dumps({
                "records": [
                    {
                        "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], 
                                  "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": 
                                  pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"],
                                   "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"],
                                   "category": pin_result["category"]
                                  }
                    }
                ]
            })   

            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            pin_response = requests.request("POST", invoke_url_pin, headers=headers, data=pin_data)

            invoke_url_geo = "https://kpo94dagq0.execute-api.us-east-1.amazonaws.com/test/topics/0affcdd81315.geo"

            geo_data = json.dumps({
                "records": [
                    {
                        "value": {"ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], 
                                  "longitude": geo_result["longitude"], "country": geo_result["country"]
                                  }
                    }
                ]
            }, default=str)   

            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            geo_response = requests.request("POST", invoke_url_geo, headers=headers, data=geo_data)

            invoke_url_user = "https://kpo94dagq0.execute-api.us-east-1.amazonaws.com/test/topics/0affcdd81315.user"

            user_data = json.dumps({
                "records": [
                    {
                        "value": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], 
                                  "age": user_result["age"], "date_joined": user_result["date_joined"]
                                  }
                    }
                ]
            }, default=str)   

            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            user_response = requests.request("POST", invoke_url_user, headers=headers, data=user_data)

            print(pin_response.status_code)
            print(geo_response.status_code)
            print(user_response.status_code) 
            


if __name__ == "__main__":
    
    run_infinite_post_data_loop()
    print('Working')
    
