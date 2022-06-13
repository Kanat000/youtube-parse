import sqlite3
import pymysql
from config import db_config


class Mysql:
    def __init__(self, db):
        self.conn = pymysql.connect(
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=db_config['password'],
            database=db
        )
        self.cur = self.conn.cursor()

    def create_youtube_table(self):
        self.cur.execute('Create Table if not exists youtube('
                         'id int PRIMARY KEY AUTO_INCREMENT NOT NULL,'
                         'channel_id varchar(255),'
                         'channel_name varchar(255),'
                         'link varchar(255),'
                         'logo varchar(255),'
                         'count_of_subscribers varchar(255)'
                         ')'
                         )
        self.conn.commit()

    def get_channels(self):
        self.cur.execute("Select channel_id from youtube")
        return self.cur.fetchall()

    def get_url_by_channel_id(self, channel_id):
        self.cur.execute(f"Select link from youtube where channel_id='{channel_id}'")
        return self.cur.fetchone()[0]

    def insert_new_channel(self, data):
        self.cur.execute(
            'Insert into youtube(channel_id, channel_name, link, logo, count_of_subscribers) values(%s,%s,%s,%s,%s)',
            (data['channel_id'], data['channel_name'], data['link'], data['logo'], data['count_of_subscribers']))
        self.conn.commit()

    def update_channel_info(self, data):
        self.cur.execute(
            f"Update youtube "
            f"Set channel_name='{data['channel_name']}', "
            f"link='{data['link']}', "
            f"logo='{data['logo']}', "
            f"count_of_subscribers = '{data['count_of_subscribers']}'"
            f"where channel_id='{data['channel_id']}';"
        )
        self.conn.commit()

    def exists_channel_by_url(self, url):
        self.cur.execute(
            f"Select count(*) from youtube where link = '{url}'"
        )
        return self.cur.fetchone()[0] > 0

    def exists_channel_by_channel_id(self, channel_id):
        self.cur.execute(
            f"Select count(*) from youtube where channel_id = '{channel_id}'"
        )
        return self.cur.fetchone()[0] > 0
