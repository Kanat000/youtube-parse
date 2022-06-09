import json
from urls import url_list
import re
from database import Sqlite
from config import dbName
from auth import youtube_authenticate
from urllib import parse as p

youtubeAPI = youtube_authenticate()


def parse_youtube_url(url):
    url_parse_info = p.urlparse(url)
    path = url_parse_info.path
    id_inside_path = path.split('/')[2]
    if "/c/" in path:
        return id_inside_path, 'c'
    elif "/channel/" in path:
        return id_inside_path, 'channel'
    elif "/user/" in path:
        return id_inside_path, 'user'


def get_channel_details(youtube, **kwargs):
    return youtube.channels().list(
        part='snippet,statistics',
        **kwargs
    ).execute()


def search(youtube, **kwargs):
    return youtube.search().list(
        part="snippet",
        **kwargs
    ).execute()


def get_channel_id_by_url(youtube, url):
    id_inside_path, channel_type = parse_youtube_url(url)
    if channel_type == "channel":
        return id_inside_path
    elif channel_type == "user":
        response = get_channel_details(youtube, forUsername=id_inside_path)
        items = response.get("items")
        if items:
            user_id = items[0].get("id")
            return user_id
    elif channel_type == "c":
        response = search(youtube, q=id_inside_path, maxResults=1)
        items = response.get("items")
        if items:
            c_id = items[0]["snippet"]["channelId"]
            return c_id


def get_data(channel_id, url):
    response = get_channel_details(youtube=youtubeAPI, id=channel_id)
    items = response["items"]
    snippet = items[0]["snippet"]
    statistics = items[0]["statistics"]
    title = snippet['title']
    logo = snippet['thumbnails']['default']['url']
    subscriber_count = statistics['subscriberCount']
    data = {
        'channel_id': channel_id,
        'channel_name': title,
        'logo': logo,
        'link': url,
        'count_of_subscribers': subscriber_count
    }

    return data


class Parser:
    def __init__(self):
        self.db = Sqlite(dbName)

    def initialize(self):
        self.db.create_youtube_table()
        for url in url_list:
            if not self.db.exists_channel_by_url(url):
                channel_id = get_channel_id_by_url(youtube=youtubeAPI, url=url)
                self.db.insert_new_channel(get_data(channel_id, url))

        print('\nDatabase is ready for work!!!')

    def update_information(self):
        channels = self.db.get_channels()
        for channel in channels:
            channel_id = channel[0]
            url = self.db.get_url_by_channel_id(channel_id)
            self.db.update_channel_info(get_data(channel_id, url))
        print('\nDatabase updated!!!')
