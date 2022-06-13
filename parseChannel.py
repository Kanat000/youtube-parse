from urls import url_list
from database import Mysql
from config import dbName
from auth import youtube_authenticate
from urllib import parse as p
import os


def count_of_tokens(dir):
    count = 0
    for x in list(os.scandir(dir)):
        if x.is_file():
            count += 1
    return count


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


def get_data(youtube, channel_id, url):
    response = get_channel_details(youtube=youtube, id=channel_id)
    items = response["items"]
    snippet = items[0]["snippet"]
    statistics = items[0]["statistics"]
    title = snippet['title']
    logo = snippet['thumbnails']['default']['url']
    subscriber_count = 0
    if not statistics['hiddenSubscriberCount']:
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
        self.db = Mysql(dbName)
        self.file_num = 1
        self.youtubeAPI = youtube_authenticate(self.file_num)

    def __error_checker(self, e):
        if e.args[0]['status'] == '403':
            if self.file_num < count_of_tokens('tokens'):
                self.file_num += 1
                print(f'Updated token to "token{self.file_num}.pickle".')
                self.youtubeAPI = youtube_authenticate(self.file_num)
                pass
            else:
                print('Limits of all accounts is ended.')
        else:
            print(e)

    def initialize(self):
        self.db.create_youtube_table()
        for url in url_list:
            if not self.db.exists_channel_by_url(url):
                try:
                    channel_id = get_channel_id_by_url(youtube=self.youtubeAPI, url=url)
                    self.db.insert_new_channel(get_data(self.youtubeAPI, channel_id, url))
                except Exception as e:
                    self.__error_checker(e)

        print('\nDatabase is ready for work!!!')

    def update_information(self):
        self.file_num = 1
        self.youtubeAPI = youtube_authenticate(self.file_num)
        channels = self.db.get_channels()
        for channel in channels:
            try:
                channel_id = channel[0]
                url = self.db.get_url_by_channel_id(channel_id)
                self.db.update_channel_info(get_data(self.youtubeAPI, channel_id, url))
            except Exception as e:
                self.__error_checker(e)

        print('\nDatabase updated!!!')
