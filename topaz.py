#!/usr/bin/python3

import os
import sys
import re
import time
import enum
import json
import begin
import typing
import logging

from collections import defaultdict

from xml.dom import minidom
from xml.parsers.expat import ExpatError

import splunklib.client as client

logger = logging.getLogger(__name__)
# logger.addHandler(logging.StreamHandler())


class Profile(object):
    """
    Profile entity..

    Example:
    {
        "account": "account",
        "username": "username",
        "password": "password",
        "indexes": {
            "sandbox": ["sandbox", "sandbox2"],
            "prod": "prod"
        },
    }
    """

    USERNAME = 'username'
    PASSWORD = 'password'
    ACCOUNT = 'account'
    INDEXES = 'indexes'
    PORT = "port"
    DEFAULT_PORT = 8089

    def __init__(self, profile: dict=None) -> None:
        """
        Constructing profile.

        @raises ValueError: If profile is empty.
        @raises TypeError: If profile is not a valid dict.
        """
        if not profile:
            raise ValueError(profile)
        if not isinstance(profile, dict):
            raise TypeError(type(profile))
        self.__data = profile
        self.__people = None

    def __str__(self) -> str:
        """
        String serializer.
        """
        return "<Profile: {}>".format(self.to_json())

    def get_host(self) -> str:
        """
        JIRA host getter.
        """
        return "{}.splunkcloud.com".format(self.account)

    def get_index(self, name: str) -> str:
        """
        Index getter.

        @raises ValueError: If name is invalid.
        @raises TypeError: If name is not a valid string.
        @raises KeyError: If index name is not found.
        """
        if not name:
            raise ValueError(name)
        if not isinstance(name, str):
            raise TypeError(type(name))
        if name not in self.indexes:
            raise KeyError(name)
        return Index(self.indexes[name])

    @property
    def indexes(self) -> dict:
        """
        Access indexes.

        @raises KeyError: If key name is not in JSON credentials.
        @raises ValueError: If key name is empty.
        @raises TypeError: if key value is not a valid string.
        @raises AttributeError: If credentials data is empty.

        @returns: Key value as a string.
        """
        if not self.__data:
            raise AttributeError("profile")
        if self.INDEXES not in self.__data:
            raise KeyError(self.INDEXES)
        if not self.__data[self.INDEXES]:
            raise ValueError(self.INDEXES)
        if not isinstance(self.__data[self.INDEXES], dict):
            raise TypeError(self.INDEXES)
        return self.__data[self.INDEXES]

    @property
    def port(self) -> str:
        """
        Access port.

        @raises TypeError: if key value is not a valid string.
        @raises AttributeError: If credentials data is empty.

        @returns: Key value as a string.
        """
        if not self.__data:
            raise AttributeError("profile")
        if self.PORT not in self.__data:
            return self.DEFAULT_PORT
        if not self.__data[self.PORT]:
            return self.DEFAULT_PORT
        if not isinstance(self.__data[self.PORT], int):
            raise TypeError(self.PORT)
        return self.__data[self.PORT]

    @property
    def username(self) -> str:
        """
        Access username.

        @raises KeyError: If key name is not in JSON credentials.
        @raises ValueError: If key name is empty.
        @raises TypeError: if key value is not a valid string.
        @raises AttributeError: If credentials data is empty.

        @returns: Key value as a string.
        """
        if not self.__data:
            raise AttributeError("profile")
        if self.USERNAME not in self.__data:
            raise KeyError(self.USERNAME)
        if not self.__data[self.USERNAME]:
            raise ValueError(self.USERNAME)
        if not isinstance(self.__data[self.USERNAME], str):
            raise TypeError(self.USERNAME)
        return self.__data[self.USERNAME]

    @property
    def password(self) -> str:
        """
        Access password.

        @raises KeyError: If key name is not in JSON credentials.
        @raises ValueError: If key name is empty.
        @raises TypeError: if key value is not a valid string.
        @raises AttributeError: If credentials data is empty.

        @returns: Key value as a string.
        """
        if not self.__data:
            raise AttributeError("profile")
        if self.PASSWORD not in self.__data:
            raise KeyError(self.PASSWORD)
        if not self.__data[self.PASSWORD]:
            raise ValueError(self.PASSWORD)
        if not isinstance(self.__data[self.PASSWORD], str):
            raise TypeError(self.PASSWORD)
        return self.__data[self.PASSWORD]

    @property
    def account(self) -> str:
        """
        Access account name.

        @raises KeyError: If key name is not in JSON credentials.
        @raises ValueError: If key name is empty.
        @raises TypeError: if key value is not a valid string.
        @raises AttributeError: If credentials data is empty.

        @returns: Key value as a string.
        """
        if not self.__data:
            raise AttributeError("profile")
        if self.ACCOUNT not in self.__data:
            raise KeyError(self.ACCOUNT)
        if not self.__data[self.ACCOUNT]:
            raise ValueError(self.ACCOUNT)
        if not isinstance(self.__data[self.ACCOUNT], str):
            raise TypeError(self.ACCOUNT)
        return self.__data[self.ACCOUNT]


class Configuration(object):
    """
    Configuration file entity.
    """

    def __init__(self, config_path: str=None) -> None:
        """
        Constructing Profile file.

        @param config_path: Configuration file name.

        @raises ValueError: If path name is empty.
        @raises TypeError: if path name is not a valid string.
        @raises RuntimeError: If config file is not a valid file.
        """
        if not config_path:
            raise ValueError("config_path")
        if not isinstance(config_path, str):
            raise TypeError("config_path")
        if not os.path.isfile(config_path):
            raise RuntimeError("File not found:", config_path)
        self.__config_path = config_path

    def __str__(self) -> str:
        """
        String serializer.
        """
        return "<Configuration: {}>".format(self.__config_path)

    def get_profile(self, profile_name: str=None) -> Profile:
        """
        Public method to access profile.

        @param profile_name: Profile name.

        @raises ValueError: If name is empty.
        @raises TypeError: If name is an invalid string.
        @raises RuntimeError: If config path is not a valid file.
        @raises KeyError: If name is not found in the profiles file.
        @raises ValueError: If file is not a valid JSON file.

        @returns: Profile instance.
        """
        if not profile_name:
            raise ValueError("profile_name")
        if not isinstance(profile_name, str):
            raise TypeError("profile_name")
        if not os.path.isfile(self.__config_path):
            raise RuntimeError("File not found:", self.__config_path)
        with open(self.__config_path, "r") as file_buffer:
            try:
                data = json.loads(file_buffer.read().strip())
            except ValueError:
                raise ValueError("Verify your JSON:", self.__config_path)
        if not data:
            raise RuntimeError(self.__config_path)
        if not isinstance(data, dict):
            raise RuntimeError(type(data))
        if profile_name not in data:
            raise KeyError(profile_name)
        if not data[profile_name]:
            raise RuntimeError("profile_name")
        return Profile(data[profile_name])


class Index(object):
    """
    Index entity.
    """

    OR = " OR "
    INDEX = 'index="{}"'

    def __init__(self, data: object) -> None:
        """
        Constructing indexes.

        @raises TypeError: If data is invalid
        """
        if not isinstance(data, (str, list)):
            raise TypeError(type(data))
        self.__index = data

    def __str__(self) -> str:
        """
        String serializer.
        """
        return "<Index: {}>".format(self.__index)

    def get_search_string(self) -> str:
        """
        Returns the index as a string.
        This can be used to search in Splunk.

        @raises NotImplementedError: If type of index is not supported.
        """
        if isinstance(self.__index, str):
            return self.INDEX.format(self.__index)
        elif isinstance(self.__index, list):
            return self.OR.join([
                self.INDEX.format(i)
                for i in self.__index
            ])
        raise NotImplementedError(self.__index)


class Level(enum.Enum):
    """
    Message levels.
    """
    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    ALL = "ALL"


class Date(enum.Enum):
    """
    Search range dates.
    """
    DAYS_30 = "30d"
    DAYS_15 = "15d"
    DAYS_7 = "7d"
    DAYS_3 = "3d"
    DAYS_1 = "1d"
    HOURS_24 = "24h"
    HOURS_10 = "10h"
    HOURS_4 = "4h"
    HOURS_2 = "2h"
    HOURS_1 = "1h"
    MINUTES_120 = "120m"
    MINUTES_60 = "60m"
    MINUTES_30 = "30m"
    MINUTES_15 = "15m"
    MINUTES_10 = "10m"
    MINUTES_5 = "5m"
    MINUTES_1 = "1m"
    NOW = "now"


class Splunk(object):
    """
    Splunk connector entity.
    """
    
    START_TIME = "earliest_time"
    END_TIME = "latest_time"
    SEARCH = "search"
    COUNT = "resultCount"

    def __init__(self, profile: Profile) -> None:
        """
        Splunk constructor.

        @raises TypeError: If profile is invalid.
        """
        if not isinstance(profile, Profile):
            raise TypeError(type(profile))
        self.__splunk = client.connect(host=profile.get_host(),
                                       port=profile.port,
                                       username=profile.username,
                                       password=profile.password)

    def search(self, index: Index, start: Date,
               end: Date, search: str='') -> typing.Generator:
        """
        Search in Splunk.

        @param index: Index instance.
        @param start: Start date.
        @param end: End date.
        @param search: Search query.

        @raises TypeError: If any of the params are invalid.
        @raises ValueError: If any of the params is empty.
        """
        logger.debug("Searching for: %s.", search)
        if not isinstance(start, Date):
            raise TypeError(type(start))
        if not isinstance(end, Date):
            raise TypeError(type(end))
        if not isinstance(search, str):
            raise TypeError(type(search))
        if not isinstance(index, Index):
            raise TypeError(type(index))
        if not start:
            raise ValueError("Start date is required")
        if not end:
            raise ValueError("End date is required")
        if not index:
            raise ValueError("Index is required.")
        if not search:
            raise ValueError("Search string is required.")
        search = search.replace("'", '"')
        logger.debug('Searching for: "%s" %s', index, search)
        query = {
            self.START_TIME: "-{}".format(start.value) if start != Date.NOW else start.value,
            self.END_TIME: "-{}".format(end.value) if end != Date.NOW else end.value,
        }
        logger.info("%s %s", index.get_search_string(), search)
        logger.info(json.dumps(query, indent=4, sort_keys=True))
        search = '{} {} {}'.format(self.SEARCH, index.get_search_string(), search)
        job = self.__splunk.jobs.create(search, **query)
        while not job.is_ready():
            pass
        total = int(job[self.COUNT])
        logger.debug("%s messages found.", total)
        if total:
            results = job.results()
            while True:
                content = results.read(1024)
                if len(content) == 0:
                    break
                yield content
            job.cancel()
        logger.debug("No more messages found.")


class XML(object):
    """
    XML output entity.
    """

    def __init__(self) -> None:
        """
        Constructing XML file.

        @raises OSError: If temporal file already exists.
        """
        self.__tmpfile = os.path.join(os.sep, "tmp", "{}.xml".format(time.time()))
        if os.path.isfile(self.__tmpfile):
            raise OSError("Temporal file already exists:", self.__tmpfile)
        logger.debug("Saving results to %s.", self.__tmpfile)

    def save(self, stream: typing.Generator) -> None:
        """
        Save generator strem to XML file.
        """
        with open(self.__tmpfile, "w") as f:
            for result in stream:
                if result:
                    f.write(result.decode('utf-8'))

    def get_messages(self) -> typing.Generator:
        """
        Returns messages from XML file..
        """
        try:
            doc = minidom.parse(self.__tmpfile)
        except ExpatError:
            logger.exception("Failed to parse temporal file.")
            return
        logger.debug("XML document found: %s", doc)
        for item in doc.getElementsByTagName('v'):
            if item.getAttribute("xml:space") == "preserve":
                text = " ".join([
                    node.data
                    for node in item.childNodes
                    if hasattr(node, "data")
                ])
                try:
                    yield Message(text)
                except Exception as error:
                    yield Alert(str(error), text)
        logger.debug("No more elements found.")


class Event(object):
    """
    Event entity.
    """

    def __init__(self) -> None:
        """
        Constructing Event.
        """
        self._raw = {}

    @property
    def line(self) -> str:
        """
        Event line getter.
        """
        return self._raw.get('line', '')

    def __str__(self) -> str:
        """
        String serializer.
        """
        return "<Event: '{}'>".format(self._raw)


class Alert(Event):
    """
    Alert event entity.
    """

    def __init__(self, error: str, data: str) -> None:
        """
        Constructing Alert.

        @param error: Error string.
        @param data: Alert payload.

        @raises TypeError: If type of params is invalid.
        """
        self.level = Level.ERROR
        if not isinstance(data, str):
            raise TypeError(type(data))
        if not isinstance(error, str):
            raise TypeError(type(error))
        self.error = error
        self._raw = json.loads(data)

    def __str__(self) -> str:
        """
        String serializer.
        """
        return "<Alert: '{}'>".format(self.line)


class Message(Event):
    """
    Message event entity.
    """

    def __init__(self, data: str) -> None:
        """
        Constructing message.

        @param data: Event payload.

        @raises TypeError: If type of params is invalid.
        """
        if not isinstance(data, str):
            raise TypeError(type(data))
        self._raw = json.loads(data)
        self._find_tags()
        self._find_text()

    @property
    def timestamp(self) -> str:
        """
        Timestamp getter.
        """
        return self.line[1:20]

    @property
    def level(self) -> str:
        """
        Message level getter.
        """
        _text = self.line[21:]
        _text = _text.split()
        return Level(_text[0])

    def _find_tags(self) -> None:
        """
        Find docker tags in message.
        """
        self.tags = json.loads(self._raw.get('tag', '{}'))
        self.source = self.tags.get('source', '')
        self.docker_image_name = self.tags.get('docker_image_name', '')
        self.docker_image_id = self.tags.get('docker_image_id', '')
        self.docker_name = self.tags.get('docker_name', '')
        self.docker_id = self.tags.get('docker_id', '')

    def _find_text(self) -> None:
        """
        Find text in message.
        """
        _text = self.line[21:]
        _text = _text.split()
        _module = _text[1]
        _module = _module.split("::")
        self.module = _module[0]
        _function = _module[1].split("(")
        self.function = _function[0]
        self.line_number = _function[1][:-2]
        self.text = " ".join(_text[2:])

    def __str__(self) -> str:
        """
        String serializer.
        """
        return "<Message: {} | {} | {}:{}:{} | '{}'>".format(self.timestamp,
                                                             self.level.value,
                                                             self.module,
                                                             self.function,
                                                             self.line_number,
                                                             self.text)

class Description(object):
    """
    Flags descriptions.
    """

    class Script(object):
        """
        Script descriptions.
        """
        PROFILE_NAME = "Config. profile name."
        CONFIG_PATH = "Config. file path."
        DEBUG = "Enable debug mode."

    class Search(object):
        """
        Search action descriptions.
        """
        START = "Start day such as: 1d."
        END = "End day such as: 15m."
        INDEX = "Search index name such as: sandbox."
        SEARCH = "Search string payload."
        LEVEL = "Search messages by level."


class Default(object):
    """
    Defaults script values.
    """
    START = Date.MINUTES_120
    END = Date.NOW
    CONFIG_PATH = os.path.join(os.sep, "home", os.getlogin(), ".topaz")
    PROFILE_NAME = "ampush"
    LEVEL = Level.ALL.value
    INDEX = ''
    SEARCH = ''


@begin.start
def run(index: Description.Search.INDEX=Default.INDEX,
        search: Description.Search.SEARCH=Default.SEARCH,
        start: Description.Search.START=Default.START,
        end: Description.Search.END=Default.END,
        level: Description.Search.LEVEL=Default.LEVEL,
        debug: Description.Script.DEBUG=False,
        profile_name: Description.Script.PROFILE_NAME=Default.PROFILE_NAME,
        config_path: Description.Script.CONFIG_PATH=Default.CONFIG_PATH):
    """
    Search in Splunk.
    """

    # Initiliaizing profile.
    config  = Configuration(config_path)
    profile = config.get_profile(profile_name)

    # Initializing logging.
    log_format = '%(message)s'
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO, format=log_format)

    # Searching in Splunk.
    try:
        splunk = Splunk(profile=profile)
        start = Date(start)
        end = Date(end)
        index = profile.get_index(index)
        levels = {
            Level(l).value
            for l in level.split(",")
        }
        x = XML()
        x.save(splunk.search(start=start, end=end, search=search, index=index))
        for message in x.get_messages():
            if message.level.value in levels or Level.ALL.value in levels:
                print(message)
    except Exception:
        logger.exception("Report FAILED!")
