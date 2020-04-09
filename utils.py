#Python3


"""
Utils collection
Version 1.0
05.03.2020
"""

from html.parser import HTMLParser
from datetime import datetime
import logging
import requests
import json
import os
import xml.etree.ElementTree as ET


""" Logger Configuration """


default_logging_name = 'utils'
default_logging_level = 20
FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
formatter = logging.Formatter(FORMAT)
# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('%s.log' % default_logging_name)

# Create formatters and add it to handlers
c_handler.setFormatter(formatter)
f_handler.setFormatter(formatter)

logging.basicConfig(format=FORMAT, level=default_logging_level, handlers=[c_handler, f_handler])
logger = logging.getLogger(default_logging_name)
logger.setLevel(default_logging_level)


""" Errors, Exceptions """


class RequestError(Exception):
    def __init__(self, status, message=None):
        self.status = status
        self.message = message

    def __str__(self):
        if self.message is None:
            return str(self.status)
        else:
            return "%s (%s)" % (self.status, self.message)


class GoogleError(RequestError):
    pass


class MyHTMLParser(HTMLParser):
    def error(self, message):
        pass

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__()
        self.path = []
        self.data = []
        self.valid_tags = []
        self.skip_tags = []
        self.parsers = {}
        self.current_tag = None
        self.is_current_tag_valid = False
        for arg in args:
            for key in arg:
                setattr(self, key, arg[key])
        # print("Parsing of tags:", self.valid_tags)

    def handle_starttag(self, tag, attrs):
        if tag in ['br', 'meta', 'head']:
            return
        self.path.append(tag)
        self.is_current_tag_valid = self.valid(tag)
        self.current_tag = tag
        if not self.is_current_tag_valid:
            return
        if self.is_skip(tag):
            return
        self.data.append((tag, attrs))
        super(self.__class__, self).handle_starttag(tag, attrs)

    def handle_endtag(self, tag):
        if tag in ['br', 'meta', 'head']:
            return
        try:
            i = len(self.path) - 1 - self.path[::-1].index(tag)
            self.path = self.path[:i]
            self.is_current_tag_valid = self.valid(tag)
            self.current_tag = None
            if not self.is_current_tag_valid:
                return
            super(self.__class__, self).handle_endtag(tag)
        except ValueError as e:
            print(e)

    def handle_data(self, data):
        if not self.current_tag or not self.is_current_tag_valid:
            self.current_tag = None if not self.path else self.path[len(self.path)-1]
            if not self.current_tag:
                return
            self.is_current_tag_valid = self.valid(self.current_tag)
            if not self.is_current_tag_valid:
                return
            if len(self.data) > 2:
                parent = self.data[len(self.data) - 2]
                self.data.append(('', []))

        if not self.parsers:
            self.default_parser(data)
        else:
            if not self.parsers[self.current_tag]:
                self.default_parser(data)
            else:
                self.parsers[self.current_tag](data, self)
        super(self.__class__, self).handle_data(data)

    def default_parser(self, data):
        if self.data:
            idx = len(self.data) - 1
            target = self.data[idx]
            if len(target) > 2:
                self.data[idx] += (data,)
            else:
                self.data[idx] = (target[0], target[1], data)

    def valid(self, tag):
        if len(self.valid_tags) > 0:
            return True if tag in self.valid_tags else False
        return True

    def is_skip(self, tag):
        if len(self.skip_tags) > 0:
            return True if tag in self.skip_tags else False
        return False

    def feed_and_return(self, data):
        self.feed(data)
        return self


class AnektodHTMLParser(HTMLParser):
    def error(self, message):
        pass

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.ready = []
        self.lines = ""
        self.collecting = False
        self.begin = False
        self.internal_divs = []

    def handle_starttag(self, tag, attrs):
        if tag not in ['div', 'p']:
            return
        for attr in attrs:
            if 'anekdot' in attr:
                if not self.collecting:
                    self.collecting = True
                else:
                    self.internal_divs.append('div')
                return
        if tag == 'p':
            self.begin = True

    def handle_data(self, data):
        if self.collecting:
            if data:
                self.lines += data

    def handle_endtag(self, tag):
        if tag not in ['div', 'p']:
            return

        if tag == 'div':
            if self.internal_divs:
                self.internal_divs.pop('div')

            if not self.internal_divs:
                self.collecting = False
            return

        if tag == 'p':
            self.begin = False
            if self.lines.replace('\n', '').replace('\r', ''):
                self.ready.append(self.lines)
            self.lines = ""


class LinksHTMLParser(HTMLParser):

    def error(self, message):
        pass

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.ready = []
        self.lines = ""
        self.collecting = False
        self.begin = False
        self.internal_divs = []
        self.media_heading = False
        self.links = []
        self.info_add = False
        self.info = []
        self.info_buffer = []

    def handle_starttag(self, tag, attrs):
        if tag not in ['p', 'h4', 'a']:
            return

        for attr in attrs:
            if 'media-heading' in attr:
                self.media_heading = True
            if 'link-reverse' in attr:
                self.info_add = True

        if tag == 'a' and self.media_heading:
            self.collecting = True
            self.links.append(attr[1])

        if tag == 'p':
            self.begin = True

    def handle_data(self, data):
        if self.collecting:
            if data:
                self.lines += data
        if self.info_add:
            self.info_buffer.append(data)

    def handle_endtag(self, tag):
        if tag not in ['p', 'h4', 'a']:
            return

        if tag == 'a' and self.media_heading:
            self.collecting = False

        if tag == 'h4':
            self.media_heading = False
            self.collecting = False
            if self.lines.replace('\n', '').replace('\r', '').strip():
                self.ready.append(self.lines.replace('\n', '').replace('\r', '').strip())
            self.lines = ""

        if tag == 'p' and self.info_add:
            self.info_add = False
            self.info.append(' '.join(self.info_buffer))
            self.info_buffer = []


class StoryHTMLParser(HTMLParser):

    def error(self, message):
        pass

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.ready = []
        self.lines = ""
        self.collecting = False
        self.begin = False
        self.internal_divs = []
        self.pages = []
        self.page_buffer = ''
        self.pages_collect = False

    def handle_starttag(self, tag, attrs):
        if tag not in ['div', 'p', 'ul', 'a']:
            return
        for attr in attrs:
            if 'full_text' in attr:
                if not self.collecting:
                    self.collecting = True
                else:
                    self.internal_divs.append('div')
                return
        if tag == 'p':
            self.begin = True

        if tag == 'ul':
            for attr in attrs:
                if 'pagination' in attr:
                    self.pages_collect = True

        if tag == 'a' and self.pages_collect:
            self.page_buffer = attrs[0][1]

    def handle_data(self, data):
        if self.collecting:
            if data:
                self.lines += data

    def handle_endtag(self, tag):
        if tag not in ['div', 'p', 'ul', 'a']:
            return

        if tag == 'div':
            if self.internal_divs:
                self.internal_divs.pop('div')

            if not self.internal_divs:
                self.collecting = False
            return

        if tag == 'p':
            self.begin = False
            if self.lines.replace('\n', '').replace('\r', ''):
                self.ready.append(self.lines)
            self.lines = ""

        if tag == 'ul' and self.pages_collect:
            self.pages_collect = False

        if tag == 'a' and self.pages_collect:
            self.pages.append(self.page_buffer)
            self.page_buffer = ''


class Result:
    el = None
    childs = []

    def __init__(self, el):
        self.el = el
        self.childs = [d[el.tag](child) for child in el]


class Corpus(Result):
    pass


class Sentense(Result):
    pass


class Word(Result):
    def __init__(self, el):
        super().__init__(el)
        for attrib in el.attrib:
            self.__dict__.update({attrib.lower(): el.attrib[attrib]})

        for kv in el.attrib['mi'].split('|'):
            kv_arr = kv.split('=')
            if len(kv_arr) > 1:
                self.__dict__.update({kv_arr[0].lower(): kv_arr[1]})


d = {"corpus": Corpus, 'SENTENCE': Sentense, 'NODE': Word}


def sentence_analyze(sentence):
    r = _get("http://lindat.mff.cuni.cz/services/udpipe/api/process?tokenizer&tagger&parser&model=russian-syntagrus-ud-2.5-191206&data="+sentence)

    if r and r.status_code == 200:
        parsed = json.loads(r.text)
        return [line.split('\t') for line in parsed['result'].split('#')[4].split('\n')[1:]]

    return None


def sentence_analyze_matxin(sentence):
    r = _get("http://lindat.mff.cuni.cz/services/udpipe/api/process?tokenizer&tagger&parser&model=russian-syntagrus-ud-2.5-191206&output=matxin&data="+sentence)

    if r and r.status_code == 200:
        root = ET.fromstring(json.loads(r.text)['result'])
        return Result(root).childs[0].childs[0]

    return None


def to_file(file_name, text):
    try:
        os.remove(file_name)
    except FileNotFoundError as e:
        pass

    if isinstance(text, str):
        mode = 'wt'
    else:
        mode = 'wb'
    with open(file_name, mode) as f:
        try:
            f.write(text)
        finally:
            f.close()


def from_file(file_name):
    with open(file_name, 'rb') as f:
        return f.read()


def txt_from_file(file_name):
    with open(file_name, 'r') as f:
        return f.read()


def json_from_file(file_name, err_msg=None):
    data = None
    with open(file_name, 'rb') as f:
        data = json.load(f)
    if not data:
        raise Exception(err_msg if err_msg else "No data loaded.")
    return data

def json_to_file(file_name, data):
    to_file(file_name, json.dumps(data, ensure_ascii=False, indent=2))


class _session:
    def __init__(self):
        super().__init__()
        self.s = requests.Session()

    def _get(self):
        pass

    def _post(self):
        pass


def _get(url, params=None, session=None, log_folder='requests/', *args, **kwargs):
    if session:
        r = session.get(url)
    else:
        r = requests.get(url, params, *args)
    if not r or not r.ok:
        raise RequestError(r.reason, url)
    timestamp = datetime.now().strftime('%Y%m%d %H %M %S %f')[:-3]
    # name = log_folder + "%s-get" % timestamp
    logger.info("%s %s %s" % (url, params, r.status_code))
    # to_file(name + ".html", r.text)
    return r


def _gete(url, params=None, session=None, *args, **kwargs):
    if session:
        r = session.get(url)
    else:
        r = requests.get(url, params, args)
    timestamp = datetime.now().strftime('%Y%m%d %H %M %S %f')[:-3]
    name = "requests/%s-get" % timestamp
    logger.info("%s %s" % (url, params))
    # to_file(name + ".html", r.text)
    return r, name + ".html"


def _poste(url, params, headers, session=None, *args, **kwargs):
    if session:
        r = session.post(url, params, headers)
    else:
        session = requests.Session()
        r = session.post(url, params, *args)
    timestamp = datetime.now().strftime('%Y%m%d %H %M %S %f')[:-3]
    # name = "requests/%s-post" % timestamp
    logger.info("%s %s" % (url, params))
    # to_file(name + ".html", r.text)
    return r, session


def google_geocode(address, components='locality:riga|country:LV', language='ru', key=''):
    response = requests.get(f'https://maps.googleapis.com/maps/api/geocode/json?address={address}&components={components}&language={language}&key={key}')
    if not response.ok:
        raise GoogleError(response.reason)
    else:
        body = response.json()
        if 'status' in body:
            if body['status'] in ['OK', 'ZERO_RESULTS']:
                return body['results']
            else:
                raise GoogleError(body['status'], body['error_message'])



