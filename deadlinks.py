# -*- coding: utf8 -*-

import logging
from bs4 import BeautifulSoup
from pelican import signals
import requests
from requests.exceptions import Timeout, RequestException
import sqlite3

log = logging.getLogger(__name__)

UNKNOWN = None
MS_IN_SECOND = 1000.0

DEFAULT_OPTS = {
    'archive': True,
    'classes': [],
    'labels': False,
    'timeout_duration_ms': 1000,
    'timeout_is_error': False,
    'cache_file': None,  # Cache disabled by default
}

SPAN_WARNING = u'<span class="label label-warning"></span>'
SPAN_DANGER = u'<span class="label label-danger"></span>'
ARCHIVE_URL = u'http://web.archive.org/web/*/{url}'

def initialize_cache(cache_file):
    conn = sqlite3.connect(cache_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS url_cache (
            url TEXT PRIMARY KEY,
            availability BOOLEAN,
            success BOOLEAN,
            code INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def get_cached_status(url, cache_file):
    conn = sqlite3.connect(cache_file)
    cursor = conn.cursor()
    cursor.execute('SELECT availability, success, code FROM url_cache WHERE url = ?', (url,))
    result = cursor.fetchone()
    conn.close()
    if result:
        availability, success, code = result
        return (bool(availability), bool(success), code)
    return None

def update_cache(url, availability, success, code, cache_file):
    conn = sqlite3.connect(cache_file)
    cursor = conn.cursor()
    cursor.execute('''
        REPLACE INTO url_cache (url, availability, success, code)
        VALUES (?, ?, ?, ?)
    ''', (url, availability, success, code))
    conn.commit()
    conn.close()

def get_status_code(url, opts, cache_file=None):
    """
    Open connection to the given url and check status code.

    :param url: URL of the website to be checked
    :return: (availibility, success, HTTP code)
    """
    if cache_file:
        cached = get_cached_status(url, cache_file)
        if cached:
            return cached

    availability, success, code = (False, False, None)
    timeout_duration_seconds = opts['timeout_duration_ms'] / MS_IN_SECOND
    try:
        response = requests.get(url, timeout=timeout_duration_seconds)
        code = response.status_code
        availability = True
        success = code == requests.codes.ok
    except Timeout:
        availability = False
        success = UNKNOWN
    except RequestException:
        availability = UNKNOWN
        success = False

    if cache_file:
        update_cache(url, availability, success, code, cache_file)

    return (availability, success, code)

def user_enabled(inst, opt):
    """
    Check whether the option is enabled.

    :param inst: instance from content object init
    :param url: Option to be checked
    :return: True if enabled, False if disabled or non present
    """
    return opt in inst.settings and inst.settings[opt]

def get_opt(opts, name):
    """
    Get value of the given option

    :param opts:    Table with options
    :param name:    Name of option
    :return:        Option of a given name from given table or default value
    """
    return opts[name] if name in opts else DEFAULT_OPTS[name]

def add_class(node, name):
    """
    Add class value to a given tag

    :param node:    HTML tag
    :param name:    class attribute value to add
    """
    node['class'] = node.get('class', []) + [name, ]

def change_to_archive(anchor):
    """
    Modify href attribute to point to archive.org instead of url directly.
    """
    src = anchor['href']
    dst = ARCHIVE_URL.format(url=src)
    anchor['href'] = dst

def on_connection_error(anchor, opts):
    """
    Called on connection error (URLError being thrown)

    :param anchor:  Anchor element (<a/>)
    :param opts:    Dict with user options
    """
    classes = get_opt(opts, 'classes')
    for cls in classes:
        add_class(anchor, cls)
    labels = get_opt(opts, 'labels')
    if labels:
        soup = BeautifulSoup(SPAN_DANGER, 'html.parser')
        soup.span.append('not available')
        idx = anchor.parent.contents.index(anchor) + 1
        anchor.parent.insert(idx, soup)
    archive = get_opt(opts, 'archive')
    if archive:
        change_to_archive(anchor)

def on_access_error(anchor, code, opts):
    """
    Called on access error (such as 403, 404)

    :param anchor:  Anchor element (<a/>)
    :param code:    Error code (403, 404, ...)
    :param opts:    Dict with user options
    """
    classes = get_opt(opts, 'classes')
    for cls in classes:
        add_class(anchor, cls)
    labels = get_opt(opts, 'labels')
    if labels:
        soup = BeautifulSoup(SPAN_WARNING, 'html.parser')
        soup.span.append(str(code))
        idx = anchor.parent.contents.index(anchor) + 1
        anchor.parent.insert(idx, soup)
    archive = get_opt(opts, 'archive')
    if archive:
        change_to_archive(anchor)

def content_object_init(instance):
    """
    Pelican callback
    """
    if instance._content is None:
        return
    if not user_enabled(instance, 'DEADLINK_VALIDATION'):
        log.debug("Configured not to validate links")
        return

    settings = instance.settings
    siteurl = settings.get('SITEURL', '')
    opts = settings.get('DEADLINK_OPTS', DEFAULT_OPTS)
    cache_file = opts.get('cache_file')

    if cache_file:
        initialize_cache(cache_file)

    soup_doc = BeautifulSoup(instance._content, 'html.parser')
    for anchor in soup_doc.find_all(['a', 'object']):
        if 'href' not in anchor.attrs:
            continue
        url = anchor['href']
        if not url.startswith('http') or (siteurl and url.startswith(siteurl)):
            continue

        avail, success, code = get_status_code(url, opts, cache_file)

        if not avail:
            timeout_is_error = get_opt(opts, 'timeout_is_error')
            if timeout_is_error or success == UNKNOWN:
                on_connection_error(anchor, opts)
            continue

        if not success and code:
            on_access_error(anchor, code, opts)

    instance._content = str(soup_doc)

def register():
    """
    Part of Pelican API
    """
    signals.content_object_init.connect(content_object_init)
