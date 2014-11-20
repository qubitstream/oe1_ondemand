#!/usr/bin/env python3
#
# Download on-demand audio files from Ö1 (Österreich 1)
#
# requires Python 3.3+
#
# needs Beautiful Soup 4 and mutagen
# FFmpeg must be in path (with libfdk_aac compiled in)
#
# Christoph Haunschmidt 2014-11


import os
import sys
import json
import pickle
import re
import time
import argparse
import datetime
import configparser
import atexit
import logging
import subprocess
import shutil
from pprint import pprint
import urllib.request
import bs4
import mutagen

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.WARNING,
    datefmt='%Y-%m-%d %H:%M')

if sys.platform in ('nt', 'win32'):
    CMD_ENC = 'latin1'
    DOWNLOAD_BASEDIR = os.path.normpath(r'm:\Ö1')
else:
    CMD_ENC = 'utf8'
    DOWNLOAD_BASEDIR = os.path.normpath(r'/media/xdata/audio/library/Ö1/')

try:
    TERMINAL_WIDTH = os.get_terminal_size()[0]
except OSError:
    TERMINAL_WIDTH = 80

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
HTML_CACHE_FN = os.path.join(DOWNLOAD_BASEDIR, 'cache.pickle')
DEFAULT_INI_FN = os.path.join(DOWNLOAD_BASEDIR, 'oe1_download.ini')
FFMPEG_EXECUTABLE = 'ffmpeg'

# base url for the json for the date, e.g.
# http://oe1.orf.at/programm/konsole/tag/20141031
BASE_URL = r'http://oe1.orf.at/programm/konsole/tag/'
BASE_URL_DETAIL = r'http://oe1.orf.at/programm/'

INI_DEFAULTS = {
    'TimeWindow':'00:00-24:00',
    'Days': '0,1,2,3,4,5,6',  # 0 = Monday, ... 6 = Sunday
    'TargetDir': '{DOWNLOAD_BASEDIR}/{SECTION}',
    'TargetName': '{Y}-{m}-{d} {H}h{M} Ö1 {title} {info_1line_limited}',
    'KeepOriginal': 'False',
    'Quality': '1',
    # search by metadata given by JSON, you can use regex here
    'title': '.*',
    'info': '.*',
    'TagArtist': 'Ö1',
    # If you don't use TagAlbum, the ini file section header will be used as album name
    'TagAlbum': '{SECTION}',
    'TagTitle': '{Y}-{m}-{d} {H}:{M} {title} {info_1line} (id:{id})',
    'TagDate': '{Y}',
    'TagGenre': 'Podcast',
    'TagComment': '{extended_info}',
}


def repl_unsave(file_name):
    """Replace unsave characters for a windows file system"""
    tmp_str = re.sub(r'[:?]+', '', file_name)
    return re.sub(r'[\\/:"*?<>|]+', '_', tmp_str)


def tag_media_file(media_fn, tag_dict):
    if not os.path.isfile(media_fn):
        logging.warn('no such file to tag: {}'.format(media_fn))
        return
    mf = mutagen.File(media_fn, easy=True)
    for key, value in tag_dict.items():
        try:
            mf[key] = value
            if key == 'comment':
                mf['description'] = value
        except mutagen.MutagenError as e:
            logging.warning('error tagging {}: {}'.format(media_fn, e))
    mf.save()
    logging.debug('tagged file {}'.format(media_fn))


def convert_to_m4a(media_fn, quality=1, conv_fn=None, length=None, aac_he_v2=False):
    if not os.path.isfile(media_fn):
        logging.warning('no such file to convert: {}'.format(media_fn))
        return -1
    path, file_name = os.path.split(media_fn)
    basename, ext = os.path.splitext(file_name)
    conv_fn = conv_fn or os.path.join(path, basename + '.m4a')

    if os.path.isfile(conv_fn) and not overwrite_existing:
        logging.debug('skipping existing file to convert: {}'.format(conv_fn))
        return

    command_list = [ARGS.ffmpeg, '-y']
    if length:
        command_list.extend(['-t', str(length)])
    command_list.extend(['-i', media_fn])

    command_list.extend(['-c:a', 'libfdk_aac', '-profile:a'])
    command_list.extend(['aac_he_v2'] if aac_he_v2 else ['aac_he'])
    command_list.extend(['-vbr', str(quality), '-sample_fmt', 's16'])
    command_list.extend([conv_fn])

    if ARGS.dry_run:
        logging.info('dry run: skipping conversion of file {} to m4a {}'.format(media_fn, conv_fn))
        return 0

    logging.debug('ffmpeg command: {}'.format(' '.join(command_list)))
    try:
        ffmpeg = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        stdout, stderr = ffmpeg.communicate()
    except:
        if os.path.isfile(conv_fn):
            os.remove(conv_fn)
        raise

    if ffmpeg.returncode != 0:
        logging.warning('An error occurred (Errorcode {}):'.format(ffmpeg.returncode))
        logging.warning(stderr.decode('utf-8'))

    return int(ffmpeg.returncode)


def cache(fn, max_age=datetime.timedelta(days=1)):
    cache = {}
    new_items = False

    def write_cache_atexit():
        if new_items or True:
            try:
                with open(HTML_CACHE_FN, 'wb') as f:
                    pickle.dump(cache, f)
                    logging.info('wrote HTML cache file {}'.format(
                        HTML_CACHE_FN))
            except IOError:
                logging.warning('could not write HTML cache {}'.format(
                    HTML_CACHE_FN))

    atexit.register(write_cache_atexit)

    try:
        with open(HTML_CACHE_FN, 'rb') as f:
            cache = pickle.load(f)
            logging.info('read HTML cache file {}'.format(HTML_CACHE_FN))
    except IOError:
        logging.info('could not open HTML cache file {}'.format(HTML_CACHE_FN))

    def cached(*args):
        utcnow = datetime.datetime.utcnow()
        if not ARGS.no_cache and args in cache and utcnow < cache[args]['utcdatetime'] + max_age:
            logging.debug('using cached result for args {}'.format(args))
            return cache[args]['result']
        else:
            new_items = True
            logging.debug('cache miss for args {}'.format(args))
            result = fn(*args)
            cache[args] = {
                'result': result,
                'utcdatetime': utcnow,
            }
            return result
    return cached

@cache
def url_read(url):
    response = urllib.request.urlopen(url)
    return response.read().decode('utf-8')


def get_broadcast_info_extended(url):
    html = url_read(url)
    soup = bs4.BeautifulSoup(html, 'html.parser')
    lines = []
    s = soup.find('div', {'class': 'textbox-wide'}).findAll('p')[1:-1]
    if s:
        for p in s:
            for string in p.stripped_strings:
                if p:
                    lines.append(string)
    s = soup.find('div', {'class': 'postarticle'})
    if s:
        lines.append('_' * 10)
        for string in s.stripped_strings:
            lines.append(string)
    return '\r\n'.join(lines)


def reporthook(blocknum, blocksize, totalsize):
    # from http://stackoverflow.com/questions/13881092/download-progressbar-for-python-3
    readsofar = blocknum * blocksize
    if totalsize > 0:
        percent = readsofar * 1e2 / totalsize
        s = "\r%5.1f%% %*d / %d" % (
            percent, len(str(totalsize)), readsofar, totalsize)
        sys.stderr.write(s)
        if readsofar >= totalsize: # near the end
            sys.stderr.write("\n")
    else: # total size is unknown
        sys.stderr.write("read %d\n" % (readsofar,))


class Broadcast:
    def __init__(self, d):
        self.data = d
        self.datetime = datetime.datetime.strptime(
            self.data['day_label'] + self.data['time'], '%d.%m.%Y%H:%M')
        self.weekday = self.datetime.weekday()
        self.time = datetime.time(self.datetime.hour, self.datetime.minute)
        self.date = datetime.date(
            self.datetime.year, self.datetime.month, self.datetime.day)
        self.info_l1 = self.data['info'].split('\n')[0]
        self.info_nonl = re.sub(r'[\n\r\s]+', ' ', self.data['info'])
        self.full_dict = self.data.copy()
        self.full_dict.update({
                'Y': self.datetime.strftime('%Y'),
                'm': self.datetime.strftime('%m'),
                'd': self.datetime.strftime('%d'),
                'H': self.datetime.strftime('%H'),
                'M': self.datetime.strftime('%M'),
                'S': self.datetime.strftime('%S'),
                'info_1line': self.info_nonl,
                'info_1line_limited': self.info_nonl[0:120],
            })

    def __str__(self):
        info_nonl = re.sub(r'[\n\r\s]+', ' ', self.data['info'])
        s = '{dt} id {id} {title} {info_nonl}'.format(
            dt=self.datetime.strftime('%Y-%d-%m %H:%M'),
            info_nonl=info_nonl,
            **self.data)
        return s[0:TERMINAL_WIDTH-14] + (' [...]' if len(s) > TERMINAL_WIDTH-14 else '')

    def download(self, name=None):
        url = self.data['url_stream']
        info_nonl = re.sub(r'[\n\r\s]+', ' ', self.data['info'])
        s = '{dt} {title} {info_nonl}'.format(
            dt=self.datetime.strftime('%Y-%d-%m--%H-%M'),
            info_nonl=self.info_nonl,
            **self.data).rstrip('.')
        name = name or repl_unsave(s+'.mp3')
        if not ARGS.dry_run:
            if not os.path.isfile(name) or ARGS.overwrite:
                logging.info('Downloading {url_stream} to {name}'.format(
                    name=name, **self.data))
                try:
                    urllib.request.urlretrieve(url, name, reporthook)
                except (urllib.error.HTTPError, urllib.error.ContentTooShortError) as e:
                    if os.path.isfile(name):
                        os.remove(name)
                    logging.warning('error downloading {}: {}'.format(url, e))
            else:
                logging.info('skipping existing file {}'.format(name))
        else:
            logging.info('dry run: skipping download of {}'.format(name))


class Config:
    def __init__(self, ini_file=None):
        self.ini_file = ini_file
        self.config = configparser.ConfigParser()
        self.config.optionxform = str
        try:
            with open(self.ini_file, 'r', encoding='utf-8') as f:
                self.config.read_file(f)
        except configparser.MissingSectionHeaderError:
            logging.critical('Unable to parse configuration file "{}"'.format(
                self.ini_file))
            sys.exit(1)
        self.broadcasts_rules = {}
        for section in self.config.sections():
            self.broadcasts_rules[section] = {}
            sr = self.broadcasts_rules[section]
            sr['ini'] = INI_DEFAULTS.copy()
            sr['ini'].update({
                key: value for key, value in self.config[section].items()
            })
            m = re.match(r'\s*(\d\d):(\d\d)\s*\-\s*(\d\d):(\d\d)\s*',
                sr['ini']['TimeWindow'])

            sr['start_time'] = datetime.time(int(m.group(1)), int(m.group(2)))
            sr['end_time'] = datetime.time(int(m.group(3)), int(m.group(4)))
            sr['search_regexes'] = {
                'title': re.compile(sr['ini']['title'], re.IGNORECASE)
            }
            sr['days'] = set(map(int, sr['ini']['Days'].split(',')))


class Broadcasts:
    def __init__(self, configuration, start_date=None, end_date=None):
        self.start_date = (start_date
            or datetime.date.today() - datetime.timedelta(days=10)) #####
        self.end_date = (end_date
            or datetime.date.today() - datetime.timedelta(days=1))
        self.config = configuration
        self.broadcasts = []
        self.actually_downloaded = []
        self.actually_converted = []
        self.broadcasts_of_interest = {}
        self._get_json()
        self.find_broadcasts_of_interest()

    def _get_json(self):
        d = self.start_date
        while d <= self.end_date:
            url = BASE_URL + d.strftime('%Y%m%d')
            logging.info('Reading {}'.format(url))
            json_day = json.loads(url_read(url))

            if 'list' in json_day:
                for b in json_day['list']:
                    self.broadcasts.append(Broadcast(b))
            else:
                logging.warning('No data for day {}'.format(d))
            d += datetime.timedelta(days=1)

    def find_broadcasts_of_interest(self):
        for section, rule in config.broadcasts_rules.items():
            if section not in self.broadcasts_of_interest:
                self.broadcasts_of_interest[section] = {
                    'rule': rule,
                    'broadcasts': set()
                }

            for broadcast in self.broadcasts:
                if (rule['start_time'] <= broadcast.time <= rule['end_time']
                    and broadcast.weekday in rule['days']
                    and rule['search_regexes']['title'].search(broadcast.data['title'])):
                        logging.debug('rule [{}]: {}'.format
                            (section, broadcast))
                        broadcast.full_dict['extended_info'] = get_broadcast_info_extended(
                            BASE_URL_DETAIL + broadcast.full_dict['id']).strip() or broadcast.full_dict['info']
                        self.broadcasts_of_interest[section]['broadcasts'].add(broadcast)

    def download_all_interesting(self, convert=True, tag=True, re_download=False):
        total_items = sum(len(v['broadcasts']) for _, v in self.broadcasts_of_interest.items())
        processed = 0
        for section, v in sorted(self.broadcasts_of_interest.items()):
            for broadcast in v['broadcasts']:
                processed += 1
                print('> processing {:> 3} of {:<3}: {!s:.80}...'.format(processed, total_items, broadcast))
                name_dict = broadcast.full_dict.copy()
                name_dict.update({
                    'SCRIPTDIR': SCRIPT_DIR,
                    'DOWNLOAD_BASEDIR': DOWNLOAD_BASEDIR,
                    'SECTION': section,
                    })

                download_fn_noext = repl_unsave(v['rule']['ini']['TargetName'].format(**name_dict))
                download_fn = download_fn_noext + '.mp3'
                download_folder = v['rule']['ini']['TargetDir'].format(**name_dict)
                actually_downloaded = False

                if not ARGS.dry_run and not os.path.isdir(download_folder) and not os.path.isfile(download_folder):
                    os.makedirs(download_folder)

                full_download_fn = os.path.join(download_folder, download_fn)
                full_conv_fn = os.path.join(download_folder, download_fn_noext + '.m4a')

                if (not os.path.isfile(full_download_fn)
                    and not ARGS.dry_run
                    and not os.path.isfile(full_conv_fn)):
                    broadcast.download(full_download_fn)
                    self.actually_downloaded.append(full_download_fn)
                    actually_downloaded = True
                else:
                    logging.info('skipping already existing file {}'.format(full_download_fn))

                if convert and not os.path.isfile(full_conv_fn):
                    r = convert_to_m4a(full_download_fn, conv_fn=full_conv_fn,
                        quality=int(v['rule']['ini']['Quality']),
                        aac_he_v2=ARGS.he2)
                    if r == 0:
                        self.actually_converted.append(full_conv_fn)
                if tag and convert and actually_downloaded: ## TODO r
                    tag_dict = {}
                    for key, value in v['rule']['ini'].items():
                        if key.startswith('Tag'):
                            tag_name = key[3:].lower()
                            tag_dict[tag_name] = value.format(**name_dict)
                    time.sleep(1)
                    tag_media_file(full_conv_fn, tag_dict)
                if v['rule']['ini']['KeepOriginal'] != 'True' and not ARGS.dry_run:
                    if not ARGS.dry_run and os.path.isfile(full_download_fn):
                        logging.info('deleting original file {}'.format(full_download_fn))
                        try:
                            os.remove(full_download_fn)
                        except IOError as e:
                            logging.warning('could not delete {}, {}'.format(full_download_fn, e))
                    else:
                        logging.debug('dry-run: would delete {}'.format(full_download_fn))

    def __str__(self):
        s = []
        for i, b in enumerate(self.broadcasts, 1):
            s.append('{: >4}: {}'.format(i, b))
        s.append('{} items.'.format(len(self.broadcasts)))
        return '\n'.join(s)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download media files from '
        'ORF Ö1 7-Tage on demand services'.format('s'),
        epilog='Written by Christoph Haunschmidt, 2014')

    parser.add_argument('--dry-run', action='store_true',
        default=False, help='dry run, do not do anything')

    parser.add_argument('--no-cache', action='store_true',
        default=False, help='do not use cached website data')

    parser.add_argument('--overwrite', action='store_true', default=False,
        help='overwrite existing files (default: %(default)s)')

    parser.add_argument('--he2', action='store_true', default=False,
        help='use AAC+ v2 instead of AAC+ v1 (default: %(default)s)')

    parser.add_argument('--ini-file', default=DEFAULT_INI_FN,
        help='ini file for the description of items to download')

    parser.add_argument('--ffmpeg', default=FFMPEG_EXECUTABLE,
        help='path to ffmpeg')

    parser.add_argument('--log', action='store', default='DEBUG',
        help='log level')

    ARGS = parser.parse_args()

    numeric_level = getattr(logging, ARGS.log.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(ARGS.log))

    config = Config(ARGS.ini_file)
    broadcasts = Broadcasts(config)

    broadcasts.download_all_interesting()

    if not ARGS.dry_run:
        if broadcasts.actually_downloaded:
            print('downloaded: \n{}'.format('\n'.join(broadcasts.actually_downloaded)))
        else:
            print('No files downloaded.')
        if broadcasts.actually_converted:
            print('converted: \n{}'.format('\n'.join(broadcasts.actually_converted)))
        else:
            print('No files converted.')
