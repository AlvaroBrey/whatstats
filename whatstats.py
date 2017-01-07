# coding=utf-8
import io
import operator
import re
import sys
from datetime import datetime

from pymongo import MongoClient

INVALID_WORDS = []


def get_type(type_message):
    """

    :param type_message:
    :rtype:str
    """
    if type_message == '<‎imagen omitida>':
        return 'image'
    elif type_message == '<audio omitido>':
        return 'audio'
    elif type_message == '<‎Video omitido>':
        return 'video'
    elif type_message == '<‎vCard omitida>':
        return 'vCard'
    elif type_message == '<Media omitted>':
        return 'generic_media'
    return ''


def process_input_ios(db, filename):
    with io.open(chat_file, buffering=2) as fp:
        chars = {}
        lc = mc = fc = wc = 0
        for line in fp:
            clean = line.strip()

            date = ''
            colon_count = 0
            for char in clean:
                if char == ':':
                    colon_count += 1
                    if colon_count == 3:
                        break
                date += char

            rest = clean.replace(date, '')[1:].strip()
            sp = rest.split(':', 1)

            if len(sp) == 2:  # message, else leaving/joining/etc
                content = sp[1].strip()
                t = get_type(content)
                author = sp[0].strip()
                dt = datetime.strptime(date, '%d/%m/%y %H:%M:%S')
                if not t:
                    db.messages.insert_one(
                        {
                            'date': dt,
                            'author': author,
                            'content': content
                        }
                    )

                    if author in chars:
                        chars[author] += len(content)
                    else:
                        chars[author] = len(content)

                    words = content.split()
                    for word in words:
                        w = re.sub(r'\W+', '', word.lower())
                        if w not in INVALID_WORDS and len(w) > 1:
                            db.words.insert_one(
                                {
                                    'date': dt,
                                    'author': author,
                                    'word': w
                                }
                            )
                            wc += 1

                    mc += 1
                else:
                    db.files.insert_one(
                        {
                            'date': dt,
                            'author': author,
                            'type': t
                        }
                    )
                    fc += 1
            lc += 1
            if lc % 500 == 0:
                print('Processed ' + str(lc) + ' lines')

        fp.close()
        return lc, mc, fc, wc, chars


def process_input_android(db, filename):
    with io.open(chat_file, buffering=2) as fp:
        chars = {}
        lc = mc = fc = wc = 0
        for line in fp:
            clean = line.strip()

            first_split = clean.split('-', 1)

            if len(first_split) != 2:
                # todo deal with multiline messages
                continue
            date = first_split[0].strip()
            try:
                dt = datetime.strptime(date, '%d/%m/%y, %H:%M')
            except:
                continue
            rest = first_split[1].strip()
            sp = rest.split(':', 1)

            if len(sp) == 2:  # message, else leaving/joining/etc
                content = sp[1].strip()
                t = get_type(content)
                author = sp[0].strip()
                if not t:
                    db.messages.insert_one(
                        {
                            'date': dt,
                            'author': author,
                            'content': content
                        }
                    )

                    if author in chars:
                        chars[author] += len(content)
                    else:
                        chars[author] = len(content)

                    words = content.split()
                    for word in words:
                        w = re.sub(r'\W+', '', word.lower())
                        if w not in INVALID_WORDS and len(w) > 1:
                            db.words.insert_one(
                                {
                                    'date': dt,
                                    'author': author,
                                    'word': w
                                }
                            )
                            wc += 1

                    mc += 1
                else:
                    db.files.insert_one(
                        {
                            'date': dt,
                            'author': author,
                            'type': t
                        }
                    )
                    fc += 1
            lc += 1
            if lc % 500 == 0:
                print('Processed ' + str(lc) + ' lines')

        fp.close()
        return lc, mc, fc, wc, chars


def print_rank(m_rank, limit=-1, total=-1):
    i = 1
    for rank in m_rank:
        p = (' (' + '{:.1%}'.format(rank['count'] / total) + ')') if total > 0 else ''
        print(str(i) + ' - ' + rank['_id'] + ': ' + str(rank['count']) + p)
        i += 1
        if 0 < limit < i:
            break


def read_invalid_words():
    with open('invalid_words.dat') as fp:
        for line in fp:
            INVALID_WORDS.append(line.strip())
        fp.close()


if __name__ == '__main__':

    client = MongoClient()
    # clear data
    client.drop_database('whatstats')

    read_invalid_words()

    db = client['whatstats']

    if len(sys.argv) != 3:
        print('Need exactly two arguments (file to read and format)')
        sys.exit(1)

    chat_file = sys.argv[1]

    if sys.argv[2] == 'ios':
        (lc, mc, fc, wc, chars) = process_input_ios(db, chat_file)
    elif sys.argv[2] == 'android':
        (lc, mc, fc, wc, chars) = process_input_android(db, chat_file)
    else:
        print('Format can only be "android" or "ios"')
        sys.exit(2)

    messages_rank = db.messages.aggregate(
        [
            {'$group': {
                '_id': '$author',
                'count': {
                    '$sum': 1
                }
            }},
            {'$sort': {'count': -1}}
        ]
    )

    print('')
    print('Messages rank  (total: ' + str(mc) + ' messages)')
    print_rank(messages_rank, total=mc)

    files_rank = db.files.aggregate(
        [
            {'$group': {
                '_id': '$author',
                'count': {
                    '$sum': 1
                }
            }},
            {'$sort': {'count': -1}}
        ]
    )

    print('')
    print('Files rank  (total: ' + str(fc) + ' files)')
    print_rank(files_rank, total=fc)

    charcount = 0
    for key, value in chars.items():
        charcount += value
    print('')
    print('Characters rank (total: ' + str(charcount) + ' characters)')

    sorted_x = sorted(chars.items(), key=operator.itemgetter(1), reverse=True)
    i = 1
    for name, count in sorted_x:
        p = (' (' + '{:.1%}'.format(count / charcount) + ')')
        print(str(i) + ' - ' + name + ': ' + str(count) + p)
        i += 1

    words_rank = db.words.aggregate(
        [
            {'$group': {
                '_id': '$author',
                'count': {
                    '$sum': 1
                }
            }},
            {'$sort': {'count': -1}}
        ]
    )

    print('')
    print('Words rank (excluding common words) (total: ' + str(wc) + ' words)')
    print_rank(words_rank, total=wc)

    common_words = db.words.aggregate(
        [
            {'$group': {
                '_id': '$word',
                'count': {
                    '$sum': 1
                }
            }},
            {'$sort': {'count': -1}}
        ]
    )

    print('')
    print('Most common words (excluding prepositions etc)')
    print_rank(common_words, total=wc, limit=25)

    client.drop_database('whatstats')
