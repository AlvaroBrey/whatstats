# coding=utf-8
import sys, io
from pymongo import MongoClient
from datetime import datetime


def get_type(type_message):
    if type_message == u'<‎imagen omitida>':
        return 'image'
    elif type_message == u'<audio omitido>':
        return 'audio'
    elif type_message == u'<‎Video omitido>':
        return 'video'
    elif type_message == u'<‎vCard omitida>':
        return 'vCard'


def process_input(db, filename):
    with io.open(chat_file, buffering=2) as fp:
        lc = 0
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
                t = get_type(sp[0].strip())
                if not t:
                    db.messages.insert_one(
                        {
                            'date': datetime.strptime(date, '%d/%m/%y %H:%M:%S'),
                            'author': sp[0].strip(),
                            'content': sp[1].strip()
                        }
                    )
                else:
                    db.files.insert_one(
                        {
                            {
                                'date': datetime.strptime(date, '%d/%m/%y %H:%M:%S'),
                                'author': sp[0].strip(),
                                'type': t
                            }
                        }
                    )
            lc += 1
            if lc % 500 == 0:
                print 'Processed ' + str(lc) + ' messages'

        fp.close()


def print_rank(m_rank, limit=-1):
    i = 1
    for rank in m_rank:
        print str(i) + ' - ' + rank['_id'] + ': ' + str(rank['count'])
        i += 1
        if 0 < limit < i:
            break


if __name__ == '__main__':

    client = MongoClient()
    # clear data
    client.drop_database('whatstats')

    db = client['whatstats']

    if len(sys.argv) != 2:
        print 'Need exactly one argument (file to read)'
        sys.exit(-1)

    chat_file = sys.argv[1]

    process_input(db, chat_file)

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

    print 'Messages rank :'
    print_rank(messages_rank)
