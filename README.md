# whatstats
Simple stats for whatsapp group chats

## Requirements:
- Python 2.7 & 3.5
- PyMongo
- MongoDB installed and running

## Usage:
```
  python whatstats.py chat.txt
```
where `chat.txt` is a chat exported through WhatsApp 'send by email' functionality.

Currently **only spanish language** and **only chats with no media attachments** are supported.
If you want to use another language you can set your phone's language to Spanish just for exporting the data, but 'common words' ranking won't ommit articles, prepositions etc. 
