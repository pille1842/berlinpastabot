import configparser
import datetime
import MySQLdb
import praw
import threading
import time

class BerlinpastaBase(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def connect(self):
        self.connect_to_mysql()
        self.connect_to_reddit()

    def connect_to_mysql(self):
        self.log('Connecting to MySQL database {}'.format(self.config['mysql']['db']))
        self.db = MySQLdb.connect(user=self.config['mysql']['user'],
                                  passwd=self.config['mysql']['passwd'],
                                  db=self.config['mysql']['db'])

    def connect_to_reddit(self):
        self.log('Connecting to Reddit as user {}'.format(self.config['reddit']['username']))
        self.reddit = praw.Reddit(client_id=self.config['reddit'][self.name+'_client_id'],
                                  client_secret=self.config['reddit'][self.name+'_client_secret'],
                                  user_agent='praw:BerlinpastaBot/'+self.name+':v1.0.0 (by /u/pille1842)',
                                  username=self.config['reddit']['username'],
                                  password=self.config['reddit']['password'])

    def log(self, message):
        now = datetime.datetime.now()
        print(now.strftime("[%Y-%m-%d %H:%M:%S]"), "<{}>".format(self.name), message)

class BerlinpastaMessagesBot(BerlinpastaBase):
    def __init__(self, threadID, name, config, reply_pattern):
        BerlinpastaBase.__init__(self, threadID, name)
        self.config = config
        self.reply_pattern = reply_pattern

    def parse_inbox(self):
        while True:
            for item in self.reddit.inbox.unread():
                if not isinstance(item, praw.models.Message):
                    self.log('Inbox item {} marked as read, is not a message'.format(item.fullname))
                    item.mark_read()
                    continue
                if 'IGNORE' not in item.body:
                    self.log('Inbox item {} marked as read, is no ignore request'.format(item.fullname))
                    item.mark_read()
                    continue
                # Add the user to the ignore table
                c = self.db.cursor()
                c.execute("""INSERT INTO ignorelist (username) VALUES (%s)
                             ON DUPLICATE KEY UPDATE username = %s""", (item.author, item.author))
                self.db.commit()
                c.close()
                item.reply(self.reply_pattern.format(username = item.author))
                self.log('Replied to item {} and blacklisted user {}'.format(item.fullname, item.author))
                item.mark_read()

    def run(self):
        self.log("Starting")
        self.connect()
        self.parse_inbox()
        self.log("Stopping")

class BerlinpastaCommentsBot(BerlinpastaBase):
    def __init__(self, threadID, name, config, reply_pattern):
        BerlinpastaBase.__init__(self, threadID, name)
        self.config = config
        self.reply_pattern = reply_pattern
        self.subredditname = config['reddit']['subreddit']

    def open_subreddit(self, subreddit):
        self.subreddit = self.reddit.subreddit(subreddit)

    def fetch_comments(self):
        return self.subreddit.stream.comments()

    def parse_comments(self, comments):
        for comment in comments:
            if comment.author == self.config['reddit']['username']:
                self.log('Skipping comment {} because it\'s from me'.format(comment.fullname))
                continue
            if 'berlin' not in comment.body.lower():
                continue
            # Check if user is blacklisted
            c = self.db.cursor()
            c.execute("""SELECT * FROM ignorelist
                         WHERE username = %s""", (comment.author,))
            if c.fetchone() != None:
                c.close()
                self.log('Skipping comment {} because user {} is blacklisted'.format(comment.fullname, comment.author))
                continue
            # Check if comment has already been processed
            c = self.db.cursor()
            c.execute("""SELECT * FROM comments
                         WHERE id = %s""", (comment.fullname,))
            if c.fetchone() != None:
                c.close()
                self.log('Skipping comment {} because it has already been processed'.format(comment.fullname))
                continue
            c.close()
            # All seems well. Reply
            self.reply(comment)

    def reply(self, comment):
        self.log('Replying to comment {}'.format(comment.fullname))
        try:
            my_reply = comment.reply(self.reply_pattern)
            c = self.db.cursor()
            c.execute("""INSERT INTO comments (id) VALUES (%s)""", (comment.fullname,))
            self.db.commit()
            c.close()
            return my_reply
        except praw.exceptions.APIException as e:
            self.log("Got an API exception:")
            self.log(e)
            self.log("Going to sleep a while.")
            time.sleep(60)
            self.log("Time to wake up!")
        except praw.exceptions.ClientException as e:
            self.log("Got a client exception:")
            self.log(e)
            self.log("Going to sleep a while.")
            time.sleep(60)
            self.log("Time to wake up!")

    def run(self):
        self.log("Starting")
        self.connect()
        self.open_subreddit(self.subredditname)
        comments = self.fetch_comments()
        self.parse_comments(comments)
        self.log("Stopping")

if __name__ == "__main__":
    print("BerlinpastaBot v1.0.0 by /u/pille1842")
    print("Reading configuration from config.ini")
    config = configparser.ConfigParser(interpolation=None)
    config.read('config.ini')
    with open('reply.txt', 'r') as patternfile:
        reply_pattern = patternfile.read()
    with open('blacklist.txt', 'r') as patternfile:
        blacklist_pattern = patternfile.read()
    print("Launching the bot")
    comments_thread = BerlinpastaCommentsBot(1, 'comments', config, reply_pattern)
    messages_thread = BerlinpastaMessagesBot(2, 'messages', config, blacklist_pattern)
    comments_thread.start()
    messages_thread.start()
    comments_thread.join()
    messages_thread.join()
    print("Stopping the bot")
