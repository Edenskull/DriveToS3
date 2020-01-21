import sqlite3
import os
import math

from kleenlogger import kleenlogger


class Database:
    def __init__(self):
        if not os.path.exists('./process.db'):
            open('process.db', 'w').close()
        self.conn = sqlite3.connect('./process.db')

    def create_table(self):
        kleenlogger.logger.debug('Dropping and creating tables')
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS files")
        cursor.execute("DROP TABLE IF EXISTS config")
        cursor.execute("CREATE TABLE files (id, upload, size)")
        cursor.execute("CREATE TABLE config (type, url, awskey, awssecret, bucket)")
        self.conn.commit()
        cursor.close()
        kleenlogger.logger.debug('Tables creation complete')

    def inject_config(self, mode, url, aws_key, aws_secret, bucket):
        kleenlogger.logger.debug('Injecting configuration to table config')
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO config VALUES (?, ?, ?, ?, ?)", (mode, url, aws_key, aws_secret, bucket,))
        self.conn.commit()
        cursor.close()
        kleenlogger.logger.debug('Injections complete')

    def get_config(self):
        kleenlogger.logger.debug('Retrieving config')
        cursor = self.conn.cursor()
        cursor.execute("SELECT type, url, awskey, awssecret, bucket FROM config")
        res = cursor.fetchone()
        cursor.close()
        kleenlogger.logger.debug('Config retrieved')
        return res

    def insert_row(self, file_id, file_size):
        kleenlogger.logger.debug('Inserting row to tables file with file {}'.format(file_id))
        cursor = self.conn.cursor()
        cursor.execute("INSERT INTO files VALUES (?, 0, ?)", (file_id, file_size))
        self.conn.commit()
        cursor.close()
        kleenlogger.logger.debug('Insertion complete')

    def update_row(self, file_id):
        kleenlogger.logger.debug('Updating row of the file {}'.format(file_id))
        cursor = self.conn.cursor()
        cursor.execute("UPDATE files SET upload=1 WHERE id=?", (file_id,))
        self.conn.commit()
        cursor.close()
        kleenlogger.logger.debug('Updating complete')

    def get_upload_size(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT SUM(size) as total_size FROM files WHERE upload=1")
        res = cursor.fetchone()
        cursor.close()
        return Database.convert_size(res[0])

    @staticmethod
    def convert_size(size_bytes):
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def is_uploaded(self, file_id):
        cursor = self.conn.cursor()
        cursor.execute("SELECT upload FROM files WHERE id=?", (file_id,))
        res = cursor.fetchone()
        cursor.close()
        if res is None:
            return False
        else:
            return True if res[0] else False

    def close_conn(self):
        self.conn.close()


database = Database()
