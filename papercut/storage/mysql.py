# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.

# https://github.com/mariadb-corporation/mariadb-connector-python/wiki/usage.md
import mariadb
# https://stackoverflow.com/questions/372885/how-do-i-connect-to-a-mysql-database-in-python
# import pymysql
# pymysql.install_as_MySQLdb()

# https://pypika.readthedocs.io/en/latest/index.html
from pypika import Query, MySQLQuery, Table, Field, Order
from pypika import functions as fn

import time
import re
from email.parser import Parser
import uuid

from pprint import pprint

import papercut.settings
import papercut.storage.strutil as strutil

settings = papercut.settings.CONF()

# we don't need to compile the regexps everytime..
singleline_regexp = re.compile("^\.", re.M)
from_regexp = re.compile("^From:(.*)", re.M)
subject_regexp = re.compile("^Subject:(.*)", re.M)
references_regexp = re.compile("^References:(.*)<(.*)>", re.M)

class Papercut_Storage:
    """
    Storage Backend interface for saving the article information in a MySQL database.

    T@s is not a storage to implement a web board -> nntp gateway, but a standalone nntp server.
    """

    def __init__(self):
        pprint("PAPERCUT STORAGE initialized")

        self.conn = mariadb.connect(host=settings.dbhost, database=settings.dbname, user=settings.dbuser, password=settings.dbpass)
        self.cursor = self.conn.cursor()
        
        # self.conn2 = pymysql.connect(host=settings.dbhost, db=settings.dbname, user=settings.dbuser, passwd=settings.dbpass)
        # self.cursor2 = self.conn2.cursor()

    def quote_string(self, text):
        """Quotes strings the MySQL way."""
        return text.replace("'", "\\'")

    def get_body(self, lines):
        pass

    def get_header(self, lines):
        pass

    def group_exists(self, group_name):
        """ Count(id) of group in DB. 
            return True if group exists
            return False if not
        """
        newsgroups = Table("newsgroups")
        q = MySQLQuery.from_(newsgroups).select(fn.Count(newsgroups.id)).where(newsgroups.group_name == str(group_name).lower())

        stmt = q.get_sql()

        self.cursor.execute(stmt)
        result = self.cursor.fetchone()[0]

        if result:
            return True
        else:
            return False

    def article_exists(self, group_name, style, range):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    COUNT(*) AS check
                FROM
                    %s
                WHERE
                    """ % (table_name)
        if style == 'range':
            stmt = "%s id > %s" % (stmt, range[0])
            if len(range) == 2:
                stmt = "%s AND id < %s" % (stmt, range[1])
        else:
            stmt = "%s id = %s" % (stmt, range[0])
        self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_first_article(self, group_name):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    IF(MIN(id) IS NULL, 0, MIN(id)) AS first_article
                FROM
                    %s""" % (table_name)
        num_rows = self.cursor.execute(stmt)
        return self.cursor.fetchone()[0]

    def get_group_stats(self, group_name):
        total, max, min = self.get_table_stats(self.get_table_name(group_name))
        return (total, min, max, group_name)

    def get_table_stats(self, table_name):
        """ Get total, first, last articles in table_name from MariaDB"""
        table = Table(table_name)
        q = MySQLQuery.from_(table)\
            .select(fn.Count(table.id), fn.Min(table.id), fn.Max(table.id))

        nr = self.cursor.execute(q.get_sql())
        result = self.cursor.fetchone()

        total = result[0]
        first = 0
        last = 0
        if result[1]:
            first = result[1]
        if result[2]:
            last = result[2]

        return [total, first, last]

    def get_table_name(self, group_name):
        ''' Return Newsgroup table name as string from group name'''
        newsgroups = Table('newsgroups')
        q = MySQLQuery.from_(newsgroups).select('id', 'table_name').where(newsgroups.group_name == group_name)

        self.cursor.execute(q.get_sql())
        id, table_name = self.cursor.fetchone()
        return table_name

    def get_message_id(self, msg_num, group):
        return '<%s@%s>' % (msg_num, group)

    def get_NEWGROUPS(self, ts, group='%'):
        return None

    def get_NEWNEWS(self, ts, group='*'):
        groups_table = Table('newsgroups')
        q = MySQLQuery.from_(groups_table).select('name', 'table_name').where(groups_table.name == group_name.replace('*', '%')).orderby('id', order=Order.desc)

        self.cursor.execute(q.get_sql())
        result = list(self.cursor.fetchall())
        articles = []
        for group, table in result:
            t = Table(table)
            q2 = MySQLQuery.from_(table).select('id').where(fn.Timestamp(t.datestamp) >= ts)

            num_rows = self.cursor.execute(q2.get_sql())

            if num_rows == 0:
                continue
            ids = list(self.cursor.fetchall())
            for id in ids:
                articles.append("<%s@%s>" % (id, group))
        if len(articles) == 0:
            return ''
        else:
            return "\r\n".join(articles)

    def get_GROUP(self, group_name):
        table_name = self.get_table_name(group_name)
        result = self.get_table_stats(table_name)
        return (result[0], result[1], result[2])

    def get_LIST(self):
        ''' 
        Return newsgroups list as array of strings
        ''' 
        table = Table('newsgroups')
        q = MySQLQuery.from_(table).select('group_name', 'table_name').orderby('group_name', order=Order.asc)

        self.cursor.execute(q.get_sql())
        result = list(self.cursor.fetchall())

        if len(result) == 0:
            return []
        else:
            groups = []
            for group_name, table_name in result:
                total, maximum, minimum = self.get_table_stats(table_name)
                groups.append("%s %s %s" % (group_name, maximum, minimum))
            return groups

    def get_STAT(self, group_name, id):
        ''' Get group stat '''
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id
                FROM
                    %s
                WHERE
                    id=%s""" % (table_name, id)
        return self.cursor.execute(stmt)

    def get_article_by_message_id(self, group_name, message_id):
        ''' Find an article by its Message-ID '''

        # Generate query
        table_name = self.get_table_name(group_name)
        article = Table(table_name)
        q = MySQLQuery.from_(article).select('id').where(article.message_id == message_id)

        # Get num of rows 
        num_rows = self.cursor.execute(q.get_sql())
        # If 0 article not found. Return None
        if num_rows == 0:
            return None

        # Assign the article ID from fetched list
        article_id = self.cursor.fetchone()[0]

        # Execute the real function with right article ID
        return self.get_ARTICLE(group_name, article_id)


    def get_ARTICLE(self, group_name, id):
        table_name = self.get_table_name(group_name)
        article = Table(table_name)
        q = MySQLQuery.from_(article).select('id', 'from', 'references', 'message_id', 'parent_id', 'subject', 'body', 'created_at').where(article.id == id)

        # Get num of resultats
        num_rows = self.cursor.execute(q.get_sql())

        if num_rows == 0:
            return None

        try:
            result = list(self.cursor.fetchone())
        except:
            pprint('[DEBUG] Article {} not found in DB'.format(id))
            return None

        headers = []
        headers.append("Path: %s" % (settings.domain_name))
        headers.append("From: %s" % (result[1]))
        headers.append("Newsgroups: %s" % (group_name))
        headers.append("Date: {}".format(result[7].strftime('%a, %d %b %Y %H:%M:%S +0100')))
        headers.append("Subject: %s" % (result[5]))
        headers.append("Message-ID: {}".format(result[3]))
        headers.append("Xref: %s %s:%s" % (settings.domain_name, group_name, result[0]))

        if result[5] != 0:
            headers.append("References: {}".format(result[2]))
        
        return ("\r\n".join(headers), strutil.format_body(result[6]))

    def get_LAST(self, group_name, current_id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id
                FROM
                    %s
                WHERE
                    id < %s
                ORDER BY
                    id DESC
                LIMIT 0, 1""" % (table_name, current_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_NEXT(self, group_name, current_id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id
                FROM
                    %s
                WHERE
                    id > %s
                ORDER BY
                    id ASC
                LIMIT 0, 1""" % (table_name, current_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        return self.cursor.fetchone()[0]

    def get_HEAD(self, group_name, id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id,
                    author,
                    subject,
                    UNIX_TIMESTAMP(datestamp) AS datestamp,
                    parent
                FROM
                    %s
                WHERE
                    id=%s""" % (table_name, id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchone())
        headers = []
        headers.append("Path: %s" % (settings.nntp_hostname))
        headers.append("From: %s" % (result[1]))
        headers.append("Newsgroups: %s" % (group_name))
        headers.append("Date: %s" %
                       (strutil.get_formatted_time(time.localtime(result[3]))))
        headers.append("Subject: %s" % (result[2]))
        headers.append("Message-ID: <%s@%s>" % (result[0], group_name))
        headers.append("Xref: %s %s:%s" %
                       (settings.nntp_hostname, group_name, result[0]))
        if result[4] != 0:
            headers.append("References: <%s@%s>" % (result[4], group_name))
        return "\r\n".join(headers)

    def get_BODY(self, group_name, id):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    body
                FROM
                    %s
                WHERE
                    id=%s""" % (table_name, id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        else:
            return strutil.format_body(self.cursor.fetchone()[0])

    def get_XOVER(self, group_name, start_id, end_id='ggg'):

        group = Table(self.get_table_name(group_name))


        if end_id != 'ggg':
            q = MySQLQuery.from_(group).select('*').where((group.id >= start_id) & (group.id <= end_id))
        else:
            q = MySQLQuery.from_(group).select('*').where(group.id >= start_id)

        self.cursor.execute(q.get_sql())

        result = list(self.cursor.fetchall())
        
        overviews = []
        for row in result:
            pprint(row)
            body = row[9]
            bytes_length = len(body)
            line_num = len(body.split('\n'))
            # res_time = row[10].strftime('%a %d %b %Y %H:%M:%S %Z')
            res_time = row[10].strftime('%a, %d %b %Y %H:%M:%S +0100')
            message_id = row[5]
            xref = 'Xref: %s %s:%s' % (settings.domain_name, group_name, row[0])

            if row[10] != 0:
                reference = row[10]
                # reference = "<%s@%s>" % (row[10], group_name)
            else:
                reference = ""

            # message_number <tab> subject <tab> author <tab> date <tab> message_id <tab> reference <tab> bytes <tab> lines <tab> xref
            overviews.append("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (row[0], row[8], row[3], res_time, message_id, reference, bytes_length, line_num, xref))
        
        return "\r\n".join(overviews)

    def get_XPAT(self, group_name, header, pattern, start_id, end_id='ggg'):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id,
                    parent,
                    author,
                    subject,
                    UNIX_TIMESTAMP(datestamp) AS datestamp,
                    bytes,
                    line_num
                FROM
                    %s
                WHERE
                    id >= %s AND""" % (table_name, header, strutil.format_wildcards(pattern), start_id)
        if header.upper() == 'SUBJECT':
            stmt = "%s AND subject REGEXP '%s'" % (
                stmt, strutil.format_wildcards(pattern))
        elif header.upper() == 'FROM':
            stmt = "%s AND (author REGEXP '%s' OR email REGEXP '%s')" % (
                stmt, strutil.format_wildcards(pattern), strutil.format_wildcards(pattern))
        elif header.upper() == 'DATE':
            stmt = "%s AND %s" % (stmt, pattern)
        if end_id != 'ggg':
            stmt = "%s AND id <= %s" % (stmt, end_id)
        num_rows = self.cursor.execute(stmt)
        if num_rows == 0:
            return None
        result = list(self.cursor.fetchall())
        hdrs = []
        for row in result:
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[3]))
            elif header.upper() == 'FROM':
                hdrs.append('%s %s' % (row[0], row[2]))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (
                    row[0], strutil.get_formatted_time(time.localtime(result[4]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append('%s <%s@%s>' % (row[0], row[0], group_name))
            elif (header.upper() == 'REFERENCES') and (row[1] != 0):
                hdrs.append('%s <%s@%s>' % (row[0], row[1], group_name))
            elif header.upper() == 'BYTES':
                hdrs.append('%s %s' % (row[0], row[5]))
            elif header.upper() == 'LINES':
                hdrs.append('%s %s' % (row[0], row[6]))
            elif header.upper() == 'XREF':
                hdrs.append('%s %s %s:%s' %
                            (row[0], settings.nntp_hostname, group_name, row[0]))
        if len(hdrs) == 0:
            return ""
        else:
            return "\r\n".join(hdrs)

    def get_LISTGROUP(self, group_name):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id
                FROM
                    %s
                ORDER BY
                    id ASC""" % (table_name)
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s" % k for k in result])

    def get_XGTITLE(self, pattern=None):
        stmt = """
                SELECT
                    name,
                    description
                FROM
                    newsgroups
                WHERE
                    LENGTH(name) > 0"""
        if pattern != None:
            stmt = stmt + """ AND
                    name REGEXP '%s'""" % (strutil.format_wildcards(pattern))
        stmt = stmt + """
                ORDER BY
                    name ASC"""
        self.cursor.execute(stmt)
        result = list(self.cursor.fetchall())
        return "\r\n".join(["%s %s" % (k, v) for k, v in result])

    def get_XHDR(self, group_name, header, style, range):
        table_name = self.get_table_name(group_name)
        stmt = """
                SELECT
                    id,
                    parent,
                    author,
                    subject,
                    UNIX_TIMESTAMP(datestamp) AS datestamp,
                    bytes,
                    line_num
                FROM
                    %s
                WHERE
                    """ % (table_name)
        if style == 'range':
            stmt = '%s id >= %s' % (stmt, range[0])
            if len(range) == 2:
                stmt = '%s AND id <= %s' % (stmt, range[1])
        else:
            stmt = '%s id = %s' % (stmt, range[0])
        if self.cursor.execute(stmt) == 0:
            return None
        result = self.cursor.fetchall()
        hdrs = []
        for row in result:
            if header.upper() == 'SUBJECT':
                hdrs.append('%s %s' % (row[0], row[3]))
            elif header.upper() == 'FROM':
                hdrs.append('%s %s' % (row[0], row[2]))
            elif header.upper() == 'DATE':
                hdrs.append('%s %s' % (
                    row[0], strutil.get_formatted_time(time.localtime(result[4]))))
            elif header.upper() == 'MESSAGE-ID':
                hdrs.append('%s <%s@%s>' % (row[0], row[0], group_name))
            elif (header.upper() == 'REFERENCES') and (row[1] != 0):
                hdrs.append('%s <%s@%s>' % (row[0], row[1], group_name))
            elif header.upper() == 'BYTES':
                hdrs.append('%s %s' % (row[0], row[6]))
            elif header.upper() == 'LINES':
                hdrs.append('%s %s' % (row[0], row[7]))
            elif header.upper() == 'XREF':
                hdrs.append('%s %s %s:%s' %
                            (row[0], settings.nntp_hostname, group_name, row[0]))
        if len(hdrs) == 0:
            return ""
        else:
            return "\r\n".join(hdrs)

    def do_Post(self, group_name, ip_address, author, message_id, subject, references, body, username = ''):
        bytes_length = len(body), # Useless ?
        line_num = len(body.split('\n')) # Useless ?

        group = Table(self.get_table_name(group_name))
        references_message_id = None # Message-ID string of the referenced article
        parent_id = None
        thread_id = None

        if references:
            # Get the referenced article parent_id,  thread_id and references_message_id
            references_query = MySQLQuery.from_(group).select('id', 'thread_id', 'message_id').where(group.message_id == references)
            try:
                references_find_result = self.cursor.execute(references_query.get_sql())
                parent_id, thread_id, references_message_id = self.cursor.fetchone()
            except:
                pprint('[DEBUG] Can\'t find parent article in database')
                return False

        # Querty to post article
        post_query = MySQLQuery.into(group)\
            .columns('id', 'ip_address', 'username', 'from', 'references', 'message_id', 'thread_id', 'parent_id', 'subject', 'body', 'created_at', 'updated_at')\
            .insert('', ip_address, username, author, references_message_id, message_id, thread_id, parent_id, subject, body, fn.Now(), fn.Now())

        r = self.cursor.execute(post_query.get_sql())

        if not thread_id:
            # Update the thread_id of the posted article (thread_id = id)
            article_inserted_id = self.cursor.lastrowid
            thread_id = article_inserted_id
            q2 = MySQLQuery.update(group)\
                .set(group.thread_id, thread_id)\
                .set(group.updated_at, fn.Now())\
                .where(group.id == article_inserted_id)

            r = self.cursor.execute(q2.get_sql())

        if r == None:
            return True
        else:
            return False