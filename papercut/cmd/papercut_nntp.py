#!/usr/bin/env python
#
# Copyright (c) 2002 Joao Prado Maia. See the LICENSE file for more information.
# Copyright (c) 2016 Johannes Grassler. See the LICENSE file for more information.

import socketserver
import sys
import os
import signal
import time
import re
import traceback
import io
import email
import uuid

from pprint import pprint

# set this to 0 (zero) for real world use
__DEBUG__ = 1
__CLIENTDEBUG__ = 1
__VERSION__ = "0.1"

# papercut based modules
import papercut.settings
import papercut.storage.mysql as Papercut_Storage

settings = papercut.settings.CONF()

# how many seconds to wait for data from the clients (draft 20 of the new NNTP protocol says at least 3 minutes)
__TIMEOUT__ = 180

# some constants to hold the possible responses
ERR_NOTCAPABLE = '500 command not recognized'
ERR_CMDSYNTAXERROR = '501 command syntax error (or un-implemented option)'
ERR_NOSUCHGROUP = '411 no such news group'
ERR_NOGROUPSELECTED = '412 no newsgroup has been selected'
ERR_NOARTICLESELECTED = '420 no current article has been selected'
ERR_NOARTICLERETURNED = '420 No article(s) selected'
ERR_NOPREVIOUSARTICLE = '422 no previous article in this group'
ERR_NONEXTARTICLE = '421 no next article in this group'
ERR_NOSUCHARTICLENUM = '423 no such article in this group'
ERR_NOSUCHARTICLE = '430 no such article'
ERR_NOIHAVEHERE = '435 article not wanted - do not send it'
ERR_NOSTREAM = '500 Command not understood'
ERR_TIMEOUT = '503 Timeout after %s seconds, closing connection.'
ERR_NOTPERFORMED = '503 program error, function not performed'
ERR_POSTINGFAILED = '441 Posting failed'
ERR_AUTH_NO_PERMISSION = '502 No permission'
ERR_NODESCAVAILABLE = '481 Groups and descriptions unavailable'
STATUS_SLAVE = '202 slave status noted'
STATUS_POSTMODE = '200 Hello, you can post'
STATUS_NOPOSTMODE = '201 Hello, you can\'t post'
STATUS_HELPMSG = '100 help text follows'
STATUS_GROUPSELECTED = '211 %s %s %s %s group selected'
STATUS_LIST = '215 list of newsgroups follows'
STATUS_STAT = '223 %s %s article retrieved - request text separately'
STATUS_ARTICLE = '220 %s %s All of the article follows'
STATUS_NEWGROUPS = '231 list of new newsgroups follows'
STATUS_NEWNEWS = '230 list of new articles by message-id follows'
STATUS_HEAD = '221 %s %s article retrieved - head follows'
STATUS_BODY = '222 %s %s article retrieved - body follows'
STATUS_READYNOPOST = '201 %s Papercut %s server ready (no posting allowed)'
STATUS_READYOKPOST = '200 %s Papercut %s server ready (posting allowed)'
STATUS_CLOSING = '205 closing connection - goodbye!'
STATUS_XOVER = '224 Overview information follows'
STATUS_XPAT = '221 Header follows'
STATUS_LISTGROUP = '211 %s %s %s %s'
STATUS_XGTITLE = '282 list of groups and descriptions follows'
STATUS_LISTNEWSGROUPS = '215 information follows'
STATUS_XHDR = '221 Header follows'
STATUS_DATE = '111 %s'
STATUS_OVERVIEWFMT = '215 information follows'
STATUS_EXTENSIONS = '215 Extensions supported by server.'
STATUS_SENDARTICLE = '340 Send article to be posted'
STATUS_READONLYSERVER = '440 Posting not allowed'
STATUS_POSTSUCCESSFULL = '240 Article received ok'
STATUS_AUTH_REQUIRED = '480 Authentication required'
STATUS_AUTH_ACCEPTED = '281 Authentication accepted'
STATUS_AUTH_CONTINUE = '381 More authentication information required'
STATUS_SERVER_VERSION = '200 Papercut %s' % (__VERSION__)

# the currently supported overview headers
overview_headers = ('Subject:', 'From:', 'Date:', 'Message-ID:',
                    'References:', 'Bytes:', 'Lines:', 'Xref:full')

# we don't need to create the regular expression objects for every request,
# so let's create them just once and re-use as needed
contenttype_regexp = re.compile("^Content-Type:(.*);", re.M)
authinfo_regexp = re.compile("AUTHINFO PASS")

if os.name == 'posix' and 0:
    class NNTPServer(socketserver.ForkingTCPServer):
        allow_reuse_address = 1
        if settings.max_connections:
            max_children = settings.max_connections
else:
    class NNTPServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = 1

# Load storage module
# if settings.storage_backend == 'mysql':
#     temp = __import__('papercut.storage.%s' % (backends[h]), globals(), locals(), ['Papercut_Storage'])

# load authentication module, if needed
if settings.nntp_auth == 'yes':
    temp = __import__('papercut.auth.%s' % (settings.auth_backend), globals(), locals(), ['Papercut_Auth'])
    auth = temp.Papercut_Auth()


class NNTPRequestHandler(socketserver.StreamRequestHandler):
    # this is the list of supported commands
    commands = ('ARTICLE', 'BODY', 'HEAD',
                'STAT', 'GROUP', 'LIST', 'POST',
                'HELP', 'LAST', 'NEWGROUPS',
                'NEWNEWS', 'NEXT', 'QUIT',
                'MODE', 'XOVER', 'XPAT',
                'LISTGROUP', 'XGTITLE', 'XHDR',
                'XGTITLE', 'XHDR',
                'SLAVE', 'DATE', 'IHAVE',
                'OVER', 'HDR', 'AUTHINFO',
                'CAPABILITIES',
                'XROVER', 'XVERSION')
    # this is the list of list of extensions supported that are obviously not in the official NNTP document
    extensions = ('XOVER', 'XPAT', 'LISTGROUP',
                  'XGTITLE', 'XHDR', 'MODE',
                  'OVER', 'HDR', 'AUTHINFO',
                  'XROVER', 'XVERSION')

    backend = Papercut_Storage.Papercut_Storage()
    terminated = 0
    selected_article = 'ggg'
    selected_group = 'ggg'
    tokens = []
    sending_article = 0
    article_lines = []
    broken_oe_checker = 0
    auth_username = ''
    auth_first_name = ''
    auth_surname = ''

    def handle_timeout(self, signum, frame):
        self.terminated = 1
        settings.logEvent('Connection timed out from %s' % (self.client_address[0]))

    def handle(self):
        # Do Welcome message
        settings.logEvent('Connection from %s' % (self.client_address[0]))
        if settings.server_type == 'read-only':
            self.send_response(STATUS_READYNOPOST % (settings.nntp_hostname, __VERSION__))
        else:
            self.send_response(STATUS_READYOKPOST % (settings.nntp_hostname, __VERSION__))
        
        # Handle
        while not self.terminated:
            if self.sending_article == 0:
                self.article_lines = []
            
            if os.name == 'posix' and 0:
                signal.signal(signal.SIGALRM, self.handle_timeout)
                signal.alarm(__TIMEOUT__)
            
            try:
                self.inputline = self.rfile.readline()
            except IOError:
                continue
            
            if os.name == 'posix' and 0:
                signal.alarm(0)
            
            if __CLIENTDEBUG__:
                print("%s > %s" % (self.client_address[0], repr(self.inputline)))
            
            # Strip spaces only if NOT receiving article
            if not self.sending_article:
                # Remove \r\n from input then decode it as utf8
                line = self.inputline.strip().decode('latin-1', 'replace')
            else:
                # Decode input as utf8
                line = self.inputline.decode('latin-1', 'replace')
           
           # somehow outlook express sends a lot of newlines (so we need to kill those users when this happens)
            if (not self.sending_article) and (line == ''):
                self.broken_oe_checker += 1
                if self.broken_oe_checker == 10:
                    self.terminated = 1
                continue
            self.tokens = line.split(' ')

            # NNTP commands are case-insensitive
            command = self.tokens[0].upper()

            # don't save the password in the log file
            match = authinfo_regexp.search(line)
            if not match:
                settings.logEvent('Received request: %s' % (line))

            if command == 'POST':
                if settings.server_type == 'read-only':
                    settings.logEvent('Error - Read-only server received a post request from \'%s\'' % self.client_address[0])
                    self.send_response(STATUS_READONLYSERVER)
                else:
                    if settings.nntp_auth == 'yes' and self.auth_username == '':
                        self.send_response(STATUS_AUTH_REQUIRED)
                    else:
                        self.sending_article = 1
                        self.send_response(STATUS_SENDARTICLE)
            else:
                if settings.nntp_auth == 'yes' and self.auth_username == '' and command not in ('AUTHINFO', 'MODE'):
                    self.send_response(STATUS_AUTH_REQUIRED)
                else:
                    if self.sending_article:
                        if line == '.\r\n':
                            self.sending_article = 0
                            try:
                                self.do_Post()
                            except:
                                # use a temporary file handle object to store the traceback information
                                temp = io.StringIO()
                                traceback.print_exc(file=temp)
                                temp_msg = temp.getvalue()
                                # save on the log file
                                settings.logEvent('Error - Posting failed for user from \'%s\' (exception triggered)' % self.client_address[0])
                                settings.logEvent(temp_msg)
                                if __DEBUG__:
                                    print('Error - Posting failed for user from \'%s\' (exception triggered; details below)' % self.client_address[0])
                                    print(temp_msg)
                                self.send_response(ERR_POSTINGFAILED)
                            continue
                        self.article_lines.append(line)
                    else:
                        if command in self.commands:
                            getattr(self, "do_%s" % (command))()
                        else:
                            self.send_response(ERR_NOTCAPABLE)
        # /Handle
        settings.logEvent('Connection closed (IP Address: %s)' % (self.client_address[0]))

    def do_CAPABILITIES(self):
        msg = "101 Capability list:\r\n" + "VERSION 2\r\n" + "IMPLEMENTATION SGUG-PAPERCUT\r\n" + "LIST ACTIVE NEWSGROUPS OVERVIEW.FMT SUBSCRIPTIONS\r\n" + "OVER\r\n" + "READER\r\n" + "."
        self.send_response(msg)

    def do_NEWGROUPS(self):
        """
        Syntax:
            NEWGROUPS date time [GMT] [<distributions>]
        Responses:
            231 list of new newsgroups follows
        """
        if (len(self.tokens) < 3) or (len(self.tokens) > 5):
            self.send_response(ERR_CMDSYNTAXERROR)
            return
        if (len(self.tokens) > 3) and (self.tokens[3] == 'GMT'):
            ts = self.get_timestamp(self.tokens[1], self.tokens[2], 'yes')
        else:
            ts = self.get_timestamp(self.tokens[1], self.tokens[2], 'no')
        allgroups = None

        groups = self.backend.get_NEWGROUPS(ts)

        if groups is not None:
            allgroups += groups

        if allgroups == None:
            msg = "%s\r\n." % (STATUS_NEWGROUPS)
        else:
            msg = "%s\r\n%s\r\n." % (STATUS_NEWGROUPS, allgroups)
        self.send_response(msg)

    def do_GROUP(self):
        """
        Syntax:
            GROUP ggg
        Responses:
            211 n f l s group selected
               (n = estimated number of articles in group,
                f = first article number in the group,
                l = last article number in the group,
                s = name of the group.)
            411 no such news group
        """
        # check the syntax of the command
        if len(self.tokens) != 2:
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if not self.backend.group_exists(self.tokens[1]):
            self.send_response(ERR_NOSUCHGROUP)
            return
        
        self.selected_group = self.tokens[1]

        total_articles, first_article_id, last_article_id = self.backend.get_GROUP(self.tokens[1])

        self.send_response(STATUS_GROUPSELECTED % (total_articles, first_article_id, last_article_id, self.tokens[1]))


    def _backends_group_exists(self, group):
        '''
        Checks whether a group exists for a backend and returns that backend
        '''
        return self.backend

    def _multi_newnews(self, param, timestamp, group_backend=None):
        '''
        Calls newnews for all for group_backend if we already know where to
        look and for all of them otherwise.
        '''
        news = ''
        if group_backend:
            news = group_backend.get_NEWNEWS(timestamp, param)
        else:
            for backend in list(backends.values()):
                news += backend.get_NEWNEWS(timestamp, param)
        return news

    def do_NEWNEWS(self):
        """
        Syntax:
            NEWNEWS newsgroups date time [GMT] [<distribution>]
        Responses:
            230 list of new articles by message-id follows
        """
        # check the syntax of the command
        if (len(self.tokens) < 4) or (len(self.tokens) > 6):
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if (len(self.tokens) > 4) and (self.tokens[4] == 'GMT'):
            ts = self.get_timestamp(self.tokens[2], self.tokens[3], 'yes')
        else:
            ts = self.get_timestamp(self.tokens[2], self.tokens[3], 'no')
        
        newnews = ''
        news = self._multi_newnews(self.tokens[1], ts, self.backend)

        if len(news) == 0:
            msg = "%s\r\n." % (STATUS_NEWNEWS)
        else:
            msg = "%s\r\n%s\r\n." % (STATUS_NEWNEWS, news)
        self.send_response(msg)

    def do_LIST(self):
        """
        Syntax:
            LIST (done)
            LIST ACTIVE [wildmat]
            LIST ACTIVE.TIMES
            LIST DISTRIBUTIONS
            LIST DISTRIB.PATS
            LIST NEWSGROUPS [wildmat]
            LIST OVERVIEW.FMT (done)
            LIST SUBSCRIPTIONS
            LIST EXTENSIONS (not documented) (done by comparing the results of other servers)
        Responses:
            215 list of newsgroups follows
            503 program error, function not performed
        """
        if (len(self.tokens) == 2) and (self.tokens[1].upper() == 'OVERVIEW.FMT'):
            self.send_response("%s\r\n%s:\r\n." % (
                STATUS_OVERVIEWFMT, "\r\n".join(overview_headers)))
            return
        elif (len(self.tokens) == 2) and (self.tokens[1].upper() == 'EXTENSIONS'):
            self.send_response("%s\r\n%s\r\n." % (
                STATUS_EXTENSIONS, "\r\n".join(self.extensions)))
            return
        elif (len(self.tokens) > 1) and (self.tokens[1].upper() == 'ACTIVE'):
            lists = backend.get_LIST_ACTIVE(self.tokens[2])
            self.send_response("%s\r\n%s\r\n." % (STATUS_LIST, "\r\n".join(lists)))
            return
        elif (len(self.tokens) > 1) and (self.tokens[1].upper() == 'NEWSGROUPS'):
            self.do_LIST_NEWSGROUPS()
            return
        elif (len(self.tokens) > 1) and (self.tokens[1].upper() == 'SUBSCRIPTIONS'):
            self.do_LIST_NEWSGROUPS()
            return
        elif len(self.tokens) == 2:
            self.send_response(ERR_NOTPERFORMED)
            return
        
        groups = self.backend.get_LIST()

        msg = [STATUS_LIST] # First line is status
        
        if settings.server_type == 'read-only':
            # add n if posting is forbidden
            for g in groups:
                msg.append("%s n" % (g))
        else:
            # add y if posting is authorized
            for g in groups:
                msg.append("%s y" % (g))

        msg.append('.') # Add ending point

        self.send_array(msg)

    def do_STAT(self):
        """
        Syntax:
            STAT [nnn|<message-id>]
        Responses:
            223 n a article retrieved - request text separately
               (n = article number, a = unique article id)
            412 no newsgroup selected
            420 no current article has been selected
            421 no next article in this group
        """

        backend = self.backend
        article_info = []  # Holds group/article ID of article

        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if ((len(self.tokens) == 1) and (self.selected_article == 'ggg')):
            self.send_response(ERR_NOARTICLESELECTED)
            return

        if len(self.tokens) == 2 and self.tokens[1].find('<') != -1:
            # Message ID specified
            self.tokens[1] = self.get_number_from_msg_id(self.tokens[1], b)
            result = backend.get_STAT(self.selected_group, self.tokens[1])
            if result:
                article_info = backend.get_article_number(self.tokens[1])
        else:
            # Article Number specified or using article number from article
            # pointer
            if len(self.tokens) == 2:
                # Set article pointer if a number was specified
                self.selected_article = self.tokens[1]
            article_info = [self.selected_group, self.selected_article]
            result = backend.get_STAT(self.selected_group, self.selected_article)

        if result == None:
            self.send_response(ERR_NOSUCHARTICLENUM)
        else:
            self.send_response(STATUS_STAT % (article_info[0], backend.get_message_id(article_info[1], article_info[0])))

    def do_ARTICLE(self):
        """
        Syntax:
            ARTICLE nnn|<message-id>
        Responses:
            220 n <a> article retrieved - head and body follow
                (n = article number, <a> = message-id)
            221 n <a> article retrieved - head follows
            222 n <a> article retrieved - body follows
            223 n <a> article retrieved - request text separately
            412 no newsgroup has been selected
            420 no current article has been selected
            423 no such article number in this group
            430 no such article found
        """

        backend = self.backend
        article_info = []  # Holds group/article ID of article

        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if ((len(self.tokens) == 1) and (self.selected_article == 'ggg')):
            self.send_response(ERR_NOARTICLESELECTED)
            return

        if len(self.tokens) == 2 and self.tokens[1].find('<') != -1:
            # Message ID specified
            result = backend.get_article_by_message_id(self.selected_group, self.tokens[1])
            if result:
                article_info = (0, self.tokens[1])
        else:
            # Article Number specified or using article number from article
            # pointer
            if len(self.tokens) == 2:
                # Set article pointer if a number was specified
                self.selected_article = self.tokens[1]
            article_info = [self.selected_group, self.selected_article]
            result = backend.get_ARTICLE(self.selected_group, self.selected_article)

        if result == None:
            self.send_response(ERR_NOSUCHARTICLENUM)
        else:
            if self.tokens[1][0] == '<':
                response = STATUS_ARTICLE % (0, self.tokens[1])
            else:
                response = STATUS_ARTICLE % (article_info[0], backend.get_message_id(article_info[1], article_info[0]))
            self.send_response("%s\r\n%s\r\n\r\n%s\r\n." % (response, result[0], result[1]))

    def do_LAST(self):
        """
        Syntax:
            LAST
        Responses:
            223 n a article retrieved - request text separately
               (n = article number, a = unique article id)
        """
        # check if there is a previous article
        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if self.selected_article == 'ggg':
            self.send_response(ERR_NOARTICLESELECTED)
            return

        backend = self.backend
        article_num = backend.get_LAST(self.selected_group, self.selected_article)
        if article_num == None:
            self.send_response(ERR_NOPREVIOUSARTICLE)
            return
        self.selected_article = article_num
        self.send_response(STATUS_STAT % (
            article_num, backend.get_message_id(article_num, self.selected_group)))

    def do_NEXT(self):
        """
        Syntax:
            NEXT
        Responses:
            223 n a article retrieved - request text separately
               (n = article number, a = unique article id)
            412 no newsgroup selected
            420 no current article has been selected
            421 no next article in this group
        """
        # check if there is a previous article
        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return
        backend = self.backend
        if self.selected_article == 'ggg':
            article_num = backend.get_first_article(self.selected_group)
        else:
            article_num = backend.get_NEXT(self.selected_group, self.selected_article)
        if article_num == None:
            self.send_response(ERR_NONEXTARTICLE)
            return
        self.selected_article = article_num
        self.send_response(STATUS_STAT % (article_num, backend.get_message_id(article_num, self.selected_group)))

    def do_BODY(self):
        """
        Syntax:
            BODY [nnn|<message-id>]
        Responses:
            222 10110 <23445@sdcsvax.ARPA> article retrieved - body follows (body text here)
        """

        backend = self.backend
        article_info = []  # Holds group/article ID of article

        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if ((len(self.tokens) == 1) and (self.selected_article == 'ggg')):
            self.send_response(ERR_NOARTICLESELECTED)
            return

        if len(self.tokens) == 2 and self.tokens[1].find('<') != -1:
            # Message ID specified
            body = backend.get_BODY(self.selected_group, self.tokens[1])
            if body:
                article_info = (0, self.tokens[1])
        else:
            # Article Number specified or using article number from article
            # pointer
            if len(self.tokens) == 2:
                # Set article pointer if a number was specified
                self.selected_article = self.tokens[1]

            article_info = [self.selected_group, self.selected_article]
            body = backend.get_BODY(self.selected_group, self.selected_article)

        if body == None:
            self.send_response(ERR_NOSUCHARTICLENUM)
        else:
            if self.tokens[1][0] == '<':
                self.send_response("%s\r\n%s\r\n." % (STATUS_HEAD % ('0', self.tokens[1]), body))
            else:
                self.send_response("%s\r\n%s\r\n." % (STATUS_BODY % (article_info[1], backend.get_message_id(article_info[1], article_info[0])), body))

    def do_HEAD(self):
        """
        Syntax:
            HEAD [nnn|<message-id>]
        Responses:
            221 1013 <5734@mcvax.UUCP> Article retrieved; head follows.
        """

        backend = self.backend
        article_info = []  # Holds group/article ID of article

        if self.selected_group == 'ggg' and self.tokens[1].find('<') == -1:
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if ((len(self.tokens) == 1) and (self.selected_article == 'ggg')):
            self.send_response(ERR_NOARTICLESELECTED)
            return

        if len(self.tokens) == 2 and self.tokens[1].find('<') != -1:
            # Message ID specified
            body = backend.get_HEAD(self.selected_group, self.tokens[1])
            if body:
                article_info = (0, self.tokens[1])
        else:
            # Article Number specified or using article number from article
            # pointer
            if len(self.tokens) == 2:
                # Set article pointer if a number was specified
                self.selected_article = self.tokens[1]

            article_info = [self.selected_group, self.selected_article]
            body = backend.get_HEAD(self.selected_group, self.selected_article)

        if body == None:
            self.send_response(ERR_NOSUCHARTICLENUM)
        else:
            # self.send_response("%s\r\n%s\r\n." % (STATUS_BODY % (article_info[0], backend.get_message_id(article_info[1], article_info[0])), body))
            if self.tokens[1][0] == '<':
                self.send_response("%s\r\n%s\r\n." %
                                   (STATUS_HEAD % ('0', self.tokens[1]), body))
            else:
                self.send_response("%s\r\n%s\r\n." % (STATUS_HEAD % (
                    article_info[1], backend.get_message_id(article_info[1], article_info[0])), body))

    def do_OVER(self):
        self.do_XOVER()

    def do_XOVER(self):
        """
        Syntax:
            XOVER [range]
        Responses:
            224 Overview information follows
            subject author date message-id references byte count line count
            412 No news group current selected
            420 No article(s) selected
        """
        backend = self.backend

        # Group is not defined. Send error message no group selected
        if self.selected_group == 'ggg' or self.selected_group == '':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        # check the command style
        if len(self.tokens) == 1:
            # only show the information for the current selected article
            if self.selected_article == 'ggg':
                overviews = backend.get_XOVER(self.selected_group, 1, 1)
            else:
                overviews = backend.get_XOVER(
                    self.selected_group, self.selected_article, self.selected_article)
        else:
            if self.tokens[1].find('-') == -1:
                overviews = backend.get_XOVER(
                    self.selected_group, self.tokens[1], self.tokens[1])
            else:
                ranges = self.tokens[1].split('-')
                if ranges[1] == '':
                    # this is a start-everything style of XOVER
                    overviews = backend.get_XOVER(self.selected_group, ranges[0])
                else:
                    # this is a start-end style of XOVER
                    overviews = backend.get_XOVER(self.selected_group, ranges[0], ranges[1])

        if overviews == None:
            self.send_response(ERR_NOTCAPABLE)
            return
        if len(overviews) == 0:
            msg = "%s\r\n." % (STATUS_XOVER)
        else:
            msg = "%s\r\n%s\r\n." % (STATUS_XOVER, overviews)
        self.send_response(msg)

    def do_XPAT(self):
        """
        Syntax:
            XPAT header range|<message-id> pat [pat...]
        Responses:
            221 Header follows
            430 no such article
            502 no permission
        """
        if len(self.tokens) < 4:
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if not self.index_in_list(overview_headers, self.tokens[1]):
            self.send_response("%s\r\n." % (STATUS_XPAT))
            return

        if self.tokens[2].find('@') != -1:
            self.tokens[2] = self.get_number_from_msg_id(self.tokens[2])
            self.do_XHDR()
            return
        else:
            ranges = self.tokens[2].split('-')
            if ranges[1] == '':
                overviews = self.backend.get_XPAT(self.selected_group, self.tokens[1], self.tokens[3], ranges[0])
            else:
                overviews = self.backend.get_XPAT(self.selected_group, self.tokens[1], self.tokens[3], ranges[0], ranges[1])
        if overviews == None:
            self.send_response(ERR_NOTCAPABLE)
            return
        self.send_response("%s\r\n%s\r\n." % (STATUS_XPAT, overviews))

    def do_LISTGROUP(self):
        """
        Syntax:
            LISTGROUP [ggg]
        Responses:
            211 list of article numbers follow
            411 No such group
            412 Not currently in newsgroup
            502 no permission
        """
        backend = self.backend

        if len(self.tokens) > 2:
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if len(self.tokens) == 2:
            # check if the group exists
            if not backend.group_exists(self.tokens[1]):
                # the draft of the new NNTP protocol tell us to reply this instead of an empty list
                self.send_response(ERR_NOSUCHGROUP)
                return
            try:
                numbers = backend.get_LISTGROUP(self.tokens[1])
            # TODO: Introduce a dedicated exception for this kind of thing -
            # depending on the plugin this might be a ENOENT or a database
            # exception.
            except KeyError:
                self.send_response(ERR_NOSUCHGROUP)
                return
        else:
            if self.selected_group == 'ggg':
                self.send_response(ERR_NOGROUPSELECTED)
                return
            try:
                numbers = backend.get_LISTGROUP(self.selected_group)
            # TODO: Introduce a dedicated exception for this kind of thing -
            # depending on the plugin this might be a ENOENT or a database
            # exception.
            except KeyError:
                self.send_response(ERR_NOSUCHGROUP)
                return

        check = numbers.split('\r\n')

        if len(check) > 0:
            # When a valid group is selected by means of this command, the
            # internally maintained "current article pointer" is set to the first
            # article in the group.
            self.selected_article = check[0]
            if len(self.tokens) == 2:
                self.selected_group = self.tokens[1]
        else:
            # If an empty newsgroup is selected, the current article pointer is made invalid.
            self.selected_article = 'ggg'

        self.send_response("%s\r\n%s\r\n." % (STATUS_LISTGROUP % (backend.get_group_stats(self.selected_group)), numbers))

    def do_XGTITLE(self):
        """
        Syntax:
            XGTITLE [wildmat]
        Responses:
            481 Groups and descriptions unavailable
            282 list of groups and descriptions follows
        """
        backend = self.backend

        if len(self.tokens) > 2:
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if len(self.tokens) == 2:
            info = backend.get_XGTITLE(self.tokens[1])
        else:
            if self.selected_group == 'ggg':
                self.send_response(ERR_NOGROUPSELECTED)
                return
            info = backend.get_XGTITLE(self.selected_group)
        if info is None:
            self.send_response(ERR_NODESCAVAILABLE)
        elif len(info) == 0:
            self.send_response("%s\r\n." % (STATUS_XGTITLE))
        else:
            self.send_response("%s\r\n%s\r\n." % (STATUS_XGTITLE, info))

    def do_LIST_NEWSGROUPS(self):
        """
        Syntax:
            LIST NEWSGROUPS [wildmat]
        Responses:
            503 program error, function not performed
            215 list of groups and descriptions follows
        """
        if len(self.tokens) > 3:
            self.send_response(ERR_CMDSYNTAXERROR)
            return
        info = ''
        if len(self.tokens) == 3:
            info = backend.get_XGTITLE(self.tokens[2])
        else:
            info = backend.get_XGTITLE()
        self.send_response("%s\r\n%s\r\n." % (STATUS_LISTNEWSGROUPS, info))

    def do_HDR(self):
        self.do_XHDR()

    def do_XROVER(self):
        self.tokens[1] = 'REFERENCES'
        self.do_XHDR()

    def do_XHDR(self):
        """
        Syntax:
            XHDR header [range|<message-id>]
        Responses:
            221 Header follows
            412 No news group current selected
            420 No current article selected
            430 no such article
        """
        backend = self.backend

        if (len(self.tokens) < 2) or (len(self.tokens) > 3):
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if self.selected_group == 'ggg':
            self.send_response(ERR_NOGROUPSELECTED)
            return

        if (self.tokens[1].upper() != 'SUBJECT') and (self.tokens[1].upper() != 'FROM'):
            self.send_response(ERR_CMDSYNTAXERROR)
            return

        if len(self.tokens) == 2:
            if self.selected_article == 'ggg':
                self.send_response(ERR_NOARTICLESELECTED)
                return
            info = backend.get_XHDR(self.selected_group, self.tokens[1], 'unique', (self.selected_article))
        else:
            # check the XHDR style now
            if self.tokens[2].find('@') != -1:
                self.tokens[2] = self.get_number_from_msg_id(
                    self.tokens[2], b)
                info = backend.get_XHDR(self.selected_group, self.tokens[1], 'unique', (self.tokens[2]))
            else:
                ranges = self.tokens[2].split('-')
                if ranges[1] == '':
                    info = backend.get_XHDR(
                        self.selected_group, self.tokens[1], 'range', (ranges[0]))
                else:
                    info = backend.get_XHDR(
                        self.selected_group, self.tokens[1], 'range', (ranges[0], ranges[1]))
        # check for empty results
        if info == None:
            self.send_response(ERR_NOTCAPABLE)
        else:
            self.send_response("%s\r\n%s\r\n." % (STATUS_XHDR, info))

    def do_DATE(self):
        """
        Syntax:
            DATE
        Responses:
            111 YYYYMMDDhhmmss
        """
        self.send_response(STATUS_DATE % (time.strftime(
            '%Y%m%d%H%M%S', time.localtime(time.time()))))

    def do_HELP(self):
        """
        Syntax:
            HELP
        Responses:
            100 help text follows
        """
        self.send_response("%s\r\n\t%s\r\n." %
                           (STATUS_HELPMSG, "\r\n\t".join(self.commands)))

    def do_QUIT(self):
        """
        Syntax:
            QUIT
        Responses:
            205 closing connection - goodbye!
        """
        self.terminated = 1
        self.send_response(STATUS_CLOSING)

    def do_IHAVE(self):
        """
        Syntax:
            IHAVE <message-id>
        Responses:
            235 article transferred ok
            335 send article to be transferred.  End with <CR-LF>.<CR-LF>
            435 article not wanted - do not send it
            436 transfer failed - try again later
            437 article rejected - do not try again
        """
        if (len(self.tokens) != 2) or (self.tokens[1].find('<') == -1):
            self.send_response(ERR_CMDSYNTAXERROR)
            return
        self.send_response(ERR_NOIHAVEHERE)

    def do_SLAVE(self):
        """
        Syntax:
            SLAVE
        Responses:
            202 slave status noted
        """
        self.send_response(STATUS_SLAVE)

    def do_MODE(self):
        """
        Syntax:
            MODE READER|STREAM
        Responses:
            200 Hello, you can post
            201 Hello, you can't post
            203 Streaming is OK
            500 Command not understood
        """
        if self.tokens[1].upper() == 'READER':
            if settings.server_type == 'read-only':
                self.send_response(STATUS_NOPOSTMODE)
            else:
                self.send_response(STATUS_POSTMODE)
        elif self.tokens[1].upper() == 'STREAM':
            self.send_response(ERR_NOSTREAM)

    def do_Post(self):
        """
        Syntax:
            POST
        Responses:
            240 article posted ok
            340 send article to be posted. End with <CR-LF>.<CR-LF>
            440 posting not allowed
            441 posting failed
        """
        # Get backend from class
        backend = self.backend
        # Define the success of posting
        success = False
        # Join all lines to a single string without seperator
        lines = ''.join(self.article_lines)
        # Generate an email object from client string
        msg = email.message_from_string(lines)
        # Get Newsgroup to post in from mail header
        group_name = msg.get('Newsgroups')
        # UUID objects generated and then splitted
        uid1 = uuid.uuid1()
        uid2 = uuid.uuid1()
        # Generate ID of Message-ID 
        unique_id = '{}${}$'.format(str(uid1)[:5], str(uid2)[:2])
        # Generate Message-ID for storage and article
        message_id = '<{}@{}>'.format(unique_id, settings.domain_name) 
        # Generate or get author from mail
        if settings.nntp_auth == 'yes':
            # Generate author
            author = "{} {} <{}@{}>".format(self.auth_first_name, self.auth_surname, self.auth_username, settings.domain_name)
        else:
            # Get author from email client
            author = msg.get('From')
        
        # Get subject from email header
        subject = msg.get('Subject')
        # Check if plurals References strings are presents
        if len(str(msg.get('References')).split(' ')) > 1:
            # Get last references from email header
            references = str(msg.get('References')).split(' ').pop()
        else:
            # Only one References, so use it.
            references = msg.get('References') 

        pprint(str(msg.get('References')).split(' '))
        pprint(str(msg.get('References')).split(' ').pop())
        # Get paybload (body) from email
        payload = msg.get_payload()

        # Require at least Newsgroups, From and Subject variables to post
        if ((not group_name) or (not author) or (not subject)):
            self.send_response(ERR_POSTINGFAILED)
            return

        # Check if group exists in backend
        if not self._backends_group_exists(group_name):
            self.send_response(ERR_NOSUCHGROUP)
            return

        success = backend.do_Post(group_name, self.client_address[0], author, message_id, subject, references, payload, self.auth_username,)
        # success = backend.do_Post(msg, group_name, self.client_address[0], message_id, self.auth_username)
    
        # pprint(backend.do_Post(msg, groupName, self.client_address[0], self.auth_username))

        if success == False:
            self.send_response(ERR_POSTINGFAILED)
        else:
            self.send_response(STATUS_POSTSUCCESSFULL)

    def do_AUTHINFO(self):
        """
        Syntax:
            AUTHINFO USER username
            AUTHINFO PASS password
        Responses:
            281 Authentication accepted
            381 More authentication information required
            480 Authentication required
            482 Authentication rejected
            502 No permission
        """
        if len(self.tokens) != 3:
            self.send_response(ERR_CMDSYNTAXERROR)
            return
        if settings.nntp_auth == 'no':
            self.send_response(STATUS_AUTH_ACCEPTED)
            return
        if self.tokens[1].upper() == 'USER':
            self.auth_username = self.tokens[2]
            self.send_response(STATUS_AUTH_CONTINUE)
        elif self.tokens[1].upper() == 'PASS' and settings.nntp_auth == 'yes':
            if auth.is_valid_user(self.auth_username, self.tokens[2]):
                self.send_response(STATUS_AUTH_ACCEPTED)
            else:
                self.send_response(ERR_AUTH_NO_PERMISSION)
                self.auth_username = ''

    def do_XVERSION(self):
        self.send_response(STATUS_SERVER_VERSION)

    def get_number_from_msg_id(self, msg_id, backend):
        '''
        Mangles the message ID by extracting just the local part for backend
        plugins that cannot handle regular message IDs. No action for plugins
        that can deal with regular message IDs.
        '''

        try:
            if backend.__class__.capabilities['message-id']:
                return msg_id
        except (AttributeError, KeyError):
            return msg_id[1:msg_id.find('@')]

    def index_in_list(self, list, index):
        for item in list:
            if item.upper() == index.upper():
                return 1
        return 0

    def get_timestamp(self, date, times, gmt='yes'):
        # like the new NNTP draft explains...
        if len(date) == 8:
            year = date[:4]
        else:
            local_year = str(time.localtime()[0])
            if date[:2] > local_year[2:4]:
                year = "19%s" % (date[:2])
            else:
                year = "20%s" % (date[:2])
        ts = time.mktime((int(year), int(date[2:4]), int(date[4:6]), int(
            times[:2]), int(times[2:4]), int(times[4:6]), 0, 0, 0))
        if gmt == 'yes':
            return ts
        else:
            return ts + time.timezone

    def send_response(self, message):
        if __DEBUG__:
            print("server>", message)
        self.wfile.write(bytes(message + "\r\n", 'utf-8', 'replace'))
        self.wfile.flush()

    def send_array(self, msg = []):
        '''
        Send response to client but take an array in input
        Add \r\n at each line end
        '''

        for m in msg:
            self.wfile.write(bytes(m + '\r\n', 'utf-8', 'replace'))
            if __DEBUG__:
                print("[>]", m)
        self.wfile.flush()


    def finish(self):
        # cleaning up after ourselves
        self.terminated = 0
        self.selected_article = 'ggg'
        self.selected_group = 'ggg'
        self.tokens = []
        self.sending_article = 0
        self.auth_username = ''
        self.article_lines = []
        self.wfile.flush()
        self.wfile.close()
        self.rfile.close()
        if __DEBUG__:
            print('Closing the request')


def main():
    # set up signal handler
    def sighandler(signum, frame):
        if __DEBUG__:
            print("\nShutting down papercut...")
        server.socket.close()
        # time.sleep(1)
        sys.exit(0)

    signal.signal(signal.SIGINT, sighandler)

    if settings.storage_backend:
        print('Papercut %s (global storage module %s) - starting up' % (__VERSION__, settings.storage_backend))
        server = NNTPServer((settings.nntp_hostname, settings.nntp_port), NNTPRequestHandler)
        server.serve_forever()
    else:
        print('Papercut %s (no global storage module found)' % __VERSION__)
        print('Exiting...')
        sys.exit(1)
