import trough.client
import sys
import argparse
import os
import cmd
import logging
import readline
import datetime
import re
from aiohttp import ClientSession
import asyncio
from contextlib import contextmanager
import subprocess
import io
import json

HISTORY_FILE = os.path.expanduser('~/.trough_history')

class BetterArgumentDefaultsHelpFormatter(
                argparse.ArgumentDefaultsHelpFormatter,
                argparse.RawDescriptionHelpFormatter):
    '''
    HelpFormatter with these properties:

    - formats option help like argparse.ArgumentDefaultsHelpFormatter except
      that it omits the default value for arguments with action='store_const'
    - like argparse.RawDescriptionHelpFormatter, does not reformat description
      string
    '''
    def _get_help_string(self, action):
        if isinstance(action, argparse._StoreConstAction):
            return action.help
        else:
            return argparse.ArgumentDefaultsHelpFormatter._get_help_string(self, action)

class TroughRepl(cmd.Cmd):
    intro = 'Welcome to the trough shell. Type help or ? to list commands.\n'
    logger = logging.getLogger('trough.client.TroughRepl')

    def __init__(
            self, trough_client, segments, writable=False,
            schema_id='default'):
        super().__init__()
        self.cli = trough_client
        self.segments = segments
        self.writable = writable
        self.schema_id = schema_id
        self.format = 'table'
        self.pager_pipe = None
        self.update_prompt()

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except Exception as e:
            self.logger.error('caught exception', exc_info=True)

    def table(self, dictlist):
        assert dictlist
        s = ''
        # calculate lengths for each column
        max_lengths = {}
        for row in dictlist:
            for k, v in row.items():
                max_lengths[k] = max(
                        max_lengths.get(k, 0), len(k),
                        len(str(v) if v is not None else '<null>'))

        if not self.column_keys:
            column_keys = list(dictlist[0].keys())
            # column order: id first, then shortest column, next biggest, etc
            # with column name alphabetical as tiebreaker
            column_keys.sort(key=lambda k: (0, '!') if k == 'id' \
                                               else (max_lengths[k], k))
            self.column_keys = column_keys

        # compose a formatter-string
        lenstr = "| "+" | ".join("{:<%s}" % max_lengths[k] for k in self.column_keys) + " |\n"
        # print header and borders
        border = "+" + "+".join(["-" * (max_lengths[k] + 2) for k in self.column_keys]) + "+\n"
        s += border
        header = lenstr.format(*self.column_keys)
        s += header
        s += border
        # print rows and borders
        for row in dictlist:
            formatted = lenstr.format(*[
                str(row[k]) if row[k] is not None else '<null>'
                for k in self.column_keys])
            s += formatted
        s += border
        return s

    def display(self, result):
        if self.pager_pipe:
            out = self.pager_pipe
        else:
            out = sys.stdout

        try:
            if not result:
                print('<no results>', file=out)
                return 0
            elif self.format == 'table':
                n_rows = 0
                result = list(result)
                print(self.table(result), end='', file=out)
                return len(result)
            elif self.format == 'pretty':
               print(json.dumps(result, indent=2), file=out)
               return len(result)
            else:
                print(json.dumps(result), file=out)
                return len(result)
        except BrokenPipeError:
            pass  # user quit the pager

    def update_prompt(self):
        if not self.segments:
            self.prompt = 'trough> '
        elif len(self.segments) == 1:
            self.prompt = 'trough:%s(%s)> ' % (
                    self.segments[0], 'rw' if self.writable else 'ro')
        else:
            self.prompt = 'trough:[%s segments](%s)> ' % (
                    len(self.segments), 'rw' if self.writable else 'ro')

    def do_show(self, argument):
        '''
        SHOW command, like MySQL. Available subcommands:
        - SHOW TABLES
        - SHOW CREATE TABLE
        - SHOW CONNECTIONS
        - SHOW SCHEMA schema-name
        - SHOW SCHEMAS
        - SHOW SEGMENTS
        - SHOW SEGMENTS MATCHING <regex>
        '''
        with self.pager():
            argument = argument.replace(";", "").lower()
            if argument[:6] == 'tables':
                self.do_select("name from sqlite_master where type = 'table';")
            elif argument[:12] == 'create table':
                self.do_select(
                        "sql from sqlite_master where type = 'table' "
                        "and name = '%s';" % argument[12:].replace(';', '').strip())
            elif argument[:7] == 'schemas':
                result = self.cli.schemas()
                self.display(result)
            elif argument[:11] == 'connections':
                connections = []
                for segment in sorted(self.segments):
                    conn = {'segment_id': segment}
                    if self.writable:
                        try:
                            conn['write_url'] = self.cli.write_url(segment)
                        except:
                            conn['write_url'] = None
                    try:
                        conn['read_url'] = self.cli.read_url(segment)
                    except:
                        conn['read_url'] = None
                    connections.append(conn)
                self.display(connections)
            elif argument[:7] == 'schema ':
                name = argument[7:].strip()
                result = self.cli.schema(name)
                self.display(result)
            elif argument[:8] == 'segments':
                regex = None
                if "matching" in argument:
                    regex = argument.split("matching")[-1].strip().strip('"').strip("'")
                try:
                    start = datetime.datetime.now()
                    result = self.cli.readable_segments(regex=regex)
                    end = datetime.datetime.now()
                    n_rows = self.display(result)
                    print("%s results" % n_rows, file=self.pager_pipe)
                except Exception as e:
                    self.logger.error(e, exc_info=True)
            else:
                self.do_help('show')

    def do_connect(self, argument):
        '''
        Connect to one or more trough "segments" (sqlite databases).
        Usage:

        - CONNECT segment [segment...]
        - CONNECT MATCHING <regex>

        See also SHOW CONNECTIONS
        '''
        argument = re.sub(r';+$', '', argument.strip().lower())
        if not argument:
            self.do_help('connect')
            return

        if argument[:8] == 'matching':
            seg_urls = self.cli.read_urls_for_regex(argument[8:].lstrip())
            self.segments = seg_urls.keys()
        else:
            self.segments = argument.split()
        self.update_prompt()

    def do_format(self, raw_arg):
        '''
        Set result output display format. Options:

        - FORMAT TABLE   - tabular format (the default)
        - FORMAT PRETTY  - pretty-printed json
        - FORMAT RAW     - raw json

        With no argument, displays current output format.
        '''
        arg = raw_arg.strip().lower()
        if not arg:
            print('Format is %r' % self.format)
        elif arg in ('table', 'pretty', 'raw'):
            self.format = arg
            print('Format is now %r' % self.format)
        else:
            self.do_help('format')

    async def async_select(self, segment, query):
        result = await self.cli.async_read(segment, query)
        try:
            print('+++++ results from segment %s +++++' % segment,
                  file=self.pager_pipe or sys.stdout)
        except BrokenPipeError:
            pass
        return self.display(result) # returns number of rows

    async def async_fanout(self, query):
        tasks = []
        for segment in self.segments:
            task = asyncio.ensure_future(self.async_select(segment, query))
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                try:
                    raise result
                except:
                    self.logger.warning(
                            'async_fanout results[%r] is an exception:',
                            i, exc_info=True)
            elif result:
                self.n_rows += result

    def do_select(self, line):
        '''Send a query to the currently-connected trough segment.

        Syntax: select...

        Example: Send query "select * from host_statistics;" to server
        trough> query select * from host_statistics;
        '''
        if not self.segments:
            print('not connected to any segments')
            return

        query = 'select ' + line
        with self.pager():
            try:
                self.n_rows = 0
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(self.async_fanout(query))
                loop.run_until_complete(future)
                # XXX not sure how to measure time not including user time
                # scrolling around in `less`
                print('%s total results' % self.n_rows, file=self.pager_pipe)
            except Exception as e:
                self.logger.error(e, exc_info=True)

    @contextmanager
    def pager(self):
        if self.pager_pipe:
            # reentrancy!
            yield
            return

        self.column_keys = None
        cmd = os.environ.get('PAGER') or '/usr/bin/less -nFSX'
        try:
            with subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE) as proc:
                with io.TextIOWrapper(
                        proc.stdin, errors='backslashreplace') as self.pager_pipe:
                    yield
                proc.wait()
        except BrokenPipeError:
            pass # user quit the pager
        self.pager_pipe = None

    def emptyline(self):
        pass

    def do_promote(self, args):
        '''
        Promote connected segments to permanent storage in hdfs.

        Takes no arguments. Only supported in read-write mode.
        '''
        if args.strip():
            self.do_help('promote')
            return
        if not self.segments:
            print('not connected to any segments')
            return
        if not self.writable:
            print('promoting segments not supported in read-only mode')
            return
        for segment in self.segments:
            self.cli.promote(segment)

    def default(self, line):
        keyword_args = line.strip().split(maxsplit=2)
        if len(keyword_args) == 1:
            keyword, args = keyword_args[0], ''
        else:
            keyword, args = keyword_args[0], keyword_args[1]

        if getattr(self, 'do_' + keyword.lower(), None):
            getattr(self, 'do_' + keyword.lower())(args)
        elif self.writable:
            if len(self.segments) == 1:
                self.cli.write(self.segments[0], line, schema_id=self.schema_id)
            elif not self.segments:
                print('not connected to any segments')
            elif len(self.segments) > 1:
                print('writing to multiple segments not supported')
        else:
            self.logger.error(
                    'invalid command %r, and refusing to execute arbitrary '
                    'sql (in read-only mode)', keyword)

    def do_quit(self, args):
        '''Exit the trough shell.'''
        if not args:
            print('bye!')
            return True
    do_EOF = do_quit
    do_exit = do_quit
    do_bye = do_quit

def trough_client(argv=None):
    argv = argv or sys.argv
    arg_parser = argparse.ArgumentParser(
            prog=os.path.basename(argv[0]),
            formatter_class=BetterArgumentDefaultsHelpFormatter)
    arg_parser.add_argument(
            '-u', '--rethinkdb-trough-db-url',
            default='rethinkdb://localhost/trough_configuration')
    arg_parser.add_argument('-w', '--writable', action='store_true')
    arg_parser.add_argument('-v', '--verbose', action='store_true')
    arg_parser.add_argument(
            '-s', '--schema', default='default',
            help='schema id for new segment')
    arg_parser.add_argument('segment', nargs='*')
    args = arg_parser.parse_args(args=argv[1:])

    logging.root.handlers = []
    logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG if args.verbose else logging.INFO, format=(
                '%(asctime)s %(levelname)s %(name)s.%(funcName)s'
                '(%(filename)s:%(lineno)d) %(message)s'))
    logging.getLogger('requests.packages.urllib3').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    cli = trough.client.TroughClient(args.rethinkdb_trough_db_url)
    shell = TroughRepl(cli, args.segment, args.writable, args.schema)

    if os.path.exists(HISTORY_FILE):
        readline.read_history_file(HISTORY_FILE)

    try:
        shell.cmdloop()
    finally:
        readline.write_history_file(HISTORY_FILE)

