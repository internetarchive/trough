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
        self.pretty_print = True
        self.show_segment_in_result = False
        self._segment_cache = None
        self.update_prompt()

    def table(self, dictlist):
        s = ''
        # calculate lengths for each column
        lengths = [max(list(map(lambda x:len(str(x.get(k))), dictlist)) + [len(str(k))]) for k in dictlist[0].keys()]
        # compose a formatter-string
        lenstr = "| "+" | ".join("{:<%s}" % m for m in lengths) + " |\n"
        # print header and borders
        border = "+" + "+".join(["-" * (l + 2) for l in lengths]) + "+\n"
        s += border
        header = lenstr.format(*dictlist[0].keys())
        s += header
        s += border
        # print rows and borders
        for item in dictlist:
            formatted = lenstr.format(*[str(value) for value in item.values()])
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
            elif self.pretty_print:
                n_rows = 0
                result = list(result)
                print(self.table(result), end='', file=out)
                return len(result)
            else:
                print(result, file=out)
                return len(result)
        except BrokenPipeError:
            pass  # user quit the pager

    def update_prompt(self):
        self.prompt = 'trough:%s(%s)> ' % (
                self.segments[0] if len(self.segments) == 1
                else '[%s segments]' % len(self.segments),
                'rw' if self.writable else 'ro')

    def do_show(self, argument):
        '''SHOW command, like MySQL. Available subcommands:
        - SHOW TABLES
        - SHOW CREATE TABLE
        - SHOW CONNECTIONS
        - SHOW SCHEMA schema-name
        - SHOW SCHEMAS
        - SHOW SEGMENTS [MATCHING 'regexp']'''
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
            self.display([{'connection': segment } for segment in self.segments])
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
                print("%s results in %s" % (n_rows, end - start))
            except Exception as e:
                self.logger.error(e, exc_info=True)
        else:
            self.do_help('show')

    def do_connect(self, argument):
        '''Connect to one or more trough "segments" (sqlite databases)'''
        segment_re = re.sub(' +', '|', argument)
        seg_urls = self.cli.read_urls_for_regex(segment_re)
        self.segments = seg_urls.keys()
        self.show_segment_in_result = len(self.segments) > 1
        self.update_prompt()

    def do_pretty(self, ignore):
        '''Toggle pretty-printed results'''
        self.pretty_print = not self.pretty_print
        print('pretty print %s' % ("on" if self.pretty_print else "off"))

    async def async_select(self, segment, query):
        result = await self.cli.async_read(segment, query)
        print('+++++ results from segment %s +++++' % segment,
              file=self.pager_pipe or sys.stdout)
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
        query = 'select ' + line
        with self.pager():
            try:
                self.n_rows = 0
                loop = asyncio.get_event_loop()
                future = asyncio.ensure_future(self.async_fanout(query))
                loop.run_until_complete(future)
                # XXX not sure how to measure time not including user time
                # scrolling around in `less`
                print('%s results' % self.n_rows, file=self.pager_pipe)
            except Exception as e:
                self.logger.error(e, exc_info=True)

    @contextmanager
    def pager(self):
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

    def default(self, line):
        if line == 'EOF':
            print()
            return True

        keyword_args = line.strip().split(maxsplit=2)
        if len(keyword_args) == 1:
            keyword, args = keyword_args[0], ''
        else:
            keyword, args = keyword_args[0], keyword_args[1]

        if getattr(self, 'do_' + keyword.lower(), None):
            getattr(self, 'do_' + keyword.lower())(args)
        elif self.writable:
            self.cli.write(self.segment_id, line, schema_id=self.schema_id)
        else:
            self.logger.error(
                    'refusing to execute arbitrary sql (in read-only mode)')

    def do_quit(self, args):
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
    arg_parser.add_argument('segment')
    args = arg_parser.parse_args(args=argv[1:])

    logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG if args.verbose else logging.WARN, format=(
                '%(asctime)s %(levelname)s %(name)s.%(funcName)s'
                '(%(filename)s:%(lineno)d) %(message)s'))

    cli = trough.client.TroughClient(args.rethinkdb_trough_db_url)
    shell = TroughRepl(cli, [args.segment], args.writable, args.schema)

    if os.path.exists(HISTORY_FILE):
        readline.read_history_file(HISTORY_FILE)

    try:
        shell.cmdloop()
    finally:
        readline.write_history_file(HISTORY_FILE)

