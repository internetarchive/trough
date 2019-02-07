import trough.client
import sys
import argparse
import os
import cmd
import logging
import readline
from prettytable import PrettyTable
import datetime
import pydoc

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
            self, trough_client, segment_id, writable=False,
            schema_id='default'):
        super().__init__()
        self.cli = trough_client
        self.segment_id = segment_id
        self.writable = writable
        self.schema_id = schema_id
        self.pretty_print = True

        self.prompt = 'trough:%s(%s)> ' % (
                segment_id, 'rw' if writable else 'ro')

    def do_show(self, argument):
        '''SHOW command, like MySQL. Available subcommands:
        - SHOW TABLES
        - SHOW CREATE TABLE
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

    import sys

    def table(self, dictlist, outfile=sys.stdout):
        # calculate lengths for each column
        lengths = [ max(list(map(lambda x:len(str(x.get(k))), dictlist)) + [len(str(k))]) for k in dictlist[0].keys() ]
        # compose a formatter-string
        lenstr = "| "+" | ".join("{:<%s}" % m for m in lengths) + " |"
        # print header and borders
        border = "+" + "+".join(["-" * (l + 2) for l in lengths]) + "+"
        print(border, file=outfile)
        header = lenstr.format(*dictlist[0].keys())
        print(header, file=outfile)
        print(border, file=outfile)
        # print rows and borders
        for item in dictlist:
            formatted = lenstr.format(*[str(value) for value in item.values()])
            print(formatted, file=outfile)
        print(border, file=outfile)
                                                                    
    def display(self, result):
        if not result:
            print('None')
        elif self.pretty_print:
            n_rows = 0
            result = list(result)
            #result = iter(result)
            #row = next(result)
            self.table(result)
            #header = row.keys()
            #pt = PrettyTable(header)
            #for item in header:
            #    pt.align[item] = "l"
            #pt.padding_width = 1
            #pt.add_row([row.get(column) for column in header])
            #n_rows += 1
            #for row in result:
                #pt.add_row([row.get(column) for column in header])
                #n_rows += 1
            #pydoc.pager(str(pt))
            return len(result)
        else:
            pydoc.pager(result)
            return len(result)

    def do_pretty(self, ignore):
        '''Toggle pretty-printed results'''
        self.pretty_print = not self.pretty_print
        print('pretty print %s' % ("on" if self.pretty_print else "off"))

    def do_select(self, line):
        '''Send a query to the currently-connected trough segment.

        Syntax: select...

        Example: Send query "select * from host_statistics;" to server
        trough> query select * from host_statistics;
        '''
        try:
            start = datetime.datetime.now()
            result = self.cli.read(self.segment_id, 'select ' + line)
            end = datetime.datetime.now()
            n_rows = self.display(result)
            print("%s results in %s" % (n_rows, end - start))
        except Exception as e:
            self.logger.error(e, exc_info=True)

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
            stream=sys.stdout, level=logging.DEBUG if args.verbose else logging.WARN, format=(
                '%(asctime)s %(levelname)s %(name)s.%(funcName)s'
                '(%(filename)s:%(lineno)d) %(message)s'))

    cli = trough.client.TroughClient(args.rethinkdb_trough_db_url)
    shell = TroughRepl(cli, args.segment, args.writable, args.schema)

    if os.path.exists(HISTORY_FILE):
        readline.read_history_file(HISTORY_FILE)

    try:
        shell.cmdloop()
    finally:
        readline.write_history_file(HISTORY_FILE)

