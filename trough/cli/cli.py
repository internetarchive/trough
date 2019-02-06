import trough.client
import sys
import argparse
import os
import cmd
import logging
import readline
from prettytable import PrettyTable
import datetime

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
        '''SHOW command. like MySQL. Currently only
        SHOW TABLES
        is implemented.'''
        if argument.lower() == 'tables':
            self.do_select("name from sqlite_master where type = 'table';")

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
        start = datetime.datetime.now()
        result = self.cli.read(self.segment_id, 'select ' + line)
        end = datetime.datetime.now()
        if self.pretty_print:
            header = result[0].keys()
            pt = PrettyTable(header)
            for item in header:
                pt.align[item] = "l"
            pt.padding_width = 1
            for row in result:
                pt.add_row([row.get(column) for column in header])
            print(pt)
        else:
            print(result)
        print("%s results in %s" % (len(result), end - start))

    do_SELECT = do_select

    def emptyline(self):
        pass

    def default(self, line):
        if line == 'EOF':
            print()
            return True
        elif self.writable:
            self.cli.write(self.segment_id, line, schema_id=self.schema_id)
        else:
            self.logger.error('read-only!')

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

