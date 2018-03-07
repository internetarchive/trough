import trough.client
import sys
import argparse
import os
import cmd
import logging
import readline

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
    logger = logging.getLogger('trough.client.TroughRepl')

    def __init__(
            self, trough_client, segment_id, writable=False,
            schema_id='default'):
        super().__init__()
        self.cli = trough_client
        self.segment_id = segment_id
        self.writable = writable
        self.schema_id = schema_id

        self.prompt = 'trough:%s(%s)> ' % (
                segment_id, 'rw' if writable else 'ro')

    def do_select(self, line):
        result = self.cli.read(self.segment_id, 'select ' + line)
        print(result)
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
            return True
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
    arg_parser.add_argument(
            '-s', '--schema', default='default',
            help='schema id for new segment')
    arg_parser.add_argument('segment')
    args = arg_parser.parse_args(args=argv[1:])

    logging.basicConfig(
            stream=sys.stdout, level=logging.DEBUG, format=(
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

