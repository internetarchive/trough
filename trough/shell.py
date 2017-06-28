# TODO: trough needs a shell.
import cmd, sys
from trough import db_api
import readline
from prettytable import PrettyTable
import datetime

class TroughShell(cmd.Cmd):
    intro = 'Welcome to the trough shell. Type help or ? to list commands.\n'
    prompt = 'trough> '
    file = None
    _connection = None

    def do_query(self, query):
        '''Send a query to the currently-connected trough segment.

        Syntax: query sql_query

        Example: Send query "select * from host_statistics;" to server
        trough> query select * from host_statistics;
        '''
        cursor = self._connection.cursor()
        start = datetime.datetime.now()
        cursor.execute(query)
        end = datetime.datetime.now()
        output = cursor.fetchall()
        if len(output):
            header = output[0].keys()
            pt = PrettyTable(header)
            for item in header:
                pt.align[item] = "l"
            pt.padding_width = 1
            for row in output:
                pt.add_row([row.get(column) for column in header])
            print(pt)
        print("%s results in %s" % (len(output), end - start))

    def do_connect(self, args):
        '''Connect to a trough segment (remote sqlite database).

        Syntax: connect database rethinkdb_host [proxy] [proxy_port] [proxy_type]

        Example: Connect to database 190123 with rethinkdb running on wbgrp-svc100 and a SOCKS5 proxy running on localhost:9000:
        trough> connect 190123 wbgrp-svc100 localhost 9000
        '''
        args = args.split(" ")
        database = args[0]
        rethinkdb = args[1]
        proxy = args[2] if len(args) >= 3 else None
        try:
            proxy_port = int(args[3]) if len(args) >= 4 else 9000
        except:
            print('Proxy port must be an integer')
        proxy_type = args[4] if len(args) >= 5 else 'SOCKS5'
        self._connection = db_api.connect(database=database,
            rethinkdb=[rethinkdb],
            proxy=proxy,
            proxy_port=proxy_port,
            proxy_type=proxy_type)
        print('connected to %s' % database)

    def do_bye(self, arg):
        '''Close connection and exit.

        Syntax: bye

        Example:
        trough> bye
        Thank you for using trough.'''
        print('\nThank you for using trough.')
        self.close()
        return True

    def do_EOF(self, arg):
        '''Close connection and exit.

        Syntax: ^D

        Example:
        trough> ^D
        Thank you for using trough.'''
        return self.do_bye(arg)

    def close(self):
        if self.file:
            self.file.close()
            self.file = None

if __name__ == '__main__':
    TroughShell().cmdloop()