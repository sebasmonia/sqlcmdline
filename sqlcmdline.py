 # !/usr/bin/env python3

import traceback
import sys
import pyodbc
import math
import operator as op
from collections import defaultdict, namedtuple
from datetime import datetime

PreparedCommand = namedtuple("PrepCmd", "query error callback")
max_column_width = 100
chars_to_cleanup = str.maketrans("\n\t\r", "   ")


def command_help(params):
    t = ('--Available commands--'
         'Syntax: :command required_parameter [optional_parameter].'
         'Common command modifiers are:\n'
         '\t-eq: makes the next parameter an exact match, by default'
         'all parameters use LIKE comparisons\n'
         '\t-full: for certain commands, will return * from '
         'INFORMATION_SCHEMA instead of a smaller subset of columns\n')
    print(t)
    sep = " -- "
    t = (f':help{sep}prints the command list\n'
         f':truncate [chars]{sep}truncates the results to columns of'
         f'maximum "chars" lenght. Default = 100. Setting to 0 shows full'
         f'contents.\n'
         f':tables [table_name]{sep}List all tables, or tables "like '
         f'table name"\n'
         f':cols [-eq] table_name [-full]{sep}List columns for the table '
         f'"like table name" (optionally, equal table_name).\n'
         f':views [view_name] [-full]{sep}List all views, or views "like '
         f'view_name"\n'
         f':procs [proc_name] [-full]{sep}List all procedures, or procs '
         f'"like proc_name"\n'
         f':funcs [func_name] [-full]{sep}List all functions, or procs '
         f'"like func_name"\n'
         f':def [obj] will  call "sp_helptext obj". Results won\'t be '
         f'truncated.\n')
    print(t)
    return (None, None, None)


def command_truncate(params):
    try:
        global max_column_width
        col_size = int(params[0])
        # I guess this could be improved...
        max_column_width = col_size if col_size != 0 else 1000000000
        print("Truncate value set")
        return PreparedCommand(None, None, None)
    except Exception as e:
        return PreparedCommand(None, "Invalid arguments", None)


def command_tables(params):
    q = f"SELECT * FROM INFORMATION_SCHEMA.TABLES "
    if params:
        if len(params) == 1:
            q += f"WHERE TABLE_NAME LIKE '%{params[0]}%'"
        else:
            return PreparedCommand(None, "Invalid arguments", None)
    return PreparedCommand(q, None, None)


def command_columns(params):
    try:
        cols = ("*" if "-full" in params else
                "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, "
                "COLUMN_NAME, DATA_TYPE")
        q = f"SELECT {cols} FROM INFORMATION_SCHEMA.COLUMNS "
        if params[0] == "-eq":
            q += f"WHERE TABLE_NAME = '{params[1]}'"
        else:
            q += f"WHERE TABLE_NAME LIKE '%{params[0]}%'"
        return PreparedCommand(q, None, None)
    except IndexError as ie:
        return PreparedCommand(None, "Invalid arguments", None)
    except Exception as e:
        return PreparedCommand(None, str(e), None)


def command_views(params):
    try:
        cols = ("*" if "-full" in params else
                "TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,"
                " CHECK_OPTION, IS_UPDATABLE")
        q = f"SELECT {cols} FROM INFORMATION_SCHEMA.VIEWS "
        # params can be either -full, viewname -full, just viewname, or empty
        if params and not all(p == '-full' for p in params):
            q += f"WHERE TABLE_NAME LIKE '%{params[0]}%'"
        return PreparedCommand(q, None, None)
    except Exception as e:
        return PreparedCommand(None, "Invalid arguments")


def command_procedures(params):
    try:
        cols = ("*" if "-full" in params else
                "ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, "
                "DATA_TYPE, CREATED, LAST_ALTERED")
        q = (f"SELECT {cols} FROM INFORMATION_SCHEMA.ROUTINES WHERE "
             f"ROUTINE_TYPE = 'PROCEDURE' ")
        if params and not all(p == '-full' for p in params):
            q += f"AND ROUTINE_NAME LIKE '%{params[0]}%'"
        return PreparedCommand(q, None, None)
    except Exception as e:
        return PreparedCommand(None, "Invalid arguments", None)


def command_functions(params):
    try:
        cols = ("*" if "-full" in params else
                "ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, "
                "DATA_TYPE, CREATED, LAST_ALTERED")
        q = (f"SELECT {cols} FROM INFORMATION_SCHEMA.ROUTINES WHERE "
             f"ROUTINE_TYPE = 'FUNCTION' ")
        if params and not all(p == '-full' for p in params):
            q += f"AND ROUTINE_NAME LIKE '%{params[0]}%'"
        return PreparedCommand(q, None, None)
    except Exception as e:
        return PreparedCommand(None, "Invalid arguments", None)


def command_definition(params):
    try:
        global max_column_width
        current_value = max_column_width

        def revert_truncate():
            global max_column_width
            max_column_width = current_value

        max_column_width = 1000000000
        q = f"sp_helptext {params[0]}"
        return PreparedCommand(q, None, revert_truncate)
    except Exception as e:
        return PreparedCommand(None, str(e), None)


commands = {":help": command_help,
            ":tables": command_tables,
            ":cols": command_columns,
            ":views": command_views,
            ":procs": command_procedures,
            ":funcs": command_functions,
            ":truncate": command_truncate,
            ":def": command_definition}


def text_formatter(value):
    value = str(value)
    value = str.translate(value, chars_to_cleanup)
    if len(value) > max_column_width:
        value = value[:max_column_width-5] + "[...]"
    return value


def print_rows(cursor):
    odbc_rows = cursor.fetchall()
    column_names = [text_formatter(column[0]) for column in cursor.description]
    format_str, print_ready = format_rows(column_names, odbc_rows)
    print()  # blank line
    for row in print_ready:
        print(format_str.format(*row))
    print("\nRows returned:", len(odbc_rows), "\n")


def format_rows(column_names, raw_rows):
    # lenghts will match columns by position
    column_widths = defaultdict(lambda: 0)
    formatted = []
    for row in raw_rows:
        new_row = []
        for index, value in enumerate(row):
            new_value = "#unknown#"
            if value is None:
                new_len = 6
                new_value = "[NULL]"
            if isinstance(value, bool):
                new_len = 6
                new_value = value
            elif isinstance(value, datetime):
                new_len = 26
                new_value = value.isoformat()
            elif isinstance(value, int):
                new_len = number_len(value)
                new_value = value
            elif isinstance(value, str):
                new_value = text_formatter(value)
                new_len = len(new_value)

            if new_len > column_widths[index]:
                column_widths[index] = new_len
            new_row.append(new_value)
        formatted.append(tuple(new_row))

    for index, col_name in enumerate(column_names):
        column_widths[index] = max((column_widths[index], len(col_name)))

    format_str = "|".join(["{{{ndx}:{len}}}".format(ndx=ndx, len=len)
                           for ndx, len in column_widths.items()])
    formatted.insert(0, column_names)
    # IIRC now dicts are ordered but just in case/for other implementations
    formatted.insert(1, ["-"*width for index, width in
                         sorted(column_widths.items(),
                                key=op.itemgetter(0))])
    return format_str, formatted


def number_len(number):
    # Source:
    # http://stackoverflow.com/questions/2189800/length-of-an-integer-in-python
    if number > 0:
        digits = int(math.log10(number))+1
    elif number == 0:
        digits = 1
    else:
        digits = int(math.log10(-number))+2  # +1 if you don't count the '-'
    return digits


def process_command(line_typed):
    command_name, *params = line_typed.split(" ")
    if command_name not in commands:
        t = "Invalid command name. Use :help for a list of available commands."
        return None, t
    command_handler = commands[command_name]
    query, error, cb = command_handler(params)
    if not error and query:
        print(f"Query: {query}")
    return query, error, cb


def parse_arguments(args):
    # boneheaded algorithm
    for index, argument in enumerate(args):
        if argument == "-S":
            server = args[index+1]
        elif argument == "-d":
            database = args[index+1]
        # elif argument == "-E"
    return server, database


if __name__ == "__main__":
    server, database = parse_arguments(sys.argv)
    connection = (f"Driver={{SQL Server Native Client 11.0}};Server={server};"
                  f"Trusted_Connection=Yes;Database={database}")
    conn = pyodbc.connect(connection, autocommit=True)
    conn.timeout = 30  # 30 second timeout for queries. Should be configurable.
    cursor = conn.cursor()
    print(f'Connected to server {server} database {database}')
    print()
    print('Special commands are prefixed with ":". For example, use ":exit" to'
          ' finish your session. Everything else is sent directly to the'
          'server using ODBC.')
    print('Use ":help" to get a list of commands available')
    print()
    prompt = f"{server}@{database}>"
    query = input(prompt)
    callback = None
    while query != ":exit":
        try:
            print()  # blank line
            if query.startswith(":"):
                query, cmd_error, callback = process_command(query)
                if cmd_error:
                    print(f"Command error: {cmd_error}")
            if query:
                cursor.execute(query)
                rcount = cursor.rowcount  # -1 for "select" queries
                if rcount == -1:
                    print_rows(cursor)
                else:
                    print("\nRows affected:", rcount, flush=True)
                if callback:
                    callback()
                    callback = None
        except Exception as e:
            print("---ERROR---\n")
            traceback.print_exc()
            print("\n---ERROR---", flush=True)
        print()  # blank line
        query = input(prompt)
