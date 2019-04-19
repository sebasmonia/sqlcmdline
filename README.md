# sqlcmdline
Drop in replacement for [sqlcmd](https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility) that works better with sql-mode in Emacs.
It only implements a subset of the options in sqlcmd, but more than enough for SQLi buffers.

The source file can be compiled with PyInstaller. Uses f-strings, so it requires Python 3.6.

## Usage

Let's pretend the docopt help is good enough that I can use it here as documentation :)

```
Usage: sqlcmdline.py [-h | --help] -S <server> -d <database>
                     [--driver <odbc_driver>]
                     (-E | -U <user> -P <password>)

Small command line utility to query MSSQL databases. The required parameters
are named to match the official tool, "sqlcmd".

Required arguments:
  -S <server>       Server name. Optionaly you can specify a port with the
                    format <servername,port>
  -d <database>     Database to open

And then either...
  -E                Use Integrated Security
           -OR-
  -U <user>         SQL Login user
  -P <password>     SQL Login password

Optional arguments:
  --driver <driver> SQL Server ODBC driver name, defaults to {SQL Server}
```

-S, -d, -E and -U & -P work just like their `sqlcmd` counterparts. That means most tools that interact with `sqlcmd` should be able
to use `sqlcmdline` with no changes to the parameter list.

**Coming soon**: read password from environment variable or prompt for it, just like `sqlcmd` does.

## Contributors 

Sebastián Monía - https://github.com/sebasmonia

Hodge - https://github.com/sukeyisme

Kevin Brubeck Unhammer - https://github.com/unhammer
