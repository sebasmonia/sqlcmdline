# sqlcmdline
Drop in replacement for [sqlcmd](https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility) that works better with sql-mode in Emacs.
It only implements a subset of the options in sqlcmd, but more than enough for SQLi buffers.

The source file can be compiled with PyInstaller. Uses f-strings, so it requires Python 3.6.

Latest version has improved compatibility with MySQL and hopefully other DB engines. Please open an issue if you run into any problems!

## Table of contents

<!--ts-->

   * [Usage](#usage)
   * [Commands](#comands)
   * [Custom commands](#custom-commands)
   * [Emacs usage tips](#emacs-usage-tips)
   * [Constributors](#constributors)

<!--te-->


## Usage

Let's pretend the docopt help is good enough that I can use it here as documentation :)

```
Usage: sqlcmdline.py [-h | --help]
                     [-S <server> -d <database>]
                     [-E | -U <user> -P <password>]
                     [--driver <odbc_driver>]

Small command line utility to query databases via ODBC. The parameter names
were chosen to match the official MSSQL tool, "sqlcmd", but all are optional
to provide maximum flexibility (support SQLite, DNS, etc.)

  -S <server>       Server name. Optionaly you can specify a port with the
                    format <servername,port>, or use a DNS

  -d <database>     Database to open

  -E                Use Integrated Security
           -OR-
  -U <user>         SQL Login user
  -P <password>     SQL Login password
           -OR-
  (Nothing at all, for example, SQLite, or DNS includes security)

  --driver <driver> ODBC driver name, defaults to {SQL Server}. Use the value
                    "DSN" to use a Data Source Name in the <server>
                    parameter instead of an actual servername
```

-S, -d, -E and -U & -P work just like their `sqlcmd` counterparts. That means most tools that interact with `sqlcmd` should be able
to use `sqlcmdline` with no changes to the parameter list.

Under Linux, to connect to MSSQL you can use a DSN, see https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-RHEL-6-or-Centos-7 for
more details. If you go that route, specify `--driver DSN` and use the DSN name in the `<server>`.

If everything works, you will get a `>` prompt to type your queries in. Once you are done with your command(s) type `GO` to send it to the server, yet
another vestige of the MSSQL origins. You can also use `;;` as a shorthand at the end of a line instead;

```
ServerName@DatabaseName
> SELECT * FROM SomeTable
> GO

-- results here--

> SELECT * FROM SomeTable;;

-- results here--

```

## Commands

Anything that starts with `:` is interpreted as a command. The `:help` command will print the following text:

```
--Available commands--
Syntax: :command required_parameter [optional_parameter].

Common command modifiers are:
	-eq: makes the search text  parameter an exact match, by default all parameters use LIKE comparisons
	-full: in some commands, will return * from INFORMATION_SCHEMA instead of a smaller subset of columns

:help -- prints the command list
:truncate [chars] -- truncates the results to columns of maximum "chars" length. Default = 100. Setting to 0 shows full contents.
:rows [rownum] -- prints only "rownum" out of the whole resultset. Default = 100. Setting to 0 prints all the rows.
:tables [table_name] -- List all tables, or tables "like table_name"
:cols [-eq] table_name [-full] -- List columns for the table "like table_name" (optionally, equal table_name).
:views [view_name] [-full] -- List all views, or views "like view_name"
:procs [proc_name] [-full] -- List all procedures, or procs "like proc_name"
:funcs [func_name] [-full] -- List all functions, or functions "like func_name"
:src obj.name -- Will call "sp_helptext obj.name". Results won't be truncated.
:deps [to|from] obj.name -- Show dependencies to/from obj.name.
:file [-enc] path -- Opens a file and runs the script. No checking/parsing of the file will take place. Use -enc to change the encoding
 used to read the file. Examples: -utf8, -cp1250, -latin_1
:dbs database_name -- List all databases, or databases "like database_name".
:use database_name -- changes the connection to "database_name".
:timeout [seconds] -- sets the command timeout. Default: 30 seconds.
```

Notes:
* `:use` starts a new ODBC connection to `database_name` in the current server.
* `:tables`, `:cols`, `:views`, `:procs` and `:funcs` use INFORMATION_SCHEMA so they should work across many engines
* `:dbs` currently supports MySQL, MSSQL and Postgres.
* `:deps` and `:src` are MSSQL-only
* Be careful when using `:file`, as the notes above say, the contents will be sent to the server without any validation.

## Custom commands

The file `commands.scl` can be used to define custome commands, which you also invoke via the prefix `:`.
These commands are defined one per line, as `:command-name`[space]`you query`. You can use Python's `format` syntax to replace 
placeholders in the query text by something else.

Example 1: if you have this in the file `:top SELECT TOP {0} * FROM {1}` it would be invoked as `:top 5 my_table` and run the query
`SELECT TOP 5 FROM my_table`

Example 2: `:logs-username SELECT * FROM MyLogTable WHERE Username = '{0}'` can be invoked as `:logs-username admin` and it will run
the query `SELECT * FROM MyLogTable WHERE Username = 'admin'`.

## Emacs usage tips

For Emacs versions < 27, there's a parameter missing in the `sql.el` setup for Microsoft SQL Server, you need to add it:

```elisp
(plist-put (alist-get 'ms sql-product-alist) :prompt-cont-regexp "^[0-9]*>")
```

If you are using this package to connect to a MS SQL Server database, the configuration needed is:

```elisp
(setq sql-ms-options nil) ;; sqlcmdline doesn't support any of the default parameters here
(setq sql-ms-program "sqlcmdline") ;; if using Windows, set this to "python sqlcmdline.py", or a version compiled with PyInstaller
```

If you would like to connect to a different database engine, then the setup is a bit more involved. We want to lift many parameters 
from the SQL Server config, but adjust others for our engine of choice. See below a setup for MySQL:

```elisp
(sql-add-product 'MySQL-ODBC "MySQL-ODBC"
   '(:font-lock sql-mode-mysql-font-lock-keywords                          ;; we want the font lock of our engine of interest
                :sqli-program "sqlcmdline"                                 ;; notes above about this parameter apply here
                :sqli-options ("--driver" "MySQL ODBC 8.0 UNICODE Driver") ;; your driver of choice here
                :sqli-login sql-ms-login-params                            ;; This parameter and all that follow are lifted
                :sqli-comint-func sql-comint-ms                            ;; from the SQL Server configuration because sqlcmdline
                :prompt-regexp "^[0-9]*>"                                  ;; was made to behave like "sqlcmd"
                :prompt-cont-regexp "^[0-9]*>"
                :prompt-length 5
                :syntax-alist ((?@ . "_"))
                :terminator ("^go" . "go"))))
```

Assuming you made the changes listed above, here is how you could add a connection for each engine:

```elisp
(setq sql-connection-alist
      '(("SQLServer-UserPass"
         (sql-product 'ms)
         (sql-user "my-username")
         (sql-password "mY-P45sw0rd!@#")
         (sql-server "aservername")
         (sql-database "master"))
        ("SQLServer-IntegratedSecurity"
         (sql-product 'ms)
         (sql-user "")
         (sql-password "")
         (sql-server "aservername")
         (sql-database "master"))
        ("MySSQL-UserPass"
         (sql-product 'MySQL-ODBC)  ;; The product name is the same we used in sql-add-product
         (sql-user "my-username")
         (sql-password "mY-P45sw0rd!@#")
         (sql-server "amysqlservername")
         (sql-database "mysql"))))
```

Now you can use `sql-connect` to connect to any of the three databases defined above.

## Contributors 

Sebastián Monía - https://github.com/sebasmonia

Hodge - https://github.com/sukeyisme

Kevin Brubeck Unhammer - https://github.com/unhammer
