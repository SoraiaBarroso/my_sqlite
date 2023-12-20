# Welcome to My Sqlite
***

## Task
This project aims to recreate a simplified version of SQLite, a relation database management system, using Ruby. 

## Description
There are two main components in this project, we have 'MySqliteRequest' class, which mimics the behaviour of the SQL queries. This class contains methods which recreate the SQL queries as well as some others built for functionality:
from, select, where, join, order, insert, values, update, set, delete and run. The request is built by progressive calling the methods and finally executed by calling run <br>

The second main component is a command line interface (CLI) created to interact with 'MySqliteRequest' class. The CLI accepts commands for the SQl queries, it as well manages wrong input. <br>

All the data that this both components use are loaded from a .csv file, where the data is read, updated, inserted or deleted. <br>
Example:
```
$>ruby my_sqlite_cli.rb my_sqlite_request.rb 
MySQLite version 0.1 20XX-XX-XX
my_sqlite_cli> SELECT * FROM students;
Jane|me@janedoe.com|A|http://blog.janedoe.com
```
## Installation
There is no intallation needed just run the program with:

```
ruby my_sqlite_cli.rb my_sqlite_request.rb 
```

## Usage
```
$> ruby my_sqlite_cli.rb my_sqlite_request.rb 
MySQLite version 0.1 20XX-XX-XX
my_sqlite_cli> SELECT * FROM students
Jane|me@janedoe.com|A|http://blog.janedoe.com
my_sqlite_cli> quit
```

### The Core Team


<span><i>Made at <a href='https://qwasar.io'>Qwasar SV -- Software Engineering School</a></i></span>
<span><img alt='Qwasar SV -- Software Engineering School's Logo' src='https://storage.googleapis.com/qwasar-public/qwasar-logo_50x50.png' width='20px' /></span>
