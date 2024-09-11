# MySQL connection testing in Flask / Quart

This project contains various attempts & approaches to connect and interact with a MySQL database from a Python code.

Each test is centered around Flask / Quart application. Database requests are triggered by HTTP requests sent to corresponding endpoints.

Main directories:
* ```Aiomysql``` - asynchronous connection pool approach
* ```MySQL_Connector``` - synchronous single connection & connection pool approach
* ```Requests``` - scripts for sending multiple requests at a time


(Yes I am aware that the files in those directories are sometimes direct copies.)