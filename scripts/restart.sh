#!/bin/sh
echo 'DROP DATABASE twaler;' | mysql
echo 'DROP DATABASE temp;' | mysql
echo 'CREATE DATABASE twaler;' | mysql
echo 'CREATE DATABASE temp;' | mysql
mysql twaler < twaler_schema.sql
mysql temp < twaler_schema.sql
