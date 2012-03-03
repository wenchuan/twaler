#!/bin/sh
echo 'DROP DATABASE twaler;' | mysql
echo 'CREATE DATABASE twaler;' | mysql
mysql twaler < twaler_schema.sql
