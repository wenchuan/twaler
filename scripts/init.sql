CREATE DATABASE twaler;
CREATE USER 'snorgadmin'@'localhost' IDENTIFIED BY 'snorg321';
GRANT ALL PRIVILEGES ON twaler.* temp.* TO 'snorgadmin'@'localhost';
