-- Basic Tables
CREATE TABLE users (
	user_id BIGINT UNSIGNED PRIMARY KEY,
	user_name VARCHAR(100) DEFAULT NULL,
	location VARCHAR(30) DEFAULT NULL,
	description VARCHAR(160) DEFAULT NULL,
	url VARCHAR(100) DEFAULT NULL, 
	followers_count BIGINT UNSIGNED DEFAULT NULL,
	friends_count INTEGER UNSIGNED DEFAULT NULL,
	status_count INTEGER UNSIGNED DEFAULT NULL,
	created_at TIMESTAMP DEFAULT 0
);

CREATE TABLE tweets (
	tweet_id BIGINT UNSIGNED PRIMARY KEY,
	user_id BIGINT UNSIGNED,
	date TIMESTAMP DEFAULT 0, 
	text VARCHAR(140)
);

CREATE TABLE friends (
	user_id BIGINT UNSIGNED,
	friend_id BIGINT UNSIGNED,
	date_added TIMESTAMP DEFAULT 0,
	date_last TIMESTAMP DEFAULT 0,
	UNIQUE (user_id, friend_id)
);

CREATE TABLE mentions (
	tweet_id BIGINT UNSIGNED,
	mentioned_name VARCHAR(100),
	retweet BOOL,
	UNIQUE (tweet_id, mentioned_name, retweet)
);

CREATE TABLE urls (
	tweet_id BIGINT UNSIGNED,
	url VARCHAR(140),
	UNIQUE (tweet_id, url)
);

CREATE TABLE lists (
	list_id BIGINT UNSIGNED PRIMARY KEY,
	list_name VARCHAR(100),
	list_owner BIGINT UNSIGNED
);

CREATE TABLE hash_tags (
	tweet_id BIGINT UNSIGNED,
	hash_tags VARCHAR(140),
	UNIQUE (tweet_id, hash_tags)
);

CREATE TABLE list_memberships (
	list_id BIGINT UNSIGNED,
	member_id BIGINT UNSIGNED,
	date_added TIMESTAMP DEFAULT 0,
	date_last TIMESTAMP DEFAULT 0,
	UNIQUE (list_id, member_id)
);

-- Metadata Tables
CREATE TABLE users_update (
	user_id BIGINT UNSIGNED PRIMARY KEY,
	info_updated TIMESTAMP DEFAULT 0,
	tweet_updated TIMESTAMP DEFAULT 0,
	friend_updated TIMESTAMP DEFAULT 0,
	membership_updated TIMESTAMP DEFAULT 0,
	last_tweet_cursor BIGINT UNSIGNED
);

CREATE TABLE list_update (
	list_id BIGINT UNSIGNED PRIMARY KEY,
	list_updated TIMESTAMP DEFAULT 0
);

CREATE TABLE crawl_instances (
	date TIMESTAMP
);

-- Snorg Account creation
-- CREATE USER 'snorgadmin'@'localhost' IDENTIFIED BY 'snorg321';
-- GRANT ALL PRIVILEGES ON twaler.* TO 'snorgadmin'@'localhost';
-- CREATE USER 'snorgadmin'@'%' IDENTIFIED BY 'snorg321';
-- GRANT ALL PRIVILEGES ON twaler.* TO 'snorgadmin'@'%';
