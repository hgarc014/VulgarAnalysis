--
-- mysql_database_schema.sql
-- Schema for the 140dev Twitter framework MySQL database server
-- sourced from: http://140dev.com/free-twitter-api-source-code-library/twitter-database-server/mysql-database-schema/
--

CREATE DATABASE IF NOT EXISTS TwitterCrawled;

use TwitterCrawled; 

CREATE TABLE IF NOT EXISTS `users` (
  `user_id` bigint(20) unsigned NOT NULL,
  `screen_name` varchar(50) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `profile_image_url` varchar(400) DEFAULT NULL,
  `location` varchar(200) DEFAULT NULL,
  `url` varchar(400) DEFAULT NULL,
  `description` varchar(200) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `followers_count` int(10) unsigned DEFAULT NULL,
  `friends_count` int(10) unsigned DEFAULT NULL,
  `statuses_count` int(10) unsigned DEFAULT NULL,
  `time_zone` varchar(50) DEFAULT NULL,
  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP 
     ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`),
  KEY `name` (`name`),
  KEY `last_update` (`last_update`),
  KEY `screen_name` (`screen_name`)
--  FULLTEXT KEY `description` (`description`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `tweets` (
  `tweet_id` bigint(20) unsigned NOT NULL,
  `tweet_text` varchar(400) NOT NULL,
  `created_at` datetime NOT NULL,
  `geo_lat` decimal(10,5) DEFAULT NULL,
  `geo_long` decimal(10,5) DEFAULT NULL,
  `user_id` bigint(20) unsigned NOT NULL,
  `screen_name` varchar(50) NOT NULL,
  `name` varchar(50) DEFAULT NULL,
  `profile_image_url` varchar(400) DEFAULT NULL,
  `is_rt` tinyint(1) NOT NULL,
  `lang` varchar(10) DEFAULT NULL,
  `favorite_count` int(10) DEFAULT '0',
  `retweet_count` int(10) DEFAULT '0',
  PRIMARY KEY (`tweet_id`),
  KEY `created_at` (`created_at`),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
  FOREIGN KEY (screen_name) REFERENCES users(screen_name) ON DELETE CASCADE,
  FOREIGN KEY (name) REFERENCES users(name) ON DELETE CASCADE
--  FULLTEXT KEY `tweet_text` (`tweet_text`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



CREATE TABLE IF NOT EXISTS `tweet_tags` (
  `tweet_id` bigint(20) unsigned NOT NULL,
  `tag` varchar(100) NOT NULL,
  KEY (tweet_id),
  FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE,
  KEY `tag` (`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `tweet_urls` (
  `tweet_id` bigint(20) unsigned NOT NULL,
  `url` varchar(400) NOT NULL,
  KEY (tweet_id),
  FOREIGN KEY (tweet_id) REFERENCES tweets(tweet_id) ON DELETE CASCADE,
  KEY `url` (`url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

