CREATE USER IF NOT EXISTS 'greyuser'@'localhost';
ALTER USER 'greyuser'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';

CREATE DATABASE IF NOT EXISTS greyfish;
GRANT ALL PRIVILEGES ON greyfish.* to 'greyuser'@'localhost';

use greyfish;

DROP TABLE IF EXISTS greykeys;
CREATE TABLE greykeys (
    username        varchar(20)     NOT NULL,
    token           varchar(24)     NOT NULL,
    timeout         datetime        DEFAULT NULL,
    PRIMARY KEY (token)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS user;
CREATE TABLE user (
    id        	int				NOT NULL AUTO_INCREMENT,
    name		varchar(24)     NOT NULL,
    max_data	varchar(20)		DEFAULT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS node;
CREATE TABLE node (
    ip				varchar(16)     NOT NULL,
    total_space		int     NOT NULL,
    free_space		int     NOT NULL,
    status			varchar(10)     NOT NULL,
    node_key 		varchar(10)     NOT NULL,
    PRIMARY KEY (ip)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS file;
CREATE TABLE file (
	ip 			varchar(16) 	NOT NULL,
	user_id		varchar(24)		NOT NULL,
	id			varchar(50)		NOT NULL,
	directory	varchar(500)	NOT NULL,
	is_dir		tinyint(1)		DEFAULT '0',
	checksum	varchar(64)		DEFAULT NULL,
	PRIMARY KEY (`ip`,`user_id`,`id`,`directory`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;	

CREATE DEFINER=`root`@`localhost` EVENT `DELETE_GREYKEYS` ON SCHEDULE EVERY 10 SECOND STARTS NOW() ON COMPLETION NOT PRESERVE ENABLE DO DELETE FROM greykeys WHERE timeout < NOW();
