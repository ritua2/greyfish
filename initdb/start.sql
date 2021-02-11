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

CREATE DEFINER=`root`@`localhost` EVENT `DELETE_GREYKEYS` ON SCHEDULE EVERY 10 SECOND STARTS NOW() ON COMPLETION NOT PRESERVE ENABLE DO DELETE FROM greykeys WHERE timeout < NOW();
