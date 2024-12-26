USE bnuybase
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';


CREATE TABLE IF NOT EXISTS nodes (
    id INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    -- name references config file, all settings are taken from there
    -- business logic ensures that all node names must exist in the config file
    -- and if new machines are added in the config file, they are added to this table
    -- nodes may never be dropped from this table

    -- todo: store whether the machine is reachable
    -- todo: store information about upload speed, download speed, uptime

    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS directories (
    id INT NOT NULL AUTO_INCREMENT,
    name TEXT NOT NULL,
    parent_id INT, -- the root directory has parent_id NULL

    PRIMARY KEY (id),
    FOREIGN KEY (parent_id) REFERENCES directories(id)
);

CREATE TABLE IF NOT EXISTS root_directory (
    directory_id INT NOT NULL,

    uniqueness_constraint ENUM('1') NOT NULL DEFAULT '1' UNIQUE,

    FOREIGN KEY (directory_id) REFERENCES directories(id)
);

INSERT INTO directories(id, name, parent_id)
    SELECT
        0, '<root>', NULL
        WHERE NOT EXISTS (SELECT * FROM directories);

INSERT INTO root_directory(directory_id)
    SELECT 0
        WHERE NOT EXISTS (SELECT * FROM root_directory);

CREATE TABLE IF NOT EXISTS files (
    uuid BINARY(16) NOT NULL,
    name BLOB NOT NULL,
    directory_id INT NOT NULL,

    stored_on_node_id INT NOT NULL,

    PRIMARY KEY (uuid),
    FOREIGN KEY (stored_on_node_id) REFERENCES nodes(id),
    FOREIGN KEY (directory_id) REFERENCES directories(id)
);

CREATE TABLE IF NOT EXISTS users (
    username TEXT NOT NULL,
    ssh_pubkey TEXT NOT NULL, -- used fo SFTP authentication
    home_directory INT NOT NULL,

    FOREIGN KEY (home_directory) REFERENCES directories(id)
);

INSERT INTO users(username, ssh_pubkey, home_directory)
    SELECT
        'xenia' as username,
        'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIC8q5YMnrLJrgp2azcgi9KgwFUIeH6tkEHrv9AxGYmRH xenia@foxhut' as ssh_pubkey,
        0 as home_directory -- root folder
        WHERE NOT EXISTS (SELECT * FROM users);
