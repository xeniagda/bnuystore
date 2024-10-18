USE bnuybase


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

CREATE TABLE IF NOT EXISTS files (
    uuid BINARY(16) NOT NULL,
    name BLOB NOT NULL,
    path BLOB NOT NULL,

    stored_on_node_id INT NOT NULL,

    PRIMARY KEY (uuid),
    FOREIGN KEY (stored_on_node_id) REFERENCES nodes(id)
);

