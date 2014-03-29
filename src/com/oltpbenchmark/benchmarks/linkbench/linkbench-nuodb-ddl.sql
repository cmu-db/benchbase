DROP TABLE IF EXISTS linktable CASCADE;
DROP TABLE IF EXISTS counttable CASCADE;
DROP TABLE IF EXISTS nodetable CASCADE;

CREATE TABLE linktable (
  id1 bigint NOT NULL,
  id2 bigint NOT NULL,
  link_type bigint NOT NULL,
  visibility int NOT NULL,
  data string NOT NULL,
  time bigint NOT NULL,
  version int NOT NULL,
  PRIMARY KEY (id1,id2,link_type)
);
CREATE TABLE counttable (
  id bigint NOT NULL,
  link_type bigint  NOT NULL,
  "count" int NOT NULL,
  time bigint NOT NULL,
  version bigint NOT NULL
);

CREATE TABLE nodetable (
  id bigint NOT NULL,
  type int NOT NULL,
  version bigint NOT NULL,
  time int NOT NULL,
  data String NOT NULL,
  PRIMARY KEY(id)
);
