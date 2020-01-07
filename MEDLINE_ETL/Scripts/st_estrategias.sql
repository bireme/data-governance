-- DROP TABLE st_estrategias;

CREATE TABLE st_estrategias (
	id				bigint 		NULL, 
	banco 			varchar(255) NULL,
	tema 			varchar(255) NULL,
	sub_tema1 		varchar(500) NULL,
	sub_tema2 		varchar(500) NULL,
	sub_tema12 		varchar(1000) NULL,
	dt_in 			timestamp 	NULL,
	dt_up 			timestamp 	NULL
);

CREATE INDEX st_estrategias_id_idx ON medline.st_estrategias (id);

