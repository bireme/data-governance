-- DROP TABLE medline.st_titulo;

CREATE TABLE medline.st_titulo
(
	ID 		bigint,
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	TL 		varchar(255) null, 
	NT 		varchar(255) null, 
	TI 		text, 
	LA 		varchar(255) null,
	dt_in 	timestamp null,
	dt_up 	timestamp null 
);

CREATE INDEX st_titulo_id_idx ON  				medline.st_titulo (id); 
