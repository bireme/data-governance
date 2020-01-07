-- DROP TABLE medline.st_idiomas;

CREATE TABLE medline.st_idiomas
(
	ID 		bigint,
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	"IS" 	varchar(255) null, 
	LA 		varchar(255) null,
	dt_in 	timestamp null,
	dt_up 	timestamp null 
);

CREATE INDEX st_idiomas_id_idx ON  				medline.st_idiomas (id); 
