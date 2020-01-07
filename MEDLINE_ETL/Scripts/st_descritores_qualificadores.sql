-- DROP TABLE medline.st_descritores_qualificadores;

CREATE TABLE medline.st_descritores_qualificadores
(
	ID 		varchar(255) null,
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	"IS" 	varchar(255) null, 
	MHD 	varchar(255) null, 
	MHS 	varchar(255) null, 
	MTN 	varchar(255) null,
	dt_in 	timestamp null,
	dt_up 	timestamp null
);

CREATE INDEX st_descritores_qualificadores_id_idx ON  				medline.st_descritores_qualificadores (id); 
