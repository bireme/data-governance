-- DROP TABLE medline.st_autores;

CREATE TABLE medline.st_autores
(
	ID 		bigint,
	FI 		varchar(255) null, 
	SCH 	varchar(255) null,
	AU 		varchar(255) null, 
	FAU 	varchar(255) null, 
	AF1 	text null, 
	AF2 	text null, 
	AF3 	text null, 
	CI 		text null, 
	PAF 	text null, 
	CN 		text null, 
	AUID 	text null,
	dt_in 	timestamp null,
	dt_up 	timestamp null 
);

CREATE INDEX st_autores_id_idx ON  				medline.st_autores (id); 
