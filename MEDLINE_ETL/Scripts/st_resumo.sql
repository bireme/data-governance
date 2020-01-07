-- DROP TABLE medline.st_resumo;

CREATE TABLE medline.st_resumo
(
	ID 		bigint, 
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	TL 		varchar(255) null, 
	NT 		varchar(255) null, 
	AB 		text null, 
	LA 		varchar(255) null,
	dt_in 	timestamp null,
	dt_up 	timestamp null 
);

CREATE INDEX st_resumo_id_idx ON  				medline.st_resumo (id); 
