-- DROP TABLE medline.st_base_dados;

CREATE TABLE medline.st_base_dados
(
	ID 		bigint,
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	"IS" 	varchar(255) null, 
	DB 		varchar(255) null, 
	SF 		varchar(255) null,
	dt_in 	timestamp null,
	dt_up 	timestamp null 
);

CREATE INDEX st_base_dados_id_idx ON  				medline.st_base_dados (id); 
