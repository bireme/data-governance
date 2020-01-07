-- DROP TABLE medline.st_meta_dados;

CREATE TABLE medline.st_meta_dados
(
	ID 		bigint, 
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	CC 		varchar(255) null, 
	TL 		varchar(255) null, 
	NT 		varchar(255) null, 
	UR 		bigint null, 
	TA 		varchar(255) null, 
	VI 		varchar(255) null, 
	FA 		varchar(255) null, 
	"IS" 	varchar(255) null, 
	DP 		bigint null, 
	CY 		varchar(255) null, 
	LA 		varchar(255) null, 
	DB 		varchar(255)  null, 
	JD 		varchar(255)  null, 
	PT 		varchar(255)  null,
	dt_in 	timestamp null,
	dt_up 	timestamp null
);

CREATE INDEX st_meta_dados_id_idx ON  				medline.st_meta_dados (id); 
