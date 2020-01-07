-- DROP TABLE medline.st_jd;

CREATE TABLE medline.st_jd
(
	ID 		bigint, 
	FI 		varchar(255) null, 
	SCH 	varchar(255) null, 
	"IS" 	varchar(255) null, 
	JD 		text null,
	dt_in 	timestamp null,
	dt_up 	timestamp null 
);

CREATE INDEX st_jd_id_idx ON  				medline.st_jd (id); 
