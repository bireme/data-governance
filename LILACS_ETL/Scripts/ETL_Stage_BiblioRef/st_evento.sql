-- DROP TABLE st_evento;

CREATE TABLE st_evento (
	id 				int NULL,
	alternate_id 	varchar(55) null,
	fi 				varchar(50) NULL,
	cn_co 			numeric NULL,
	cn_cy 			varchar(100) NULL,
	cn_da 			varchar(100) NULL,
	cn_dt 			varchar(100) NULL,
	cn_in 			text NULL,
	cn_na 			text NULL,
	pr_in 			varchar(500) NULL, 
	pr_na 			varchar(500) NULL,
	pr_nu 			varchar(500) NULL,
	dt_in 			timestamp null,
	dt_up 			timestamp null 
);
