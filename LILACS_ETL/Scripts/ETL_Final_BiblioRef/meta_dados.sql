-- DROP TABLE meta_dados;

CREATE TABLE meta_dados (
	id 					int NULL, 
	alternate_id 		varchar(55) NULL, 
	fi 					varchar(50) NULL, 
	cc 					varchar(50) NULL, 
	tl 					varchar(10) NULL, 
	nt 					varchar(10) NULL, 
	ta 					varchar(1200) NULL, 
	vi 					varchar(10) NULL, 
	vis					varchar(100) NULL, 
	vim					varchar(100) NULL, 
	fa 					varchar(1200) NULL, 
	issn 				varchar(50) NULL, 
	dp 					varchar(25) NULL, 
	cy 					varchar(100) NULL, 
	pt 					varchar(1200) NULL, 
	ct 					timestamp NULL, 
	ut 					timestamp NULL, 
	st 					varchar(10) NULL, 
	icc 				varchar(50) null,
	lilacs_original_id 	varchar(50) null, 
	code 				varchar(55) null, 
	cp 					varchar(100) null, 
	da 					varchar(25) null, 
	ed 					varchar(150) null, 
	entry_date 			varchar(10) null, 
	isbn 				varchar(60) null, 
	pu 					text null,
	non_decs_region 	text null,
	tombo 				text null,
	dt_in 				timestamp null,
	dt_up 				timestamp null 
);

CREATE INDEX meta_dados_id_idx ON  						public.meta_dados (id);
