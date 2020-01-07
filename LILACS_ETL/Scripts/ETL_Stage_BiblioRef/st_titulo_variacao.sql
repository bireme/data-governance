-- DROP TABLE st_titulo_variacao;

CREATE TABLE st_titulo_variacao (
	id 						int NULL, 
	title 					text NULL,
	medline_code 			varchar(255) NULL,
	medline_shortened_title varchar(455) NULL,
	subtitle 				varchar(455) NULL,
	id_variacao				int NULL,
	type 					varchar(55) NULL,
	label 					varchar(455) NULL,
	issn 					varchar(55) NULL,
	initial_year 			varchar(55) NULL,
	title_id 				int NULL,
	initial_number 			varchar(55) NULL,
	initial_volume 			varchar(55) NULL,
	dt_in 					timestamp null NULL,
	dt_up 					timestamp null
);
