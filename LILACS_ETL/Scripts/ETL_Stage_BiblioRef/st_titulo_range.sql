-- DROP TABLE st_titulo_range;

CREATE TABLE st_titulo_range (
	id 						int NULL, 
	alternate_id 			varchar(55) NULL, 
	title_serial 			text NULL,
	fi 						varchar(50) NULL, 
	title_range				varchar(455) NULL,
	medline_code 			varchar(255) NULL,
	medline_shortened_title varchar(455) NULL,
	thematic_area 			text NULL,
	subtitle 				varchar(455) NULL,
	state 					varchar(255) NULL,
	status_ragne 			varchar(55) NULL,
	editor_cc_code 			varchar(55) NULL, 
	title_id				int NULL, 
	index_code_id 			int NULL, 
	indexer_cc_code 		varchar(55) NULL,
	initial_date 			varchar(255) NULL, 
	initial_volume 			varchar(55) NULL, 
	initial_number 			varchar(55) NULL,
	final_date 				varchar(255) NULL,
	final_volume 			varchar(255) NULL, 
	final_number 			varchar(255) NULL,
	copy 					varchar(55) NULL,
	distribute 				boolean NULL,
	selective 				boolean NULL, 
	code_range 				varchar(55) NULL, 
	pais_range 				varchar(255) NULL,	
	dt_in 					timestamp NULL,
	dt_up 					timestamp NULL 
);

CREATE INDEX st_titulo_range_id_idx ON  						public.st_titulo_range (id);
