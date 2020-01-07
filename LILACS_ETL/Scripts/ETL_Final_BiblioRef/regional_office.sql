-- DROP TABLE regional_office;

CREATE TABLE regional_office (
	id				serial 			primary key, 
	regional_ex 	varchar(1200) 	NULL,
	regional_abv	varchar(255) 	NULL, 
	pais_ex 		varchar(1200) 	NULL,
	pais_abv 		varchar(255) 	NULL,
	la				varchar(255) 	NULL,
	dt_in 			timestamp 		NULL,
	dt_up 			timestamp 		NULL
);

CREATE INDEX regional_office_id_idx ON public.regional_office (id);

