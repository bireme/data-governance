-- DROP TABLE sjr;

CREATE TABLE sjr (
	id				bigint	NULL, 
	titulo 			varchar(1200) NULL,
	issn			varchar(50)	NULL, 
	score 			numeric(12,4) NULL,
	pais 			varchar(200) NULL,
	dt_in 			timestamp 	NULL,
	dt_up 			timestamp 	NULL
);

CREATE INDEX sjr_id_idx ON public.sjr (id);

