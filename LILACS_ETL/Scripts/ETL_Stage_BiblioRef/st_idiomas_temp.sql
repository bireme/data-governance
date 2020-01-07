-- DROP TABLE public.st_idiomas_temp;
CREATE TABLE public.st_idiomas_temp (
	id 				int,
	alternate_id 	varchar(55) NULL, 
	fi 				varchar(50) NULL,
	la_1 			varchar(10) NULL,
	la_2 			varchar(10) NULL,
	la_3 			varchar(10) NULL,
	la_4 			varchar(10) NULL
);

CREATE INDEX st_idiomas_temp_id_idx ON  				public.st_idiomas_temp (id);
