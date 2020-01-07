-- DROP TABLE st_descritor_qualificadores;

CREATE TABLE st_descritor_qualificadores (
	id 				int,
	alternate_id 	varchar(55) NULL, 
	fi 				varchar(50) NULL,
	issn 			varchar(50) NULL,
	mhd_cod 		varchar(50) null,
	mhd 			varchar(255) NULL,
	mhs_cod 		varchar(50) null,
	mhs 			varchar(255) NULL,
	tipo_desc		varchar(20) null,
	dt_in 			timestamp null,
	dt_up 			timestamp null
);

CREATE INDEX st_descritor_qualificadores_id_idx ON public.st_descritor_qualificadores (id);
CREATE INDEX st_descritor_qualificadores_id_idx ON public.st_descritor_qualificadores (id,mhd_cod,mhs_cod);