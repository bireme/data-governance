-- DROP TABLE public.descritores_qualificadores;

CREATE TABLE public.descritores_qualificadores (
	id 				int,
	alternate_id 	varchar(55) NULL, 
	fi 				varchar(50) NULL,
	issn 			varchar(50) NULL,
	mhd_cod 		varchar(50) null,
	mhd 			varchar(255) NULL,
	mhs_cod 		varchar(50) null,
	mhs 			varchar(255) NULL,
	decs_code_des 	varchar(50) NULL,
	en_des 			varchar(255) NULL,
	es_des 			varchar(255) NULL,
	fr_des 			varchar(255) NULL,
	pt_br_des 		varchar(255) NULL,
	decs_code_qua 	varchar(50) NULL,
	en_qua 			varchar(255) NULL,
	es_qua 			varchar(255) NULL,
	fr_qua 			varchar(255) NULL,
	pt_br_qua 		varchar(255) NULL,
	tipo_desc		varchar(20) null,
	dt_in 			timestamp null,
	dt_up 			timestamp null
);
