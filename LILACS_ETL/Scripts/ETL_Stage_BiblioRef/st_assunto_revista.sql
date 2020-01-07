-- DROP TABLE st_assunto_revista;

CREATE TABLE st_assunto_revista (
	id 				int,
	alternate_id 	varchar(55) NULL, 
	fi 				varchar(50) NULL,
	issn 			varchar(50) NULL,
	mhd_cod 		varchar(50) null,
	mhd 			varchar(255) NULL,
	tipo_desc		varchar(20) null,
	dt_in 			timestamp null,
	dt_up 			timestamp null
);

CREATE INDEX st_assunto_revista_id_idx ON  						public.st_assunto_revista (id);
