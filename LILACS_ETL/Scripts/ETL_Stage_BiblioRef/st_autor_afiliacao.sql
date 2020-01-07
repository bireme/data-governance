-- DROP TABLE st_autor_afiliacao;

CREATE TABLE st_autor_afiliacao (
	id 						int,
	alternate_id 			varchar(55) NULL, 
	fi 						varchar(50) NULL,
	au 						varchar(255) NULL,
	ordem_au 				int NULL,
	af1 					varchar(1200) NULL,
	af2 					varchar(1200) NULL,
	af3 					varchar(1200) NULL,
	ci 						varchar(255) NULL,
	paf 					varchar(50) NULL,
	cn 						varchar(255) NULL,
	auid 					varchar(50) NULL,
	gr 						varchar(1200) NULL,
	nome_s_ab 				varchar(255) NULL,
	nome_f 					varchar(255) NULL,
	tl 						varchar(10) NULL, 
	nt 						varchar(10) NULL, 
	th_orientador 			text NULL,
	th_titulo_academico 	varchar(255) NULL,
	dt_in 					timestamp null,
	dt_up 					timestamp null
);

CREATE INDEX st_autor_afiliacao_id_idx ON  				public.st_autor_afiliacao (id); 
