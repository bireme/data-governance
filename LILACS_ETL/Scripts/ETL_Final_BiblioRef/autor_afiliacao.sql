-- DROP TABLE autor_afiliacao;

CREATE TABLE autor_afiliacao (
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
	af1_9_1_1  				varchar(1200) NULL,
	af1_10  				varchar(1200) NULL,
	af2_9_1_1  				varchar(1200) NULL,
	af2_10  				varchar(1200) NULL,
	af3_9_1_1  				varchar(1200) NULL,
	af3_10  				varchar(1200) NULL,
	tl 						varchar(10) NULL, 
	nt 						varchar(10) NULL, 
	th_orientador 			TEXT NULL,
	th_titulo_academico 	VARCHAR(255) NULL,
	dt_in 					timestamp null,
	dt_up 					timestamp null
);

CREATE INDEX autor_afiliacao_id_idx ON  				public.autor_afiliacao (id);
