-- DROP TABLE public.st_autor_afiliacao_transf_temp;
CREATE TABLE public.st_autor_afiliacao_transf_temp (
	id 						INT,
	alternate_id 			VARCHAR(55) NULL, 
	fi 						VARCHAR(50) NULL,
	au 						VARCHAR(1200) NULL,
	ordem_au 				SERIAL,
	af1 					VARCHAR(3000) NULL,
	af2 					VARCHAR(1200) NULL,
	af3 					VARCHAR(1200) NULL,
	ci 						VARCHAR(300) NULL,
	paf 					VARCHAR(50) NULL,
	grau_resp 				VARCHAR(1200) NULL,
	cn 						VARCHAR(255) NULL,
	auid 					VARCHAR(50) NULL,
	tl 						varchar(10) NULL, 
	nt 						varchar(10) NULL, 
	th_orientador 			TEXT NULL,
	th_titulo_academico 	VARCHAR(255) NULL
);

CREATE INDEX st_autor_afiliacao_transf_temp_id_idx ON  	public.st_autor_afiliacao_transf_temp (id); 
