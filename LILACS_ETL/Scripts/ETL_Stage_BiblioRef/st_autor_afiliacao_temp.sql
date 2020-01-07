-- DROP TABLE public.st_autor_afiliacao_temp;
CREATE TABLE public.st_autor_afiliacao_temp (
	id 						INT,
	alternate_id 			VARCHAR(55) NULL, 
	fi 						VARCHAR(50) NULL,
	au 						TEXT null,
	tl 						varchar(10) NULL, 
	nt 						varchar(10) NULL, 
	th_instituicao 			VARCHAR(300) NULL,
	th_orientador 			TEXT NULL,
	th_titulo_academico 	VARCHAR(255) NULL
);

CREATE INDEX st_autor_afiliacao_temp_id_idx ON  		public.st_autor_afiliacao_temp (id); 
