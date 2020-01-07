-- DROP TABLE st_url;

CREATE TABLE st_url (
	id 				int NULL,
	alternate_id 	varchar(55) NULL,
	fi 				varchar(50) NULL,
	tp 				varchar(50) null,
	ur 				text null,
	arq 			varchar(50) null,
	la 				varchar(10) null,
	texto_completo 	bool null,
	nota_publicacao varchar(1200) null,
	nota_n_publica  varchar(1200) null,
	dt_in 			timestamp null,
	dt_up 			timestamp null
);
