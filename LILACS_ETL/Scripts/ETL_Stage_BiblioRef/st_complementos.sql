-- DROP TABLE st_complementos;

CREATE TABLE st_complementos (
	id 					int NULL, 
	alternate_id 		varchar(55) NULL, 
	fi 					varchar(50) NULL, 
	lo 					varchar(55) null, 
	pg_i 				varchar(50) null, 
	pg_f 				varchar(50) null, 
	pg_e 				varchar(1200) null, 
	pg_m 				varchar(255) null, 
	di 					varchar(1200) null,
	sy 					text null, 
	local_descriptors	varchar(255) null,
	tombo				text null,
	ec_t				varchar(255) null,
	ec_u				varchar(255) null,
	ec_b				varchar(255) null,
	dt_in 				timestamp null,
	dt_up 				timestamp null 
);

CREATE INDEX st_complementos_id_idx ON  						public.st_complementos (id);
