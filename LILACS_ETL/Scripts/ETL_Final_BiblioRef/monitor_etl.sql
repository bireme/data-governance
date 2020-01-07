-- DROP TABLE monitor_etl;

CREATE TABLE monitor_etl (
	id 				serial 		primary key,
	qtde 			int 		NULL, 
	tabela 			varchar(50) NULL,
	tipo 			varchar(20) NULL,
	dt_in 			timestamp 	NULL
);
