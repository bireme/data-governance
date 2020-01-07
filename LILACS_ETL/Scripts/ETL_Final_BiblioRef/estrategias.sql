-- DROP TABLE estrategias;

CREATE TABLE estrategias (
	id				bigint 		NULL, 
	banco 			varchar(50) NULL,
	id_iahx			varchar(50)	NULL, 
	tema 			varchar(100) NULL,
	sub_tema1 		varchar(500) NULL,
	sub_tema2 		varchar(500) NULL,
	sub_tema12 		varchar(1000) NULL,
	dt_in 			timestamp 	NULL,
	dt_up 			timestamp 	NULL
);

CREATE INDEX estrategias_id_idx ON public.estrategias USING btree (id);
CREATE INDEX estrategias_id_iahx_idx ON public.estrategias USING btree (id, banco, id_iahx);
CREATE INDEX estrategias_banco_idx ON public.estrategias USING btree (banco);

