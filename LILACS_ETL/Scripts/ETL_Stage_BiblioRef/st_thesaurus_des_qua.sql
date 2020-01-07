-- DROP TABLE public.st_thesaurus_des_qua;

CREATE TABLE public.st_thesaurus_des_qua (
	decs_code varchar(50) NULL,
	en varchar(255) NULL,
	es varchar(255) NULL,
	fr varchar(255) NULL,
	pt_br varchar(255) NULL,
	dt_in timestamp null,
	dt_up timestamp null
);

CREATE INDEX st_thesaurus_des_qua_decs_code_idx ON public.st_thesaurus_des_qua USING btree (decs_code, en, es, fr, pt_br);