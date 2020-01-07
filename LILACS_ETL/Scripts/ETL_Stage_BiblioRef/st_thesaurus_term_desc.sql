-- DROP TABLE st_thesaurus_term_desc;

CREATE TABLE st_thesaurus_term_desc (
  	id int null, 
	en varchar(255) NULL,
	es varchar(255) NULL,
	fr varchar(255) NULL,
	pt_br varchar(255) NULL,
	dt_in timestamp null,
	dt_up timestamp null
);

CREATE INDEX st_thesaurus_term_desc_id_idx ON public.st_thesaurus_term_desc (id,en,es,fr,pt_br);