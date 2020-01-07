CREATE INDEX autor_afiliacao_id_idx ON  				public.autor_afiliacao (id);
CREATE INDEX st_autor_afiliacao_id_idx ON  				public.st_autor_afiliacao (id); 
CREATE INDEX st_autor_afiliacao_temp_id_idx ON  		public.st_autor_afiliacao_temp (id); 
CREATE INDEX st_autor_afiliacao_transf_temp_id_idx ON  	public.st_autor_afiliacao_transf_temp (id); 

CREATE INDEX st_autor_keywords_id_idx ON  				public.st_autor_keywords (id);

CREATE INDEX st_base_dados_id_idx ON  					public.st_base_dados (id);

CREATE INDEX st_descritor_qualificadores_id_idx ON  	public.st_descritor_qualificadores (id);
CREATE INDEX st_desc_quali_checktag_id_idx	ON  		public.st_desc_quali_checktag (id);
CREATE INDEX st_thesaurus_term_desc_id_idx	ON  		public.st_thesaurus_term_desc (id);
CREATE INDEX descritores_qualificadores_id_idx ON  		public.descritores_qualificadores (id);

CREATE INDEX st_evento_id_idx ON  						public.st_evento (id);

CREATE INDEX st_idiomas_id_idx ON  						public.st_idiomas (id);
CREATE INDEX st_idiomas_temp_id_idx ON  				public.st_idiomas_temp (id);

CREATE INDEX st_instituto_id_idx ON  					public.st_instituto (id);

CREATE INDEX meta_dados_id_idx ON  						public.meta_dados (id);

CREATE INDEX st_resumo_id_idx ON  						public.st_resumo (id);

CREATE INDEX st_titulo_id_idx ON  						public.st_titulo (id);

CREATE INDEX st_url_id_idx ON  							public.st_url (id);
