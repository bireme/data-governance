/*
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Cria a tabela de controle de atualizações do Big Data
#
---------------------------------------------------------------------------------------------------------------------- 
*/

drop table control_updates_bigdata_rows;

create table control_updates_bigdata_rows
(
	id_control_update     int						        not null		auto_increment,
	object					      varchar(256)			    not null,
	id_row_object		      int						        not null,
	action					      enum('I', 'U', 'D')		not null,
	processing_indicator	enum('Y', 'N')			  not null,
	instant					      timestamp				      not null		default current_timestamp,
	primary key (id_control_update)
) engine=innodb;

create index idx_control_updates_bigdata_rows_object on control_updates_bigdata_rows(object);

create index idx_control_updates_bigdata_rows_id_row on control_updates_bigdata_rows(id_row_object);

create index idx_control_updates_bigdata_rows_processing_indicator on control_updates_bigdata_rows(processing_indicator);

create index idx_control_updates_bigdata_rows_object_processing_indicator on control_updates_bigdata_rows(object, processing_indicator);

create index idx_control_updates_bigdata_rows_instant on control_updates_bigdata_rows(instant);
