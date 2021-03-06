delimiter $$

create trigger biblioref_reference_after_insert	after insert on biblioref_reference for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        Jos� Marcello Lopes
# Data:         13.01.2019
#
# Vers�o:       1.0
#
# Objetivo:     Trigger para gerar o controle de inserts no MongoDB da tabela biblioref_reference
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_reference', new.id, 'I', 'N');

end
$$

delimiter;
