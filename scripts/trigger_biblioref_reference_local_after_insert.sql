delimiter $$

create trigger biblioref_referencelocal_after_insert	after insert on biblioref_referencelocal for each row

/* 
----------------------------------------------------------------------------------------------------------------------
#
# Autor:        José Marcello Lopes
# Data:         13.01.2019
#
# Versão:       1.0
#
# Objetivo:     Trigger para gerar o controle de inserts no MongoDB da tabela biblioref_referencelocal
#
---------------------------------------------------------------------------------------------------------------------- 
*/

begin

	insert into control_updates_bigdata_rows (object, id_row_object, action, processing_indicator) values ('biblioref_referencelocal', new.id, 'I', 'N');

end
$$

delimiter;
