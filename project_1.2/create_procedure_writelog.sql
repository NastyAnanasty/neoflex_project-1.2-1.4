create or replace procedure DMA.writelog (
   i_message		varchar,
   i_messageType	int
)
language plpgsql    
as $$
declare
	log_NOTICE            constant int := 1;
	log_WARNING           constant int := 2;
	log_ERROR             constant int := 3;
	log_DEBUG             constant int := 4;

	c_splitToTable        constant int := 4000;
	c_splitToDbmsOutput   constant int := 900;

	v_logDate           timestamp;
	--v_callerType        varchar;
	--v_callerOwner       varchar;
	--v_caller            varchar;
	--v_line              numeric;
	v_message           varchar;
begin
    v_logDate := now();
    -- split to log table
    v_message := i_message;
	i_messageType	:= log_NOTICE;
    while length(v_message) > 0 loop
      insert into DMA.lg_messages ( 	
		record_id,
		date_time,
		pid,
		message,
		message_type,
		usename, 
		datname, 
		client_addr, 
		application_name,
		backend_start
    )
	select 	
			nextval('DMA.seq_lg_messages'),
			now(),
			pid,
			substr(v_message, 1, c_splitToTable),
			i_messageType,
			usename, 
			datname, 
			client_addr, 
			application_name,
			backend_start
	 from pg_stat_activity
	where pid = pg_backend_pid();
      v_message := substr(v_message, c_splitToTable + 1);
    end loop;

    commit;
end;$$;

SELECT * FROM DMA.lg_messages;
