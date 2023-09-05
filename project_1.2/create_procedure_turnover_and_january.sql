create or replace procedure DDS.fill_account_turnover_f (
   i_OnDate date
)
language plpgsql    
as $$
declare
	v_RowCount int;
begin
	
	call DMA.writelog( '[BEGIN] fill(i_OnDate => date ''' 
         || to_char(i_OnDate, 'yyyy-mm-dd') 
         || ''');', 1
       );
    
    call DMA.writelog( 'delete on_date = ' 
         || to_char(i_OnDate, 'yyyy-mm-dd'), 1
       );
	   
    delete
      from DMA.dm_account_turnover_f f
     where f.on_date = i_OnDate;
   
    call DMA.writelog('insert', 1);
	
    insert
      into DMA.dm_account_turnover_f
           ( on_date
           , account_rk
           , credit_amount
           , credit_amount_rub
           , debet_amount
           , debet_amount_rub
           )
    with wt_turn as
    ( select p.credit_account_rk                  as account_rk
           , p.credit_amount                      as credit_amount
           , p.credit_amount * COALESCE(er.reduced_cource, 1)         as credit_amount_rub
           , cast(null as numeric)                 as debet_amount
           , cast(null as numeric)                 as debet_amount_rub
        from DDS.ft_posting_f p
        join DDS.md_account_d a
          on a.account_rk = p.credit_account_rk
        left
        join DDS.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date   and er.data_actual_end_date
       where TO_CHAR(p.oper_date, 'YYYY-MM-DD') = TO_CHAR(i_OnDate, 'YYYY-MM-DD')
         and i_OnDate           between a.data_actual_date    and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
       union all
      select p.debet_account_rk                   as account_rk
           , cast(null as numeric)                 as credit_amount
           , cast(null as numeric)                 as credit_amount_rub
           , p.debet_amount                       as debet_amount
           , p.debet_amount * nullif(er.reduced_cource, 1)          as debet_amount_rub
        from DDS.ft_posting_f p
        join DDS.md_account_d a
          on a.account_rk = p.debet_account_rk
        left 
        join DDS.md_exchange_rate_d er
          on er.currency_rk = a.currency_rk
         and i_OnDate between er.data_actual_date and er.data_actual_end_date
       where TO_CHAR(p.oper_date, 'YYYY-MM-DD') = TO_CHAR(i_OnDate, 'YYYY-MM-DD')
         and i_OnDate           between a.data_actual_date and a.data_actual_end_date
         and a.data_actual_date between date_trunc('month', i_OnDate) and (date_trunc('MONTH', i_OnDate) + INTERVAL '1 MONTH - 1 day')
    )
    select i_OnDate                               as on_date
         , t.account_rk
         , sum(t.credit_amount)                   as credit_amount
         , sum(t.credit_amount_rub)               as credit_amount_rub
         , sum(t.debet_amount)                    as debet_amount
         , sum(t.debet_amount_rub)                as debet_amount_rub
      from wt_turn t
     group by t.account_rk;
	 
	GET DIAGNOSTICS v_RowCount = ROW_COUNT;
    call DMA.writelog('[END] inserted ' || to_char(v_RowCount,'FM99999999') || ' rows.', 1);

    commit;
	
end;$$;

--CALL DDS.fill_account_turnover_f(TO_DATE('2018-01-15', 'YYYY-MM-DD'));

CREATE OR REPLACE PROCEDURE january_showcase() AS $$
DECLARE 
	jan_date DATE := '2018-01-01';
BEGIN
	WHILE jan_date < TO_DATE('2018-02-01', 'YYYY-MM-DD') LOOP
		CALL DDS.fill_account_turnover_f(jan_date);
		jan_date := jan_date+ INTERVAL '1 DAY';
	END LOOP;
END; 
$$ LANGUAGE plpgsql;

CALL january_showcase();

SELECT * FROM DMA.DM_ACCOUNT_TURNOVER_F;
SELECT * FROM DMA.lg_messages;