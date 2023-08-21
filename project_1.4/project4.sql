
SELECT * FROM import_export_logs;

SELECT *  FROM DDS.ft_posting_f;


CREATE OR REPLACE FUNCTION min_max_info(input_date DATE)
RETURNS TABLE (
    transaction_date DATE,
    min_credit_amount NUMERIC,
    max_credit_amount NUMERIC,
    min_debit_amount NUMERIC,
    max_debit_amount NUMERIC
)
LANGUAGE plpgsql AS $func$
BEGIN
    RETURN QUERY
    SELECT
        input_date,
        MIN(CASE WHEN credit_amount IS NOT NULL THEN credit_amount ELSE 0 END)::NUMERIC AS min_credit_amount,
        MAX(CASE WHEN credit_amount IS NOT NULL THEN credit_amount ELSE 0 END)::NUMERIC AS max_credit_amount,
        MIN(CASE WHEN debet_amount IS NOT NULL THEN debet_amount ELSE 0 END)::NUMERIC AS min_debit_amount,
        MAX(CASE WHEN debet_amount IS NOT NULL THEN debet_amount ELSE 0 END)::NUMERIC AS max_debit_amount
    FROM DDS.ft_posting_f
    WHERE TO_CHAR(oper_date, 'YYYY-MM-DD') = TO_CHAR(input_date, 'YYYY-MM-DD');
END
$func$;

SELECT * FROM min_max_info(TO_DATE('2018-01-16', 'YYYY-MM-DD'));



SELECT DISTINCT oper_date FROM DDS.ft_posting_f