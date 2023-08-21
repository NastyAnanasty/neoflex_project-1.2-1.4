SELECT 
	column_name, 
	data_type 
FROM information_schema.columns 
WHERE table_name = 'dm_f101_round_f' 
ORDER BY ordinal_position;


SELECT * FROM DMA.dm_f101_round_f_v2;


SELECT * FROM DMA.dm_f101_round_f_v2
EXCEPT
SELECT * FROM DMA.dm_f101_round_f;


CREATE TABLE import_export_logs (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT current_timestamp,
    message TEXT
);

SELECT * FROM import_export_logs;
