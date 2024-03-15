SELECT * FROM transactions.transactions;

SELECT count(*) FROM transactions.transactions;
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'transactions'  
    AND TABLE_NAME = 'transactions';   
