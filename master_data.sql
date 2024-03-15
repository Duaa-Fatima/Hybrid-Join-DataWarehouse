SELECT * FROM master_data.master_data;

SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'master_data'  
    AND TABLE_NAME = 'master_data';    

##UPDATE master_data.master_data SET productPrice = CAST(productPrice AS SIGNED) WHERE NOT productPrice REGEXP '^[0-9]+$';

UPDATE master_data.master_data
SET productPrice = REPLACE(productPrice, '$', '');

SELECT * FROM master_data.master_data LIMIT 10
