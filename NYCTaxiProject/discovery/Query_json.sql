--Read single line json:defines Field to read each record as a single json document.
SELECT CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
       CAST(JSON_VALUE(jsonDoc,'$.payment_type_desc') AS VARCHAR(15)) payment_type_desc
FROM
    OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        ROWTERMINATOR = '0x0a',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    )AS payment_type


--OpenJsOn function
SELECT payment_type, description
FROM
    OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    )AS payment_type
    CROSS APPLY OPENJSON(jsonDoc) --INNER JOIN WITHOUT ANY JOIN CONDITION.
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc'
    );

--READ DATA with arrys.
SELECT CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
       CAST(JSON_VALUE(jsonDoc,'$.payment_type_desc[0].value') AS VARCHAR(15)) payment_type_desc,
       CAST(JSON_VALUE(jsonDoc,'$.payment_type_desc[1].value') AS VARCHAR(15)) payment_type_desc
FROM
    OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        ROWTERMINATOR = '0x0a',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    )AS payment_type

--Use OPENJSON FOR ARRAYS (Nested json)
SELECT payment_type, sub_type, payment_type_desc_value
FROM
    OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        ROWTERMINATOR = '0x0a',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    )AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        payment_type_desc NVARCHAR(MAX) AS JSON
    )
    CROSS APPLY OPENJSON(payment_type_desc)
    WITH(
        sub_type SMALLINT,
        payment_type_desc_value VARCHAR(20) '$.value'
    );

--Query Standard json (Overwrite the new line character)
SELECT rate_code_id, rate_code
FROM
    OPENROWSET(
        BULK 'rate_code.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        ROWTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    )AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(20)
    );

--Query json multiple lines (classic json)

SELECT rate_code_id, rate_code
FROM
    OPENROWSET(
        BULK 'rate_code_multi_line.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        ROWTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b'
    ) WITH(
        jsonDoc NVARCHAR(MAX)
    )AS rate_code
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(20)
    );