CREATE OR REPLACE PROCEDURE `main`.`gold`.`sp_gold_dim_shift_upsert`()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
    /* 
       Estrategia:
       - Full refresh vía CTAS para evitar INSERT/MERGE.
       - Si existe la tabla destino, se DROP.
       - Se vuelve a crear con CTAS desde la external table de Silver.
    */
    
    -- Drop tabla gold si existe
    DROP TABLE IF EXISTS main.gold.DimShift;
    
    /* 
       CTAS: crea la dimensión directamente desde Silver.
       Nota: guardo StartTime/EndTime como VARCHAR para evitar problemas de tipo TIME
             dependiendo de cómo venga tipada la external table en tu Dedicated pool.
       Si en tu tabla Silver ya son TIME, puedes cambiar a TIME(0) sin problema.
    */
    
    CREATE OR REPLACE TABLE main.gold.DimShift AS
    SELECT
        CAST(ShiftID AS int)        AS ShiftID,
        CAST(Name AS STRING)        AS ShiftName,
        CAST(StartTime AS STRING)   AS ShiftStartTime,
        CAST(EndTime AS STRING)     AS ShiftEndTime
    FROM main.silver.humanresources_Shift;
END;
