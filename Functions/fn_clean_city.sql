Use OlistDW

GO

CREATE FUNCTION dbo.fn_clean_city (@city VARCHAR(255))
RETURNS VARCHAR(255)
AS
BEGIN
    DECLARE @result VARCHAR(255);

    -- 0) Manejo de NULL
    IF @city IS NULL RETURN NULL;

    -- 1) Inicializar
    SET @result = @city;

    --------------------------------------------------
    -- 2) LIMPIEZA ESPECÍFICA
    --------------------------------------------------

    -- casos particulares
    SET @result = REPLACE(@result, 'z-', '');
    SET @result = REPLACE(@result, '-z', '');
    SET @result = REPLACE(@result, '%26apos%3b', '''');

    -- caracteres problemáticos
    SET @result = REPLACE(@result, '4º', '');
    SET @result = REPLACE(@result, '4o.', '');
    SET @result = REPLACE(@result, '.', '');
    SET @result = REPLACE(@result, '*', '');
    SET @result = REPLACE(@result, '´', '');
    SET @result = REPLACE(@result, '`', '');
    SET @result = REPLACE(@result, '''', ' ');

    -- normalizaciones específicas
    SET @result = REPLACE(@result, 'doeste', 'd oeste');
    SET @result = REPLACE(@result, 'do oeste', 'd oeste');
    SET @result = REPLACE(@result, 'dalho', 'd alho');

    --------------------------------------------------
    -- 3) NORMALIZAR ACENTOS
    --------------------------------------------------

    SET @result = TRANSLATE(@result, N'áéíóú', N'aeiou');
    SET @result = TRANSLATE(@result, N'ãõâêô', N'aoaeo');
    SET @result = TRANSLATE(@result, N'ç', N'c');

    --------------------------------------------------
    -- 4) ELIMINAR NÚMEROS
    --------------------------------------------------

    SET @result = TRANSLATE(@result, '0123456789', '          ');

    --------------------------------------------------
    -- 5) CORTAR POR DELIMITADORES
    --------------------------------------------------

    SET @result =
    CASE
        WHEN CHARINDEX(',', @result) > 0
          OR CHARINDEX('(', @result) > 0
          OR CHARINDEX('-', @result) > 0
        THEN LEFT(
            @result,
            (
                SELECT MIN(pos) 
                FROM (VALUES
                    (NULLIF(CHARINDEX(',', @result), 0)),
                    (NULLIF(CHARINDEX('(', @result), 0)),
                    (NULLIF(CHARINDEX('-', @result), 0))
                ) AS t(pos)
            ) - 1
        )
        ELSE @result
    END;

    --------------------------------------------------
    -- 6) ELIMINAR CARACTERES NO ALFABÉTICOS AL INICIO
    --------------------------------------------------

    WHILE LEN(@result) > 0 AND LEFT(@result,1) LIKE '[^a-zA-Z]'
        SET @result = SUBSTRING(@result, 2, LEN(@result));

    --------------------------------------------------
    -- 7) ELIMINAR DOBLES ESPACIOS
    --------------------------------------------------

    WHILE CHARINDEX('  ', @result) > 0
        SET @result = REPLACE(@result,'  ',' ');

    --------------------------------------------------
    -- 8) TRIM FINAL
    --------------------------------------------------

    SET @result = LTRIM(RTRIM(@result));

    RETURN @result;
END;