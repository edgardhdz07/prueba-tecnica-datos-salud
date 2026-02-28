SELECT 
    o.type_desc AS tipo_objeto,  -- USER_TABLE / VIEW
    s.name + '.' + o.name AS [tabla/vista],
    c.column_id,
    c.name AS nombre_campo,

    -- Tipo de dato formateado
    CASE 
        WHEN ty.name IN ('varchar','char','varbinary') 
            THEN ty.name + '(' + 
                 CASE WHEN c.max_length = -1 
                      THEN 'MAX' 
                      ELSE CAST(c.max_length AS VARCHAR) 
                 END + ')'
        WHEN ty.name IN ('nvarchar','nchar')
            THEN ty.name + '(' + 
                 CASE WHEN c.max_length = -1 
                      THEN 'MAX' 
                      ELSE CAST(c.max_length/2 AS VARCHAR) 
                 END + ')'
        WHEN ty.name IN ('decimal','numeric')
            THEN ty.name + '(' + 
                 CAST(c.precision AS VARCHAR) + ',' + 
                 CAST(c.scale AS VARCHAR) + ')'
        ELSE ty.name
    END AS tipo_dato,

    c.is_nullable AS permite_nulos,
    c.is_identity AS es_identity,

    -- Clave primaria
    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS es_pk,

    -- Clave foránea
    CASE WHEN fk.parent_column_id IS NOT NULL THEN 1 ELSE 0 END AS es_fk,

    -- Descripción extendida (si existe)
    ep.value AS descripcion

FROM sys.objects o
JOIN sys.schemas s 
    ON o.schema_id = s.schema_id
JOIN sys.columns c 
    ON o.object_id = c.object_id
JOIN sys.types ty 
    ON c.user_type_id = ty.user_type_id

LEFT JOIN (
    SELECT ic.object_id, ic.column_id
    FROM sys.indexes i
    JOIN sys.index_columns ic 
        ON i.object_id = ic.object_id 
       AND i.index_id = ic.index_id
    WHERE i.is_primary_key = 1
) pk 
    ON c.object_id = pk.object_id 
   AND c.column_id = pk.column_id

LEFT JOIN sys.foreign_key_columns fk
    ON c.object_id = fk.parent_object_id
   AND c.column_id = fk.parent_column_id

LEFT JOIN sys.extended_properties ep
    ON ep.major_id = c.object_id
   AND ep.minor_id = c.column_id
   AND ep.name = 'MS_Description'

WHERE s.name = 'core'
  AND o.type IN ('U','V')  -- U = Table, V = View

ORDER BY [tabla/vista], c.column_id;