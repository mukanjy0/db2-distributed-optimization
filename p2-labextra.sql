create schema lab15s;
set search_path to lab15s;

CREATE TABLE Venta (
    IdVenta SERIAL NOT NULL,
    DNI_Cliente INTEGER NOT NULL,
    FechaVenta DATE NOT NULL,
    CodLocal VARCHAR(10) NOT NULL,
    ImporteTotal DECIMAL(10, 2) NOT NULL,
    IdEmpleado INTEGER NOT NULL
) PARTITION BY RANGE (FechaVenta);

CREATE TABLE Venta_menor_2022 PARTITION OF Venta FOR VALUES FROM (MINVALUE) TO ('2022-01-01');
CREATE TABLE Venta_2022 PARTITION OF Venta FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE Venta_2023 PARTITION OF Venta FOR VALUES FROM ('2023-01-01') TO (MAXVALUE);

CREATE TABLE Reclamo (
    IdReclamo SERIAL ,
    DNI_Cliente INTEGER NOT NULL,
    FechaReclamo DATE NOT NULL,
    CodLocal VARCHAR(10) NOT NULL,
    Descripcion TEXT NOT NULL,
    Estado VARCHAR(50) NOT NULL
) PARTITION BY LIST (CodLocal);

CREATE TABLE Reclamo_Local_1_4 PARTITION OF Reclamo FOR VALUES IN ('1', '2', '3', '4');
CREATE TABLE Reclamo_Local_5_8 PARTITION OF Reclamo FOR VALUES IN ('5', '6', '7', '8');
CREATE TABLE Reclamo_Local_9_12 PARTITION OF Reclamo FOR VALUES IN ('9', '10', '11', '12');

COPY Venta (IdVenta, DNI_Cliente, FechaVenta, CodLocal, ImporteTotal, IdEmpleado)
    FROM '/var/lib/postgresql/data/data3/ventas.csv'
    DELIMITER ','
    CSV HEADER;

COPY Reclamo (IdReclamo, DNI_Cliente, FechaReclamo, Codlocal, Descripcion, Estado)
    FROM '/var/lib/postgresql/data/data3/reclamos.csv'
    DELIMITER ','
    CSV HEADER;

--Select * From Venta Order By ImporteTotal desc;
--Query 1
SELECT * FROM (
SELECT * FROM (
		SELECT V.*
        FROM Venta_menor_2022 V
        WHERE V.ImporteTotal >= 3000
        UNION ALL
        SELECT V.*
        FROM Venta_2022 V
        WHERE V.ImporteTotal >= 3000
        UNION ALL
        SELECT V.*
        FROM Venta_2023 V
        WHERE  V.ImporteTotal >= 3000
) AS venta3 
ORDER BY ImporteTotal DESC
) AS V3
UNION ALL

SELECT * FROM(
SELECT * FROM (
		SELECT V.*
        FROM Venta_menor_2022 V
        WHERE V.ImporteTotal >= 2000 AND V.ImporteTotal < 3000
        UNION ALL
        SELECT V.*
        FROM Venta_2022 V
        WHERE V.ImporteTotal >= 2000 AND V.ImporteTotal < 3000
        UNION ALL
        SELECT V.*
        FROM Venta_2023 V
        WHERE V.ImporteTotal >= 2000 AND V.ImporteTotal < 3000
) AS venta2 
ORDER BY ImporteTotal DESC
) AS V2

UNION ALL

SELECT * FROM (
	SELECT * FROM (
		SELECT V.*
        FROM Venta_menor_2022 V
        WHERE V.ImporteTotal < 2000
        UNION ALL
        SELECT V.*
        FROM Venta_2022 V
        WHERE V.ImporteTotal < 2000
        UNION ALL
        SELECT V.*
        FROM Venta_2023 V
        WHERE V.ImporteTotal < 2000

	) AS venta1 
	ORDER BY ImporteTotal DESC

) AS V1;


 		
select max(dni_cliente) from venta;

select min(dni_cliente) from venta;


--Select distinct DNI_Cliente From Venta;
 
--QUERY2
SELECT *
FROM (
	
	SELECT DISTINCT DNI_Cliente
    FROM (
        SELECT V.DNI_Cliente
        FROM Venta_menor_2022 V
        WHERE V.DNI_Cliente < 6666
        UNION ALL
        SELECT V.DNI_Cliente
        FROM Venta_2022 V
        WHERE V.DNI_Cliente < 6666
        UNION ALL
        SELECT V.DNI_Cliente
        FROM Venta_2023 V
        WHERE V.DNI_Cliente < 6666
    ) AS venta1
    

    UNION ALL

    SELECT DISTINCT DNI_Cliente
    FROM (
        SELECT V.DNI_Cliente
        FROM Venta_menor_2022 V
        WHERE V.DNI_Cliente >= 6666 AND V.DNI_Cliente < 8333
        UNION ALL
        SELECT V.DNI_Cliente
        FROM Venta_2022 V
        WHERE V.DNI_Cliente >= 6666 AND V.DNI_Cliente < 8333
        UNION ALL
        SELECT V.DNI_Cliente
        FROM Venta_2023 V
        WHERE V.DNI_Cliente >= 6666 AND V.DNI_Cliente < 8333
    ) AS venta2

    UNION ALL
	
	SELECT DISTINCT DNI_Cliente
    FROM (
        SELECT V.DNI_Cliente
        FROM Venta_menor_2022 V
        WHERE V.DNI_Cliente >= 8333
        UNION ALL
        SELECT V.DNI_Cliente
        FROM Venta_2022 V
        WHERE V.DNI_Cliente >= 8333
        UNION ALL
        SELECT V.DNI_Cliente
        FROM Venta_2023 V
        WHERE V.DNI_Cliente >= 8333
    ) AS venta3
    
) AS venta;

		

--QUERY3
--d) Select CodLocal, AVG(ImporteTotal) From Venta Group By CodLocal.
SELECT *
FROM (
    SELECT CodLocal, SUM(SI) / SUM(CV) AS PI
    FROM (
        SELECT CodLocal, SUM(ImporteTotal) AS SI, COUNT(*) AS CV
        FROM (
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_2023 V
            WHERE V.CodLocal IN ('1', '2', '3', '4')
            UNION ALL
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_2022 V
            WHERE V.CodLocal IN ('1', '2', '3', '4')
            UNION ALL
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_menor_2022 V
            WHERE V.CodLocal IN ('1', '2', '3', '4')
        ) AS v1
        GROUP BY CodLocal
    ) AS venta1
    GROUP BY CodLocal

    UNION ALL

    SELECT CodLocal, SUM(SI) / SUM(CV) AS PI
    FROM (
        SELECT CodLocal, SUM(ImporteTotal) AS SI, COUNT(*) AS CV
        FROM (
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_2023 V
            WHERE V.CodLocal IN ('5', '6', '7', '8')
            UNION ALL
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_2022 V
            WHERE V.CodLocal IN ('5', '6', '7', '8')
            UNION ALL
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_menor_2022 V
            WHERE V.CodLocal IN ('5', '6', '7', '8')
        ) AS v2
        GROUP BY CodLocal
    ) AS venta2
    GROUP BY CodLocal

    UNION ALL

    SELECT CodLocal, SUM(SI) / SUM(CV) AS PromedioImporte
    FROM (
        SELECT CodLocal, SUM(ImporteTotal) AS SI, COUNT(*) AS CV
        FROM (
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_2023 V
            WHERE V.CodLocal IN ('9', '10', '11', '12')
            UNION ALL
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_2022 V
            WHERE V.CodLocal IN ('9', '10', '11', '12')
            UNION ALL
            SELECT V.CodLocal, V.ImporteTotal
            FROM Venta_menor_2022 V
            WHERE V.CodLocal IN ('9', '10', '11', '12')
        ) AS v3
        GROUP BY CodLocal
    ) AS venta3
    GROUP BY CodLocal
) AS resultF
ORDER BY CodLocal;


		




