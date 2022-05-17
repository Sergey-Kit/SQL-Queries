WITH Unique_Check AS (
	SELECT DISTINCT
		CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
		CAST(date_new AS DATE) AS Date, 
		Id_check, 
		TRANSACTIONS.ID_client,
		SUM(Sum_payment) OVER(PARTITION BY Id_check) AS Check_sum,
		SUM(Sum_payment) OVER(PARTITION BY date_new) AS Monthly_sum,
	    COUNT(Id_check) OVER(PARTITION BY date_new) AS Monthly_Count_Operations,
		COUNT(Id_check) OVER() AS Year_Operations,
		SUM(Sum_payment) OVER() AS Year_Payments
	FROM DigitalLineDB.dbo.TRANSACTIONS
	INNER JOIN DigitalLineDB.dbo.CUSTOMER ON DigitalLineDB.dbo.TRANSACTIONS.ID_client = DigitalLineDB.dbo.CUSTOMER.Id_client
),
	Unique_ID AS (
	SELECT DISTINCT
			CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
			CAST(date_new AS DATE) AS Date, 
			date_new,
			Gender,
			TRANSACTIONS.ID_client,
			(SELECT Gender 
				WHERE Gender = 'F' ) AS Gender_F,
			(SELECT Gender
				WHERE Gender = 'M' ) AS Gender_M,
			(SELECT Gender
				WHERE Gender = '' ) AS Gender_NA
		FROM DigitalLineDB.dbo.TRANSACTIONS
		INNER JOIN DigitalLineDB.dbo.CUSTOMER ON DigitalLineDB.dbo.TRANSACTIONS.ID_client = DigitalLineDB.dbo.CUSTOMER.Id_client
)
SELECT DISTINCT
	t1.Month,
	ROUND(Monthly_sum, 2) AS Monthly_sum,
	Monthly_Count_Operations,
	Mean_Sum_Monthly,
	Count_Clients,
	Per_Year_Operations,
	Per_Year_Payments,
	ROUND(Gender_F_Monthly * 100 / Unique_People_Monthly, 2) AS Per_F,
	ROUND(Gender_M_Monthly * 100 / Unique_People_Monthly, 2) AS Per_M,
	ROUND(Gender_NA_Monthly * 100 / Unique_People_Monthly, 2) AS Per_NA,
	ROUND(Sum_F_Monthly * 100 / Monthly_sum, 2) AS Per_F_Monthly,
	ROUND(Sum_M_Monthly * 100 / Monthly_sum, 2) AS Per_M_Monthly,
	ROUND(Sum_NA_Monthly * 100 / Monthly_sum, 2) AS Per_NA_Monthly
FROM
	(SELECT DISTINCT
		Month,
		Monthly_sum,
		Monthly_Count_Operations,
		ROUND((Monthly_sum/Monthly_Count_Operations),2) AS Mean_Sum_Monthly,
		COUNT(ID_client) OVER(PARTITION BY Month) AS Count_Clients,
		Year_Operations,
		Year_Payments,
		(CAST (CAST (Monthly_Count_Operations AS DECIMAL (18, 2)) * 100 / Year_Operations AS DECIMAL (18, 2))) AS Per_Year_Operations,
		ROUND(Monthly_sum * 100 / Year_Payments, 2) AS Per_Year_Payments,
		SUM(Check_sum) OVER(PARTITION BY ID_client) AS Pay_Per_Person
	FROM Unique_Check
) t1
JOIN
	(SELECT
		Month,
		COUNT(Gender) AS Unique_People_Monthly,
		COUNT(Gender_F) AS Gender_F_Monthly,
		COUNT(Gender_M) AS Gender_M_Monthly,
		COUNT(Gender_NA) AS Gender_NA_Monthly
	FROM Unique_ID
	GROUP BY Month) t2
ON (t1.Month = t2.Month)
JOIN
	(SELECT DISTINCT
		SUM(Check_sum_F) OVER(PARTITION BY TR_1.Month) AS Sum_F_Monthly, 
		TR_1.Month
	FROM
		(SELECT DISTINCT
			CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
			Id_check, 
			SUM(Sum_payment) OVER(PARTITION BY Id_check) AS Check_sum_F
		FROM DigitalLineDB.dbo.TRANSACTIONS
		INNER JOIN DigitalLineDB.dbo.CUSTOMER ON DigitalLineDB.dbo.TRANSACTIONS.ID_client = DigitalLineDB.dbo.CUSTOMER.Id_client
		WHERE Gender = 'F'
	) AS TR_1
) AS TR_12
INNER JOIN (
	SELECT DISTINCT
		SUM(Check_sum_M) OVER(PARTITION BY TR_2.Month) AS Sum_M_Monthly, 
		TR_2.Month
	FROM
		(SELECT DISTINCT
			CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
			Id_check, 
			SUM(Sum_payment) OVER(PARTITION BY Id_check) AS Check_sum_M
		FROM DigitalLineDB.dbo.TRANSACTIONS
		INNER JOIN DigitalLineDB.dbo.CUSTOMER ON DigitalLineDB.dbo.TRANSACTIONS.ID_client = DigitalLineDB.dbo.CUSTOMER.Id_client
		WHERE Gender = 'M'
	) AS TR_2
) AS TR_21
ON (TR_12.Month = TR_21.Month)
INNER JOIN (
	SELECT DISTINCT
		SUM(Check_sum_NA) OVER(PARTITION BY TR_3.Month) AS Sum_NA_Monthly, 
		TR_3.Month
	FROM
		(SELECT DISTINCT
			CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
			Id_check, 
			SUM(Sum_payment) OVER(PARTITION BY Id_check) AS Check_sum_NA
		FROM DigitalLineDB.dbo.TRANSACTIONS
		INNER JOIN DigitalLineDB.dbo.CUSTOMER ON DigitalLineDB.dbo.TRANSACTIONS.ID_client = DigitalLineDB.dbo.CUSTOMER.Id_client
		WHERE Gender = ''
	) AS TR_3
) AS TR_31
ON (TR_21.Month = TR_31.Month)
ON (TR_21.Month = t1.Month)
ORDER BY Month