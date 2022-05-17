WITH per_check_t AS (
	SELECT DISTINCT
	CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
	CAST(date_new AS DATE) AS Date, 
	Id_check, 
	ID_client, 
	SUM(Sum_payment) OVER(PARTITION BY Id_check) AS Check_sum,
	COUNT(Count_products) OVER(PARTITION BY Id_check) AS Transactions_Count
	FROM DigitalLineDB.dbo.TRANSACTIONS)
SELECT 
	ID_client, 
	--COUNT(DISTINCT Month) AS Months,
	ROUND(AVG(Check_sum),2) AS Avg_check,
	--ROUND(SUM(Check_sum),2) AS Total_sum,
	ROUND((SUM(Check_sum) / COUNT(DISTINCT Month)),2) AS Mean_Sum_Monthly,
	COUNT(Id_check) AS Checks,
	SUM(Transactions_Count) AS Purchases
FROM per_check_t
GROUP BY per_check_t.ID_client
HAVING COUNT(DISTINCT Month) > 12
ORDER BY Mean_Sum_Monthly DESC;