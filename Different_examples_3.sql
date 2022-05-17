WITH Age_Quarter_Groups AS (
SELECT DISTINCT
	Id_client,
	Month_Group,
	Check_sum,
	Id_check,
	ROUND(SUM(Check_sum) OVER(PARTITION BY Month_Group), 2) AS Quarter_sum,
	COUNT (Id_check) OVER(PARTITION BY Month_Group) AS Quarter_Count_Operations,
	CASE
        WHEN age BETWEEN 1 AND 10 THEN '1-10'
        WHEN age BETWEEN 11 AND 20 THEN '11-20'
        WHEN age BETWEEN 21 AND 30 THEN '21-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        WHEN age BETWEEN 51 AND 60 THEN '51-60'
        WHEN age BETWEEN 61 AND 70 THEN '61-70'
        WHEN age BETWEEN 71 AND 80 THEN '71-80'
        WHEN age BETWEEN 81 AND 90 THEN '81-90'
        ELSE 'NaN' END AS Age_Group
FROM
(
	SELECT DISTINCT
		Unique_Check.Id_client, 
		Age, 
		CASE
			WHEN Month BETWEEN '2015-3' AND '2015-6' THEN '2_quarter_2015'
			WHEN Month BETWEEN '2015-7' AND '2015-9' THEN '3_quarter_2015'
			WHEN Month BETWEEN '2015-10' AND '2015-12' THEN '4_quarter_2015'
			WHEN Month BETWEEN '2016-1' AND '2016-3' THEN '1_quarter_2016'
			WHEN Month BETWEEN '2016-4' AND '2016-6' THEN '2_quarter_2016'
			END AS Month_Group,
		Id_check,
		Check_sum
	FROM 
	(
		SELECT DISTINCT
			CONCAT(YEAR(date_new),'-',MONTH(date_new)) AS Month, 
			CUSTOMER.Id_client,
			Id_check,
			Age,
			SUM(Sum_payment) OVER(PARTITION BY Id_check) AS Check_sum
		FROM DigitalLineDB.dbo.TRANSACTIONS
		INNER JOIN DigitalLineDB.dbo.CUSTOMER ON DigitalLineDB.dbo.TRANSACTIONS.ID_client = DigitalLineDB.dbo.CUSTOMER.Id_client
	) AS Unique_Check
) AS Unique_Quarter
)
SELECT DISTINCT
	a1.Age_Group,
	ROUND(SUM(Client_sum_All) OVER(PARTITION BY a6.Age_Group), 2) AS All_sum,
	SUM (ID_Count_Operations_All) OVER(PARTITION BY a6.Age_Group) AS All_Operations,
	Age_Group_sum_2q_2015,
	ROUND(Age_Group_sum_2q_2015 * 100 / a1.Quarter_sum, 2) AS Sum_Per_2q_2015,
	Age_Group_Operations_2q_2015,
	CAST (CAST (Age_Group_Operations_2q_2015 AS DECIMAL (18, 2)) * 100 / a1.Quarter_Count_Operations AS DECIMAL (18, 2)) AS Operations_Per_2q_2015,
	Age_Group_sum_3q_2015,
	ROUND(Age_Group_sum_3q_2015 * 100 / a2.Quarter_sum, 2) AS Sum_Per_3q_2015,
	Age_Group_Operations_3q_2015,
	CAST (CAST (Age_Group_Operations_3q_2015 AS DECIMAL (18, 2)) * 100 / a2.Quarter_Count_Operations AS DECIMAL (18, 2)) AS Operations_Per_3q_2015,
	Age_Group_sum_4q_2015,
	ROUND(Age_Group_sum_4q_2015 * 100 / a3.Quarter_sum, 2) AS Sum_Per_4q_2015,
	Age_Group_Operations_4q_2015,
	CAST (CAST (Age_Group_Operations_4q_2015 AS DECIMAL (18, 2)) * 100 / a3.Quarter_Count_Operations AS DECIMAL (18, 2)) AS Operations_Per_4q_2015,
	Age_Group_sum_1q_2016,
	ROUND(Age_Group_sum_1q_2016 * 100 / a4.Quarter_sum, 2) AS Sum_Per_1q_2016,
	Age_Group_Operations_1q_2016,
	CAST (CAST (Age_Group_Operations_1q_2016 AS DECIMAL (18, 2)) * 100 / a4.Quarter_Count_Operations AS DECIMAL (18, 2)) AS Operations_Per_1q_2016,
	Age_Group_sum_2q_2016,
	ROUND(Age_Group_sum_2q_2016 * 100 / a5.Quarter_sum, 2) AS Sum_Per_2q_2016,
	Age_Group_Operations_2q_2016,
	CAST (CAST (Age_Group_Operations_2q_2016 AS DECIMAL (18, 2)) * 100 / a5.Quarter_Count_Operations AS DECIMAL (18, 2)) AS Operations_Per_2q_2016,
	(Age_Group_sum_2q_2015 + Age_Group_sum_3q_2015 + Age_Group_sum_4q_2015 + Age_Group_sum_1q_2016 + Age_Group_sum_2q_2016) / 5 AS Mean_Quaterly_Sum,
	(Age_Group_Operations_2q_2015 + Age_Group_Operations_3q_2015 + Age_Group_Operations_4q_2015 + Age_Group_Operations_1q_2016 + Age_Group_Operations_2q_2016) / 5 AS Mean_Amount_Operations_Quaterly
FROM(
	(SELECT DISTINCT
		--Month_Group,
		t1.Quarter_sum,
		t1.Quarter_Count_Operations,
		Age_Group,
		SUM(Check_sum) OVER(PARTITION BY Age_Group) AS Age_Group_sum_2q_2015,
		COUNT (Id_check) OVER(PARTITION BY Age_Group) AS Age_Group_Operations_2q_2015
	FROM(
		SELECT *
		FROM Age_Quarter_Groups
		WHERE Month_Group = '2_quarter_2015'
	) AS t1
) a1
	JOIN
		(SELECT DISTINCT
			t2.Quarter_sum,
			t2.Quarter_Count_Operations,
			Age_Group,
			SUM(Check_sum) OVER(PARTITION BY Age_Group) AS Age_Group_sum_3q_2015,
			COUNT (Id_check) OVER(PARTITION BY Age_Group) AS Age_Group_Operations_3q_2015
		FROM(
			SELECT *
			FROM Age_Quarter_Groups
			WHERE Month_Group = '3_quarter_2015'
		) AS t2
	) a2
	ON (a1.Age_Group = a2.Age_Group)
	JOIN
		(SELECT DISTINCT
			Age_Group,
			t3.Quarter_sum,
			t3.Quarter_Count_Operations,
			SUM(Check_sum) OVER(PARTITION BY Age_Group) AS Age_Group_sum_4q_2015,
			COUNT (Id_check) OVER(PARTITION BY Age_Group) AS Age_Group_Operations_4q_2015
		FROM(
			SELECT *
			FROM Age_Quarter_Groups
			WHERE Month_Group = '4_quarter_2015'
		) AS t3
	) a3
	ON (a1.Age_Group = a3.Age_Group)
	JOIN
		(SELECT DISTINCT
			Age_Group,
			t4.Quarter_sum,
			t4.Quarter_Count_Operations,
			SUM(Check_sum) OVER(PARTITION BY Age_Group) AS Age_Group_sum_1q_2016,
			COUNT (Id_check) OVER(PARTITION BY Age_Group) AS Age_Group_Operations_1q_2016
		FROM(
			SELECT *
			FROM Age_Quarter_Groups
			WHERE Month_Group = '1_quarter_2016'
		) AS t4
	) a4
	ON (a1.Age_Group = a4.Age_Group)
	JOIN
		(SELECT DISTINCT
			Age_Group,
			t5.Quarter_sum,
			t5.Quarter_Count_Operations,
			SUM(Check_sum) OVER(PARTITION BY Age_Group) AS Age_Group_sum_2q_2016,
			COUNT (Id_check) OVER(PARTITION BY Age_Group) AS Age_Group_Operations_2q_2016
		FROM(
			SELECT *
			FROM Age_Quarter_Groups
			WHERE Month_Group = '2_quarter_2016'
		) AS t5
	) a5
	ON (a1.Age_Group = a5.Age_Group)
)
	JOIN
	(
	SELECT DISTINCT
		Age_Group,
		SUM(Check_sum) OVER(PARTITION BY Age_Quarter_Groups.Id_client) AS Client_sum_All,
		COUNT (Id_check) OVER(PARTITION BY Age_Quarter_Groups.Id_client) AS ID_Count_Operations_All
	FROM Age_Quarter_Groups
	) a6
	ON (a1.Age_Group = a6.Age_Group)
ORDER BY Age_Group