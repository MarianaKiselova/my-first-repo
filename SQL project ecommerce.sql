--STEP 1
--1.Creating a table for data on all instalment purchase contracts from 01.01.2018 to 31.01.2019:

CREATE TABLE installment_plan
(contract_number int NOT NULL,
client_id int NOT NULL,
phone_id int NOT NULL,
color_id tinyint NOT NULL,
merchant_id tinyint NOT NULL,
price numeric(10, 2) NULL,
date_purch date NULL,
qu_inst int NOT NULL,
inst int NULL)

SELECT COUNT(DISTINCT CONCAT(merchant_id, '-', contract_number)) AS 'Number of Customers' FROM installment_plan 
WHERE date_purch BETWEEN '2018-01-01' AND '2020-04-30'


--2. Creating a table for data on payments under these contracts:
CREATE TABLE payments
(merchant_id tinyint NOT NULL,
contract_number int NOT NULL,
date_payment date NULL,
payment int NULL)

SELECT * FROM payments


--STEP 2
-- Importing data from both tables in EXCEL format (via P_STORE > TASKS > IMPORT_DATA...)

--STEP 3: REPORT DEVELOPMENT:
--I.1. SQL query that selects data on the instalment contract:


SELECT
    ip.merchant_id AS "Merchant ID",
    ip.contract_number AS "Number of Instalment Contract",
    m.merchant_name AS "Merchant Name",
   c.client_name AS "Customer Name",
    b.brand_name AS "Brand Name",
    ph.phone_name AS "Phone Name",
    col.color_name AS "Colour",
    ip.qu_inst AS "Number of Months according to Instalment Contract",
    ip.inst AS "Monthly Instalment Sum in UAH",
    ip.date_purch AS "Date of Purchase",
	CASE
        WHEN DATEDIFF(month,ip.date_purch, '2020-04-30') >= ip.qu_inst THEN ip.qu_inst
		ELSE DATEDIFF(month,ip.date_purch, '2020-04-30')+1 END
	AS "Number of monthly payments due by the last day of the reporting month",
    
	ip.inst *(CASE
        WHEN DATEDIFF(month,ip.date_purch, '2020-04-30') >= ip.qu_inst THEN ip.qu_inst
		ELSE DATEDIFF(month,ip.date_purch, '2020-04-30')+1 END) AS " Sum (in UAH) of monthly instalments due by the last day of the reporting month"
FROM
    installment_plan ip
JOIN
    merchants m ON ip.merchant_id = m.merchant_id
JOIN
    clients c ON ip.client_id = c.client_id
JOIN
    phones ph ON ip.phone_id = ph.phone_id
JOIN
    brands b ON ph.brand_id = b.brand_id
JOIN
    colors col ON ip.color_id = col.color_id

	WHERE m.merchant_id = 67 AND ip.contract_number = 227 --for contract №Т67/227

	--WHERE m.merchant_id = 84 AND ip.contract_number = 228 -- for contract №Т84/228

	--WHERE m.merchant_id = 44 AND ip.contract_number = 1229 --for contract №Т44/1229;


--I.2.  SQL-query that selects data about instalment payments:

WITH AllContracts AS (
    SELECT DISTINCT merchant_id, contract_number, qu_inst, date_purch
    FROM installment_plan
),
AllMonths AS (
    SELECT 0 AS month_offset UNION ALL
    SELECT 1 AS month_offset UNION ALL
    SELECT 2 AS month_offset UNION ALL
    SELECT 3 AS month_offset UNION ALL
    SELECT 4 AS month_offset UNION ALL
    SELECT 5 AS month_offset UNION ALL
    SELECT 6 AS month_offset UNION ALL
    SELECT 7 AS month_offset UNION ALL
    SELECT 8 AS month_offset UNION ALL
    SELECT 9 AS month_offset UNION ALL
    SELECT 10 AS month_offset UNION ALL
    SELECT 11 AS month_offset 
),
DuePaymentDates AS (
    SELECT
        ac.merchant_id,
        ac.contract_number,
       DATEADD(MONTH, am.month_offset, DATEADD(DAY, -DAY(ac.date_purch) + 1, ac.date_purch)) AS due_payment_date
    FROM
        AllContracts ac
    JOIN
        AllMonths am ON ac.qu_inst >= am.month_offset 
                      
),
NumberedPayments AS (
    SELECT
        d.*,
        ip.inst AS monthly_installment,
		ip.date_purch,
		ip.qu_inst,
        p.date_payment,
        p.payment,
        ROW_NUMBER() OVER (PARTITION BY d.merchant_id, d.contract_number, DATEPART(yy, d.due_payment_date), DATEPART(mm, d.due_payment_date) ORDER BY p.date_payment) AS row_num
    FROM
        DuePaymentDates d
    LEFT JOIN
        payments p ON d.merchant_id = p.merchant_id 
                    AND d.contract_number = p.contract_number
                    AND p.date_payment >= d.due_payment_date
                    AND p.date_payment < DATEADD(MONTH, 1, d.due_payment_date)
                    
    JOIN
        installment_plan ip ON d.merchant_id = ip.merchant_id AND d.contract_number = ip.contract_number
    WHERE
	d.merchant_id = 67 AND d.contract_number = 227  -- for contract №Т67/227

	--d.merchant_id = 84 AND d.contract_number = 228 -- for contract №Т84/228

	--d.merchant_id = 44 AND d.contract_number = 1229-- for contract №Т44/1229
	AND d.due_payment_date < '2020-04-30'
	
)
SELECT
    np.merchant_id AS "Merchant ID",
    np.contract_number AS "Instalment contract number",
    DATEPART(yy, np.due_payment_date) AS "Year",
    DATEPART(mm, np.due_payment_date) AS "Month of the Instalment Payment due to the Contract",
    CASE WHEN np.row_num = 1 AND DATEPART(mm,np.date_purch)+np.qu_inst-1>=DATEPART(mm, np.due_payment_date) THEN np.monthly_installment ELSE 0 END AS "Monthly Instalment Amount in UAH",
    np.date_payment AS "Customer's Payment Date",
    CASE WHEN np.payment IS NOT NULL THEN np.payment ELSE 0 END AS "Paid amount"
FROM
    NumberedPayments np
ORDER BY
    np.due_payment_date ASC, np.date_payment ASC;

	--	I.3.  SQL-query that selects summerized data about instalment payments:

SELECT
    ip.merchant_id AS "Merchant ID",
    ip.contract_number AS "Number of Instalment Contract",
    ip.inst * ip.qu_inst AS "Monthly instalment amount as of the End of the Reporting Month in UAH",
    SUM(p.payment) AS "Paid Amount",
    ip.inst * ip.qu_inst - SUM(p.payment) AS "Total remaining Instalment Balance",
    ip.inst *CASE
        WHEN DATEDIFF(month,ip.date_purch, '2020-04-30') >= ip.qu_inst THEN ip.qu_inst
		ELSE DATEDIFF(month,ip.date_purch, '2020-04-30')+1 END -SUM(p.payment)
	AS "Remaining Instalment Balance (Including Debt arising from underpayments and arrears)"
FROM
    installment_plan ip
LEFT JOIN
    payments p ON ip.merchant_id = p.merchant_id AND ip.contract_number = p.contract_number
	
	WHERE ip.merchant_id = 67 AND ip.contract_number = 227 -- for the contract №Т67/227

	-- WHERE ip.merchant_id = 84 AND ip.contract_number = 228 --for the contract №Т84/228

	-- WHERE ip.merchant_id = 44 AND ip.contract_number = 1229--for the contract №Т44/1229

GROUP BY
    ip.merchant_id, ip.contract_number, ip.inst, ip.qu_inst, ip.date_purch;



	--II. SQL Query for Extracting Summary Data for the Debt Report on All Installment Contracts

--To generate the final query for the report, we first create auxiliary temporary tables. This approach simplifies the query and improves execution speed.

-- Creating a temporary table with a list of all installment contracts based on the data from the installment_plan table (AllContracts CTE).
CREATE TABLE #TempAllContracts (
    client_code varchar (15), 
	merchant_id INT,
    contract_number INT,
    qu_inst INT,
	inst INT,
    date_purch DATE
);

-- Inserting data into the temporary table AllContracts CTE.
INSERT INTO #TempAllContracts (client_code, merchant_id, contract_number, qu_inst, inst, date_purch)
SELECT a.client_code, merchant_id, contract_number, qu_inst, inst, date_purch
FROM( 
SELECT DISTINCT  CONCAT(merchant_id, '-', contract_number) AS client_code, *
FROM installment_plan)a
GROUP BY a.client_code, merchant_id, contract_number, qu_inst, inst, date_purch;

SELECT * FROM #TempAllContracts 

DROP TABLE #TempAllContracts; -- Auxiliary queries for viewing the temporary table and resetting its data.

-- Creating a temporary table to generate a list of possible deferred payment months based on the qu_inst indicator (AllMonths CTE).
CREATE TABLE #TempAllMonths (
    month_offset INT
);

-- Inserting data into the temporary table AllMonths CTE
INSERT INTO #TempAllMonths (month_offset)
VALUES
    (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11);

SELECT * FROM #TempAllMonths 

DROP TABLE #TempAllMonths; -- Auxiliary queries for viewing the temporary table and resetting its data.

-- Creating a temporary table to generate a list of all due instalment payment dates  DuePaymentDates CTE
CREATE TABLE #TempDuePaymentDates (
	client_code varchar (15), 
	merchant_id INT,
    contract_number INT,
    qu_inst INT,
	inst INT,
    date_purch DATE,
    due_payment_date DATE
);

-- Inserting data into the temporary table DuePaymentDates CTE
INSERT INTO #TempDuePaymentDates (client_code, merchant_id, contract_number, qu_inst, inst, date_purch, due_payment_date)
SELECT
	ac.*,
    DATEADD(MONTH, am.month_offset, DATEADD(DAY, -DAY(ac.date_purch)+1, ac.date_purch)) AS due_payment_date
FROM #TempAllContracts ac
JOIN #TempAllMonths am ON (ac.qu_inst = 6 AND am.month_offset <= 5) OR
            (ac.qu_inst = 12 AND am.month_offset <= 11) 

SELECT * FROM #TempDuePaymentDates

DROP TABLE #TempDuePaymentDates;-- Auxiliary queries for viewing the temporary table and resetting its data.

-- Creating a temporary table to generate a list of contracts with a breakdown by payment overdue periods, total payments, and the expected amount due for such contracts NumberedPayments CTE
CREATE TABLE #TempNumberedPayments (
	client_code varchar (15), 
	merchant_id INT,
    contract_number INT,
    qu_inst INT,
	inst INT,
    date_purch DATE,
    due_payment_date DATE,
    date_payment DATE,
    payment INT,
	delay_months INT,
	total_duepayment INT
);

--Inserting data into the temporary table #TempNumberedPayments 
INSERT INTO #TempNumberedPayments (client_code, merchant_id, contract_number, qu_inst, inst, date_purch, due_payment_date, date_payment, payment,
delay_months,  total_duepayment)
SELECT
	d.client_code, d.merchant_id, d.contract_number, d.qu_inst, d.inst, d.date_purch, d.due_payment_date,
    p.date_payment,
    p.payment, 
    CASE WHEN MONTH(p.date_payment) IS NULL THEN COUNT(d.due_payment_date) ELSE 0 END AS delay_months,
	d.qu_inst*d.inst AS total_duepayment
    FROM #TempDuePaymentDates d
LEFT JOIN payments p ON d.merchant_id = p.merchant_id 
                    AND d.contract_number = p.contract_number
                    AND p.date_payment >= d.due_payment_date
                    AND p.date_payment < DATEADD(MONTH, 1, d.due_payment_date)
WHERE d.due_payment_date BETWEEN '2018-01-01' AND '2020-04-30'
GROUP BY d.client_code, d.merchant_id, d.contract_number, d.qu_inst, d.inst, d.date_purch, d.due_payment_date, p.date_payment, p.payment
ORDER BY d.client_code;

SELECT * FROM #TempNumberedPayments;

DROP TABLE #TempNumberedPayments;-- Auxiliary queries for viewing the temporary table and resetting its data.

SELECT * FROM #TempNumberedPayments
WHERE client_code='10-1' -- Auxiliary queries for selecting (visualizing) data for individual contracts from the temporary table.

-- Creating a temporary table to generate a list of clients (by contract numbers) based on the number of months of payment delays #TempDelayTotal.
CREATE TABLE #TempDelayTotal (
	client_code varchar (15), 
	total_delay INT,
	total_payment INT,
	debt_byclient INT
);

--Inserting data into the temporary table #TempDelayTotal
INSERT INTO #TempDelayTotal (client_code, total_delay, total_payment, debt_byclient)
SELECT client_code, 
SUM (delay_months) AS total_delay,
SUM(payment) AS total_payment,
total_duepayment-SUM (payment) AS debt_byclient
FROM #TempNumberedPayments
GROUP BY client_code, total_duepayment;

SELECT * FROM #TempDelayTotal
WHERE client_code='10-1'
ORDER BY client_code;

DROP TABLE #TempDelayTotal;-- Auxiliary queries for viewing the temporary table and resetting its data.

-- SQL query for displaying data for the Report:

SELECT sub.installment_period AS 'Instalment Period ',
	sub.debt AS 'Presence of Debt',
	SUM(sub.due_payment) AS 'Instalment Amount ',
	SUM(sub.report_duepay)AS 'Amount Due as of the End of the Reporting Month',
	SUM(sub.total_payment) AS 'Amount Paid as of the End of the Reporting Month',
	COUNT(sub.client_code) AS 'Number of Customers',
	--SUM(sub.report_duepay)- SUM(sub.total_payment) 
	SUM(sub.totaldebt) AS 'Outstanding Debt',
	SUM(sub.total_without_debt) AS 'Remaining Instalment Balance (Excluding Debt)',
	SUM(sub.nodelay_clients) AS 'Number of Customers with 0 Missed Monthly Payments',
	SUM (sub.clients_1monthdelay) AS 'Number of Customers with 1 Missed Monthly Payment',
	SUM(sub.clients_2monthdelay) AS '2 Missed Monthly Payments',
	SUM(sub.clients_3monthdelay) AS '3 Missed Monthly Payments',
	SUM(sub.clients_4monthdelay) AS '4 or More Missed Monthly Payments',
	SUM(sub.nodelay_totaldebt) AS 'Remaining Debt Amount for Customers with 0 Missed Monthly Payments',
	SUM (sub.delay1month_totaldebt) AS 'Remaining Debt Amount for Customers with 1 Missed Monthly Payment',
	SUM (sub.delay2month_totaldebt) AS 'Remaining Debt: 2 Missed Monthly Payments',
	SUM (sub.delay3month_totaldebt) AS 'Remaining Debt: 3 Missed Monthly Payments',
	SUM (sub.delay4month_totaldebt) AS 'Remaining Debt: 4 and more Missed Monthly Payments'
	FROM(
SELECT ac.client_code, ac.qu_inst*ac.inst AS due_payment,
CASE WHEN DATEDIFF(MONTH, ac.date_purch, '2020-04-30') < ac.qu_inst 
THEN DATEDIFF(MONTH, ac.date_purch, '2020-04-30') * ac.inst
        ELSE ac.qu_inst*ac.inst END AS report_duepay,
	IIF(DATEDIFF(MONTH, ac.date_purch, '2020-04-30') >= ac.qu_inst, 'Finished', 'Unfinished') AS installment_period,
	IIF(dt.total_payment< ac.qu_inst*ac.inst, 'Debt Remaining', 'No Debt Remaining') AS debt,
	dt.total_payment,
CASE WHEN DATEDIFF(MONTH, ac.date_purch, '2020-04-30') < ac.qu_inst 
THEN DATEDIFF(MONTH, DATEADD (mm, 1,'2020-04-30'),DATEADD(mm,ac.qu_inst, ac.date_purch))*ac.inst
ELSE 0 END AS total_without_debt,
	dt.total_delay,
	CASE WHEN dt.total_delay =0 THEN COUNT(ac.client_code) ELSE 0 END AS nodelay_clients,
	CASE WHEN dt.total_delay =1 THEN COUNT(ac.client_code) ELSE 0 END AS clients_1monthdelay,
	CASE WHEN dt.total_delay =2 THEN COUNT(ac.client_code) ELSE 0 END AS clients_2monthdelay,
	CASE WHEN dt.total_delay =3 THEN COUNT(ac.client_code) ELSE 0 END AS clients_3monthdelay,
	CASE WHEN dt.total_delay >=4 THEN COUNT(ac.client_code) ELSE 0 END AS clients_4monthdelay,
	CASE WHEN dt.total_delay =0 THEN SUM(dt.debt_byclient) ELSE 0 END AS nodelay_totaldebt,
	CASE WHEN dt.total_delay =1 THEN SUM(dt.debt_byclient) ELSE 0 END AS delay1month_totaldebt,
	CASE WHEN dt.total_delay =2 THEN SUM(dt.debt_byclient) ELSE 0 END AS delay2month_totaldebt,
	CASE WHEN dt.total_delay =3 THEN SUM(dt.debt_byclient) ELSE 0 END AS delay3month_totaldebt,
	CASE WHEN dt.total_delay >=4 THEN SUM(dt.debt_byclient) ELSE 0 END AS delay4month_totaldebt,
	SUM(dt.debt_byclient) AS totaldebt
	FROM #TempAllContracts ac
	LEFT JOIN #TempDelayTotal dt ON ac.client_code=dt.client_code
	GROUP BY ac.client_code, ac.qu_inst, ac.inst, ac.date_purch, dt.total_payment, dt.total_delay) sub
GROUP BY sub.installment_period, sub.debt
ORDER BY sub.installment_period, sub.debt;