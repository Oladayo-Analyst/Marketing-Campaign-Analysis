create database Marketing_Database;
use Marketing_Database;
Create table Marketing_dataset1(ID INT Primary key,Year_Birth text,Education text,Marital_Status text,Income text,KidHome INT,TeenHome INT,Dt_Customer text,Recency INT,MntWines INT,MntFruits INT,	
MntMeatProducts INT,MntFishProducts INT,MntSweetProducts INT,MntGoldProds INT,NumDealsPurchases INT,NumWebPurchases INT,NumCatalogPurchases INT,NumStorePurchases INT,
NumWebVisitsMonth INT,AcceptedCmp3 INT,AcceptedCmp4 INT,AcceptedCmp5 INT,AcceptedCmp1 INT,AcceptedCmp2 INT,Response INT,Complain INT,Country text);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Marketing_dataset.csv"
  INTO TABLE Marketing_dataset1
  FIELDS TERMINATED BY ","
  ENCLOSED BY '"' 
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
select ID,Education,Income,row_number() over(partition by ID order by ID desc) as duplicate_row from marketing_dataset1;                    
select  * from marketing_dataset1 where income is null;

  select * from marketing_dataset1;
  SET SQL_SAFE_UPDATES = 0;
update Marketing_dataset1
set `Dt_Customer` = str_to_date(Dt_Customer,"%m/%d/%Y");
alter table marketing_dataset1
modify column Dt_Customer Date;

alter table marketing_dataset1
modify column Year_Birth INT;

alter table marketing_dataset1
add column Current_Year text;

update marketing_dataset1
set Current_Year = (select curdate() as current_Year);
select ((select max(Registered_date) as PresentYear from marketing_dataset1) - year_birth) as Age from marketing_dataset1;

alter table marketing_dataset1
add column Age text;
update marketing_dataset1
set Age =  ((select max(Registered_date) as PresentYear) - year_birth);
alter table marketing_dataset1
drop column Current_Year;

alter table marketing_dataset1
drop column present_Year;
alter table marketing_dataset1
drop column Age;

update marketing_dataset1
  Set Education = "2nd Level"
  where Education = "2n Cycle";
  
update marketing_dataset1
set Education = "Graduates"
where Education = "Graduation";
Select distinct(Education) from marketing_dataset1;

alter table marketing_dataset1
add column Generation_Age text;
update marketing_dataset1
set Generation_Age = (select case when Age <= 22 then "Generation Z"
     when Age <= 38 then "Millennial"
     when Age <= 54 then "Generation X"
     when Age <= 73 then "Boomers"
     else "Silent Generation"
     End);
     
alter table marketing_dataset1
add column Total_AmountSpent text;
update marketing_dataset1
set Total_AmountSpent = (select (MntWines+MntFruits+MntMeatProducts+MntFishProducts+MntSweetProducts+MntGoldProds)
                              as TotalMoneySpent);
alter table marketing_dataset1
add column TotalNoAcceptedCpn int;
update marketing_dataset1
set TotalNoAcceptedCpn =   AcceptedCmp1+AcceptedCmp2+AcceptedCmp3+AcceptedCmp4+AcceptedCmp5;                         

alter table marketing_dataset1
add column Child_Status text;
update marketing_dataset1
set Child_Status = (select case when No_of_Children >= 1 then "Have Child"
                    else "Does Not Have Child"
                    End);
alter table marketing_dataset1
add column No_of_Children INT;
update marketing_dataset1
set No_of_Children = (KidHome + TeenHome);

alter table marketing_dataset1
add column Total_Number_of_purchase int;
update marketing_dataset1
set Total_Number_of_purchase = (NumWebPurchases + NumCatalogPurchases + NumStorePurchases);

alter table marketing_dataset1
add column Education_background text;
update marketing_dataset1
set Education_background = (select case when Education in ("Basic","2nd level") then "Undergraduate"
                                when Education in ("Master","PhD") then "Post-Graduate"
                                else "Graduates"
                                End);
alter table marketing_dataset1
add column Marital_group text;
update marketing_dataset1
set Marital_group = (select case when Marital_Status in ("married","together") then "Married"
                    when Marital_status in ("single","widow","divorced","alone") then "Single"
                    else "Complicated"
                    End);
alter table marketing_dataset1
add column Registered_date text;
update marketing_dataset1
set Registered_date = year(Dt_Customer);   
               
update marketing_dataset1
set income = replace(income,"$"," ");

-- ADVANCED QUERYING
-- Total Income
select concat("$",sum(income)) as Total_Income from marketing_dataset1;
-- Average Income Per Customer
select concat("$",round(avg(income),2))as Average_income_per_customer from marketing_dataset1;

-- CUSTOMER DEMOGRAPHY INFORMATION
-- Number of Customer
select count(ID) as Number_of_Customer from marketing_dataset1;
-- Number of Customer by Education background 
select Education_background,count(ID) as EducationClass from marketing_dataset1
group by Education_background;
-- Number of Customer by Marital Status
select Marital_group,count(ID) as MaritalClass from marketing_dataset1
group by Marital_group;
-- Number of Customer by Generation
select Generation_Age,count(ID) as Number_Per_Generation from marketing_dataset1
group by Generation_Age; 
-- Total Money spent per Education background
select Education_background,sum(Total_AmountSpent) as Amount_Spent from marketing_dataset1
group by Education_background;
-- Total Money Spent by Country
select country,sum(Total_AmountSpent) as MoneySpent from marketing_dataset1
group by country
order by MoneySpent desc;
-- Total Spending by Generation
select Generation_Age,sum(Total_AmountSpent) as MoneySpent from marketing_dataset1
group by Generation_Age
order by MoneySpent desc;
-- Total Spending by Child Status
select Child_status,sum(Total_AmountSpent) as MoneySpent from marketing_dataset1
group by Child_Status
order by MoneySpent desc;
-- Purchase Frequency by country
select country,count(Total_Number_of_purchase) as purchaseFrequency from marketing_dataset1
group by country
order by purchaseFrequency desc;

-- PURCHASING CHANNEL 
-- Education
select Education_background,count(NumWebPurchases) as WebPurchase from marketing_dataset1
group by Education_background;
-- Country
select country,count(NumWebPurchases) as WebPurchase from marketing_dataset1
group by country
order by WebPurchase desc;
-- Marital Status
select Marital_group,count(NumWebPurchases) as WebPurchase from marketing_dataset1
group by Marital_group
order by WebPurchase desc;
-- Total Web Visit/ Registered Month
select Registered_date,sum(NumWebVisitsMonth) as TotalWebVisit from marketing_dataset1
group by Registered_date
order by Registered_date asc;

-- CAMPAIGN  ACCEPTANCE INFORMATION
-- Acceptance by Marital Status
select Marital_group,count(TotalNoAcceptedCpn) as AcceptedCampaign  from marketing_dataset1
group by Marital_group
order by AcceptedCampaign desc;
-- Acceptance by Education
select Education_background,count(TotalNoAcceptedCpn) as AcceptedCampaign  from marketing_dataset1
group by Education_background
order by AcceptedCampaign desc;
-- Acceptance by Child Status
select Child_Status,count(TotalNoAcceptedCpn) as AcceptedCampaign  from marketing_dataset1
group by Child_Status
order by AcceptedCampaign desc;
-- Total Response by Marital Status
select Marital_group,sum(Response) as TotalResponse from marketing_dataset1
group by Marital_group
order by TotalResponse desc;
-- Total Response by Education background
select Education_background,sum(Response) as TotalResponse from marketing_dataset1
group by Education_background
order by TotalResponse desc;
-- Total Response by Generation
select Generation_Age,sum(Response) as TotalResponse from marketing_dataset1
group by Generation_Age
order by TotalResponse desc;
-- Total Complain by Marital Status
select Marital_group,sum(Complain) as TotalComplain from marketing_dataset1
group by Marital_group
order by TotalResponse desc;
-- Total Complain by Education background
select Education_background,sum(Complain) as TotalComplain from marketing_dataset1
group by Education_background
order by TotalResponse desc;
-- Total Complain by Generation
select Generation_Age,sum(Complain) as TotalComplain from marketing_dataset1
group by Generation_Age
order by TotalResponse desc;

-- Write queries to calculate the response rates for each campaign
-- Campaign 1 rate
with CTE as (select sum(AcceptedCmp1) as TotalAcceptedCmp1 from marketing_dataset1)
 select round(TotalAcceptedCmp1/(select sum(TotalNoAcceptedCpn) from marketing_dataset1),2) as Rate_C1 from CTE;
 -- Campaign 2 rate
 with CTE as (select sum(AcceptedCmp2) as TotalAcceptedCmp2 from marketing_dataset1)
 select round(TotalAcceptedCmp2/(select sum(TotalNoAcceptedCpn) from marketing_dataset1),2) as Rate_C2 from CTE;
 -- Campaign 3 rate
 with CTE as (select sum(AcceptedCmp3) as TotalAcceptedCmp3 from marketing_dataset1)
 select round(TotalAcceptedCmp3/(select sum(TotalNoAcceptedCpn) from marketing_dataset1),2) as Rate_C3 from CTE;
 -- Campaign 4 rate
 with CTE as (select sum(AcceptedCmp4) as TotalAcceptedCmp4 from marketing_dataset1)
 select round(TotalAcceptedCmp4/(select sum(TotalNoAcceptedCpn) from marketing_dataset1),2) as Rate_C4 from CTE;
 -- Campaign 5 rate
 with CTE as (select sum(AcceptedCmp5) as TotalAcceptedCmp5 from marketing_dataset1)
 select round(TotalAcceptedCmp5/(select sum(TotalNoAcceptedCpn) from marketing_dataset1),2) as Rate_C5 from CTE;

-- Identify customers who have accepted multiple campaigns and analyze their behavior
select ID,TotalNoAcceptedCpn from marketing_dataset1
where TotalNoAcceptedCpn > 1;

-- RECENCY,FREQUENCY AND MONETARY ANALYSIS
create table RFM_Table as select * from marketing_dataset1;
select * from RFM_Table;
alter table RFM_Table
rename column `Total_Number_of_purchase` to `Frequency`;
alter table RFM_Table
add column MonetaryRank int;
-- update rfm_table
-- set MonetaryRank = (select ntile(5) over (order by Monetary asc) as MonetaryRank);
-- update rfm_table
-- set RecencyRank = (select ntile(5) over (order by Recency desc) as RecencyRank);
-- update rfm_table
-- set FrequencyRank = (select Id,Frequency,ntile(5) over (order by Frequency asc));
-- alter table rfm_table
-- drop column MonetaryRank;
alter table rfm_table
modify column monetary int;
-- Calculating RFM Rank
select * from rfm_table;
With RFM as (select ID,Recency,Frequency,Monetary from rfm_table)
select *,ntile(3) over (order by Recency desc) as rfm_recency,
         ntile(3) over (order by Frequency asc) as rfm_frequency,
         ntile(3) over (order by monetary asc) as rfm_monetary from RFM;
         
-- Calculate RFM SCORE
With RFM as (select ID,Recency,Frequency,Monetary from rfm_table group by ID,Recency,Frequency,Monetary),
rfm_calculation as (select *,ntile(3) over (order by Recency) as rfm_recency,
                    ntile(3) over (order by Frequency asc) as rfm_frequency,
                    ntile(3) over (order by monetary asc) as rfm_monetary from rfm)
select *,rfm_recency + rfm_frequency + rfm_monetary as rfm_score, concat(rfm_recency,rfm_frequency,rfm_monetary)as rfm from rfm_calculation;

-- CUSTOMER SEGMENTATION(- Write queries to calculate recency, frequency, and monetary value for each customer.)
select *, 
         Case when rfm in (311,312,311) then "New Customers"
              when rfm in (111,121,131,122,133,113,112,132) then "Lost Customers"
              when rfm in (212,313,123,221,211,232) then "Regular Customers"
              when rfm in (223,222,213,322,231,321,331) then "Loyal Customers"
              when rfm in (333,332,323,233) then "Champion Customers"
		End as RFM_SEGMENT
from(with rfm as (select id,recency,frequency,monetary from rfm_table),rfm_calculation as (select *,ntile(3) over (order by Recency) as rfm_recency,
                    ntile(3) over (order by Frequency asc) as rfm_frequency,ntile(3) over (order by monetary asc) as rfm_monetary from rfm)
                    select *,rfm_recency + rfm_frequency + rfm_monetary as rfm_score,concat(rfm_recency,rfm_frequency,rfm_monetary) as rfm from rfm_calculation) as rfm_derived;
  
  -- Perform an RFM analysis to identify high-value customers.
WITH rfm AS (
    SELECT id, recency, frequency, monetary
    FROM rfm_table
),
rfm_calculation AS (
    SELECT *, 
           ntile(3) OVER (ORDER BY recency) AS rfm_recency,
           ntile(3) OVER (ORDER BY frequency ASC) AS rfm_frequency,
           ntile(3) OVER (ORDER BY monetary ASC) AS rfm_monetary
    FROM rfm
),
rfm_derived AS (
    SELECT *,
           rfm_recency + rfm_frequency + rfm_monetary AS rfm_score,
           CONCAT(rfm_recency, rfm_frequency, rfm_monetary) AS rfm
    FROM rfm_calculation
)
SELECT 
    CASE 
        WHEN rfm IN ('311', '312', '311') THEN 'New Customers'
        WHEN rfm IN ('111', '121', '131', '122', '133', '113', '112', '132') THEN 'Lost Customers'
        WHEN rfm IN ('212', '313', '123', '221', '211', '232') THEN 'Regular Customers'
        WHEN rfm IN ('223', '222', '213', '322', '231', '321', '331') THEN 'Loyal Customers'
        WHEN rfm IN ('333', '332', '323', '233') THEN 'Champion Customers'
    END AS RFM_Segment,
    COUNT(id) AS Number_of_Customers
FROM rfm_derived
GROUP BY RFM_Segment;

select * from marketing_dataset1;










  