use  `ELECTRONICA-DW`;

###Present total sales of all products supplied by each supplier with respect to quarter and
##month using drill down concept.
##only performed month wise
##moving from higher level deatil to lower level details
SELECT SupplierDimension.SupplierID, SupplierDimension.SupplierName,Timee.year,Timee.month,SUM(SalesFact.Revenue) AS TotalSales
FROM SalesFact INNER JOIN Timee ON SalesFact.OrderID = Timee.OrderID
INNER JOIN SupplierDimension  ON SalesFact.SupplierID = SupplierDimension.SupplierID AND SalesFact.OrderID = SupplierDimension.OrderID
GROUP BY SupplierDimension.SupplierID, SupplierDimension.SupplierName,Timee.year,Timee.month
ORDER BY SupplierDimension.SupplierID, Timee.year,Timee.month;


##Find total sales of product with respect to month using feature of rollup on month and
##feature of dicing on supplier with name "DJI" and Year as "2019". You will use the
##grouping sets feature to achieve rollup. Your output should be sequentially ordered
##according to product and month.
SELECT ProductDimension.ProductName,Timee.month,SUM(SalesFact.Revenue) AS TotalSales
FROM SalesFact INNER JOIN ProductDimension ON SalesFact.ProductID = ProductDimension.ProductID
INNER JOIN Timee  ON SalesFact.OrderID = Timee.OrderID
INNER JOIN SupplierDimension  ON SalesFact.SupplierID = SupplierDimension.SupplierID
WHERE SupplierDimension.SupplierName = 'DJI' AND Timee.year = 2019
GROUP BY ProductDimension.ProductName, Timee.month WITH ROLLUP
ORDER BY ProductDimension.ProductName ASC, Timee.month ASC;

####Create a materialized view named "CUSTOMER_STORE_SALES_MV" that presents the
##monthly sales analysis for each store and then customers wise.
CREATE VIEW CUSTOMER_STORE_SALES_MV AS SELECT StoreDimension.StoreID,
CustomerDimension.CustomerID,
YEAR(SalesFact.datee) AS SalesYear,
MONTH(SalesFact.datee) AS SalesMonth,
SUM(SalesFact.QuantityOrdered) AS TotalQuantityOrdered,
SUM(SalesFact.Revenue) AS TotalRevenue
FROM SalesFact JOIN StoreDimension ON SalesFact.StoreID = StoreDimension.StoreID
JOIN CustomerDimension ON SalesFact.CustomerID = CustomerDimension.CustomerID
GROUP BY StoreDimension.StoreID, CustomerDimension.CustomerID, SalesYear, SalesMonth;

select* from CUSTOMER_STORE_SALES_MV;


###Identify the top 5 customers with the highest total sales in 2019, considering the number
###of unique products they purchased.
SELECT SalesFact.CustomerID, CustomerDimension.CustomerName,
COUNT(DISTINCT SalesFact.ProductID) AS UniqueProductsPurchased,
SUM(SalesFact.Revenue) AS TotalSales
FROM SalesFact JOIN CustomerDimension ON SalesFact.CustomerID = CustomerDimension.CustomerID
WHERE YEAR(SalesFact.datee) = 2019
GROUP BY SalesFact.CustomerID, CustomerDimension.CustomerName
ORDER BY TotalSales DESC
LIMIT 5;

####Use the concept of Slicing calculate the total sales for the store “Tech Haven”and product
##combination over the months.
select month(SalesFact.datee) as SalesMonth,year(SalesFact.datee) as SalesYear,sum(SalesFact.Revenue) as TotalSales
from SalesFact join StoreDimension on SalesFact.StoreID = StoreDimension.StoreID
join ProductDimension on SalesFact.ProductID = ProductDimension.ProductID
where StoreDimension.StoreName = 'Tech Haven'
group by SalesYear, SalesMonth
order by SalesYear, SalesMonth;

###Find the 5 most popular products sold over the weekends.
###it came null as my data has no 7th day :(
SELECT SalesFact.ProductID, ProductDimension.ProductName, COUNT(SalesFact.Revenue) AS TotalSales
FROM SalesFact JOIN ProductDimension  ON SalesFact.ProductID = ProductDimension.ProductID
WHERE DAYOFWEEK(SalesFact.datee) = 7
GROUP BY SalesFact.ProductID, ProductDimension.ProductName
ORDER BY TotalSales DESC
LIMIT 5;


##Create a materialised view with the name “STOREANALYSIS_MV” that presents the
##product-wise sales analysis for each store.
create view STOREANALYSIS_MV as select StoreID, ProductID, sum(QuantityOrdered) as STORE_TOTAL from SalesFact
group by StoreID, ProductID;
select* from STOREANALYSIS_MV;

####Create a materialized view named "SUPPLIER_PERFORMANCE_MV" that presents the
###monthly performance of each supplier.
create view SUPPLIER_PERFORMANCE_MV as
select SupplierDimension.SupplierID,year(SalesFact.datee) as SalesYear,month(SalesFact.datee) as SalesMonth,sum(SalesFact.Revenue) as TotalRevenue
from SalesFact join SupplierDimension on SalesFact.SupplierID = SupplierDimension.SupplierID
group by SupplierDimension.SupplierID, SalesYear, SalesMonth;
select* from SUPPLIER_PERFORMANCE_MV;


