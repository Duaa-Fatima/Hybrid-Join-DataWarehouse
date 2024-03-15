CREATE DATABASE `ELECTRONICA-DW`;
use  `ELECTRONICA-DW`;
drop table ProductDimension;
##INSERT INTO ProductDimension (ProductID, ProductName, ProductPrice, OrderID) VALUES (0, '0', 0, 0);
CREATE TABLE ProductDimension 
( 
    ProductID int ,
    ProductName varchar(255),
    ProductPrice double,
    OrderID int,
    PRIMARY KEY (ProductID, OrderID)
);
INSERT INTO ProductDimension(ProductID, ProductName, ProductPrice, OrderID)VALUES (?, ?, ?, ?);
CREATE TABLE StoreDimension 
(
    StoreID int ,
    StoreName varchar(255),
     OrderID int,
    PRIMARY KEY (StoreID, OrderID)
);
CREATE TABLE CustomerDimension 
(
    CustomerID int,
    CustomerName varchar(255),
    Gender varchar(10),
	OrderID int,
    PRIMARY KEY (CustomerID, OrderID)
);

CREATE TABLE SupplierDimension 
(
    SupplierID int ,
    SupplierName varchar(255),
    OrderID int,
    PRIMARY KEY (SupplierID, OrderID)
   
);

select* from ProductDimension;
select* from SalesFact ;
select* from Timee;
##drop table Timee;
CREATE table Timee
(
OrderID int ,
day int,
month int,
year int,
datee datetime,
PRIMARY KEY (day, OrderID)
);


##drop table SalesFact;
CREATE TABLE SalesFact (
    OrderID INT PRIMARY KEY,
    ProductID INT,
    timeID int,
    CustomerID INT,
    StoreID INT,
    SupplierID INT,
    QuantityOrdered INT,
    datee datetime,
    ProductPrice DECIMAL(10, 2),
    Revenue DECIMAL(10, 2) AS (QuantityOrdered * ProductPrice),
    Profit DECIMAL(10, 2) AS (Revenue - (QuantityOrdered * ProductPrice)),
    ProfitMargin DECIMAL(5, 2) AS (
	CASE WHEN Revenue > 0 THEN (Profit / Revenue) * 100 ELSE NULL END
    ),
    FOREIGN KEY (ProductID,OrderID) REFERENCES ProductDimension(ProductID, OrderID),
    FOREIGN KEY (timeID,OrderID) REFERENCES  Timee(day,OrderID),
    FOREIGN KEY (CustomerID,OrderID) REFERENCES CustomerDimension(CustomerID,OrderID),
    FOREIGN KEY (StoreID,OrderID) REFERENCES StoreDimension(StoreID,OrderID),
    FOREIGN KEY (SupplierID,OrderID) REFERENCES SupplierDimension(SupplierID,OrderID)
);






