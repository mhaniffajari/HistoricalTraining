-- 1. Create script to create table for each object
-- a. Employee
CREATE TABLE [dbo].[Employee]
(
[Id] INT NOT NULL,
[EmployeeId] VARCHAR(10) NOT NULL,
[Fullname] VARCHAR(100) NOT NULL,
[Birthdate] DATE NOT NULL,
[Address] VARCHAR(500)
CONSTRAINT PK_Employee PRIMARY KEY ([Id]),
CONSTRAINT UK_Employee UNIQUE ([EmployeeId])
);
-- b. PositionHistory
CREATE TABLE [dbo].[PositionHistory]
(
[Id] INT NOT NULL,
[PosId] VARCHAR(10) NOT NULL,
[PosTitle] VARCHAR(100) NOT NULL,
[EmployeeId] VARCHAR(10) NOT NULL,
[StartDate] DATE NOT NULL,
[EndDate] DATE NOT NULL,
CONSTRAINT PK_PositionHistory PRIMARY KEY ([Id])
);

--2. Create insert script to inserting data into each table (Employee and PositionHistory)
--A. Employee
INSERT INTO [dbo].[Employee] ([Id],[EmployeeId],[Fullname],[Birthdate],[Address])
 VALUES (1,'10105001','Ali Anton','19-Aug-82','Jakarta Utara')

 INSERT INTO [dbo].[Employee] ([Id],[EmployeeId],[Fullname],[Birthdate],[Address])
 VALUES (2,'10105002' ,'Rara Siva','1-Jan-82','Mandalika')

 INSERT INTO [dbo].[Employee] ([Id],[EmployeeId],[Fullname],[Birthdate],[Address])
 VALUES (3,'10105003','Rin Aini','20-Feb-82','Sumbawa Besar')

  INSERT INTO [dbo].[Employee] ([Id],[EmployeeId],[Fullname],[Birthdate],[Address])
 VALUES (4,'10105004','Budi','22-Feb-82','Mataram Kota')
 --B. PositionHistory
 INSERT INTO [dbo].[PositionHistory]([Id],[PosId],[PosTitle],[EmployeeId],[StartDate],[EndDate])
 VALUES(1,'50000','IT Manager','10105001','1-Jan-2022','28-Feb-2022')

 INSERT INTO [dbo].[PositionHistory]([Id],[PosId],[PosTitle],[EmployeeId],[StartDate],[EndDate])
 VALUES(2,'50001','IT Sr. Manager','10105001','1-Mar-2022','31-Dec-2022')

  INSERT INTO [dbo].[PositionHistory]([Id],[PosId],[PosTitle],[EmployeeId],[StartDate],[EndDate])
 VALUES(3,'50002','Programmer Analyst','10105002','1-Jan-2022','28-Feb-2022')

  INSERT INTO [dbo].[PositionHistory]([Id],[PosId],[PosTitle],[EmployeeId],[StartDate],[EndDate])
 VALUES(4,'50003','Sr. Programmer Analyst','10105002','1-Mar-2022','31-Dec-2022')

   INSERT INTO [dbo].[PositionHistory]([Id],[PosId],[PosTitle],[EmployeeId],[StartDate],[EndDate])
 VALUES(5,'50004','IT Admin','10105003','1-Jan-2022','28-Feb-2022')

    INSERT INTO [dbo].[PositionHistory]([Id],[PosId],[PosTitle],[EmployeeId],[StartDate],[EndDate])
 VALUES(6,'50005','IT Secretary','10105003','1-Mar-2022','31-Dec-2022')

 -- 3 Create query to display all employee (EmployeeId, FullName, BirthDate, Address) data 
 --with their current position information (PosId, PosTitle, EmployeeId, StartDate, EndDate).
 WITH LastPosition 
 AS 
 (SELECT  EmployeeId,MAX(b.StartDate) AS StartDate 
 FROM [dbo].[PositionHistory] AS b
 GROUP BY EmployeeId)
 SELECT 
 b.EmployeeId,b.Fullname,b.Birthdate,b.Address,
 c.PosId,c.PosTitle,c.StartDate,c.EndDate
 FROM LastPosition AS a
RIGHT JOIN [dbo].[Employee] AS b
ON a.EmployeeId = b.EmployeeId
LEFT JOIN [dbo].[PositionHistory] AS c
 ON a.EmployeeId = c.EmployeeId AND a.StartDate = c.StartDate