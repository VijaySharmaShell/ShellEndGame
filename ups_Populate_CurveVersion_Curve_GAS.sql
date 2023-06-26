create proc ups_Populate_CurveVersion_Curve_GAS
as 
begin
	set nocount off

	    Create table #C05_Gas_Shell_Curve  (settle_date date,Indexcode varchar(100),CM_CONTRACT_MONTH int,PRICE decimal(18,5))

        insert into #C05_Gas_Shell_Curve(settle_date,Indexcode,CONTRACT_Date,PRICE)
        select distinct settle_date,Indexcode,CONTRACT_Date,PRICE 
        from   [MP2-RISKSQL].[Curves].[dbo].C05_Gas_Shell_Curve with(nolock)where contract_date is not null
	
		

	      --Inserting data into CurveVersion Table 
	      INSERT INTO CurveVersion  (AsOfDate,VersionNotes,CreatedBy,CurveVersionStatuscode)	      
	      Select DATEADD (second,DATEDIFF(second,GETDATE(), GETUTCDATE()), cast(A.settle_date as DateTime)),null,
		  'Sena', 'Active' 
          From 
            (
               select Distinct settle_date
               From  #C05_Gas_Shell_Curve with(nolock)
            ) A  where settle_date is not null order by settle_date
	       

	--Inserting data into curve table 

       INSERT INTO Curve(CurveHeaderId,CurveVersionId,IntervalDate,Interval,StartDate,EndDate,value,IsDaylightSavings,createdBy)	  
		  Select distinct 
		       chh.CurveHeaderId,
		       cv.curveversionid,
		       contract_date ,
		  	  1,
		  	  convert(datetime,contract_date),
		  	  convert(datetime,EOMONTH(CONVERT(VARCHAR(10), contract_date))),
		  	  Hrc.price,
		  	  null,'Ko Oudomvilay'
		  From CurveHeaderReferencetable ch with(nolock) inner join curveHeader chh on ch.CurveRepo_CurveHeader=chh.CurveHeaderId inner join
			 #C05_Gas_Shell_Curve Hrc on ch.historical_Value_1=hrc.indexcode
		                         inner join CurveVersion cv on convert(date,Hrc.settle_date)=convert(date,cv.AsOfDate)
            order by settle_date
END
