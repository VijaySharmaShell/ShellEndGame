create proc ups_Populate_CurveVersion_Curve_LIBOR
as 
begin
	set nocount off

	    Create table #E05_Libor_Shell_Curve  (settle_date date,IRC_CURVE_NAME varchar(100),CM_CONTRACT_MONTH int,INTEREST_RATE decimal(18,5))

        insert into #E05_Libor_Shell_Curve(settle_date,IRC_CURVE_NAME,CM_CONTRACT_MONTH,INTEREST_RATE)
        select distinct settle_date,IRC_CURVE_NAME,CM_CONTRACT_MONTH,INTEREST_RATE 
        from   [MP2-RISKSQL].[Curves].[dbo].E05_Libor_Shell_Curve with(nolock)where CM_CONTRACT_MONTH is not null
	
	      --Inserting data into CurveVersion Table 
	      INSERT INTO CurveVersion  (AsOfDate,VersionNotes,CreatedBy,CurveVersionStatuscode)	      
	      Select DATEADD (second,DATEDIFF(second,GETDATE(), GETUTCDATE()), cast(A.settle_date as DateTime)),null,
		  'Sena', 'Active' 
          From 
            (
               select Distinct settle_date
               From  #E05_Libor_Shell_Curve with(nolock)
            ) A  where settle_date is not null order by settle_date
	       

	--Inserting data into curve table 

       INSERT INTO Curve(CurveHeaderId,CurveVersionId,IntervalDate,Interval,StartDate,EndDate,value,IsDaylightSavings,createdBy)	  
		  Select distinct 
		       chh.CurveHeaderId,
		       cv.curveversionid,
		        CONVERT(VARCHAR(10), CONVERT(DATE, cast(cm_contract_month as varchar(200)) + '01'), 120) ,
		  	  1,
		  	  CONVERT(VARCHAR(10), CONVERT(DATE, cast(cm_contract_month as varchar(200)) + '01'), 120),
		  	  convert(datetime,EOMONTH(CONVERT(VARCHAR(10), CONVERT(DATE,cast(cm_contract_month as varchar(200))+ '01'), 120))),
		  	  Hrc.INTEREST_RATE,
		  	  null,'Ko Oudomvilay'
		  From CurveHeaderReferencetable ch with(nolock) inner join curveHeader chh on ch.CurveRepo_CurveHeader=chh.CurveHeaderId inner join
			 #E05_Libor_Shell_Curve Hrc on ch.historical_Value_1=hrc.IRC_CURVE_NAME
		                         inner join CurveVersion cv on convert(date,Hrc.settle_date)=convert(date,cv.AsOfDate)
            order by settle_date
END
