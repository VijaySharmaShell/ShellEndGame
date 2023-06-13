alter procedure usp_setNymexData
(
	@xml as xml
)
as
begin
	set nocount on

	if exists (select 1 from NYMex_Curve_StageCurrent)
	begin
		truncate table NYMex_Curve_StageCurrent
	end

	insert into dbo.NYMex_Curve_StageCurrent (
			[NYMex_Curve]
		
		)
		select distinct x.v.value('NymexCurve[1]','decimal(18,3)') 
		from @xml.nodes('/root/row') x(v)
			
--exec usp_setNymexData '<root><row><NymexCurve>1.234</NymexCurve></row><row><NymexCurve>2.345</NymexCurve></row></root>'
end



