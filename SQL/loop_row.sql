declare @loop_row INT
declare @row_count INT
declare @loop_amount INT
set @loop_row=0
set @loop_amount=(select count(count_atc_duplicate.molecule) from
(select distinct molecule,count(distinct atc) as b from dbo.[BK-ATC-Result]
group by molecule
having count(distinct atc)>=2) as count_atc_duplicate)
declare @mol varchar(max)
 while @loop_row<@loop_amount

begin
set @mol=(select distinct molecule from dbo.[BK-ATC-Result]
group by molecule
having count(distinct atc)>=2
order by molecule
offset 0 rows
fetch next 1 rows only)

update dbo.[BK-ATC-Result]
 
set atc=(select top 1 atc from dbo.[BK-ATC-Result]
where molecule=@mol
group by atc
order by count(atc) desc)
where molecule=@mol
set @loop_row=@loop_row+1
end;
