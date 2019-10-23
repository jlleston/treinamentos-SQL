-->> consulta para avaliar a viabilidade de colunas para Segment Elimination
-->> script baseado na versão de Niko Neugebauer (www.nikoport.com)

declare @tabela bigint;

--set @tabela = OBJECT_ID('Fact.OrderHistoryExtended');

with segment
as
	(
		select  part.object_id, part.partition_number, seg.partition_id, seg.column_id, cols.name as ColumnName, tp.name as ColumnType,
			seg.segment_id, 
			isnull(min(seg.max_data_id - filteredSeg.min_data_id),-1)   as SegmentDifference
		from sys.column_store_segments seg
			inner join sys.partitions part
				on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
			inner join sys.columns cols
				on part.object_id = cols.object_id and seg.column_id = cols.column_id
			inner join sys.types tp
				on cols.system_type_id = tp.system_type_id and cols.user_type_id = tp.user_type_id
			outer apply 
				(select * from sys.column_store_segments otherSeg
					where seg.hobt_id = otherSeg.hobt_id and seg.partition_id = otherSeg.partition_id 
						and seg.column_id = otherSeg.column_id and seg.segment_id <> otherSeg.segment_id
						and ((seg.min_data_id < otherSeg.min_data_id and seg.max_data_id > otherSeg.min_data_id )  -- Scenario 1 
							or 
							(seg.min_data_id < otherSeg.max_data_id and seg.max_data_id > otherSeg.max_data_id ) -- Scenario 2 
							) ) filteredSeg
		group by part.object_id, part.partition_number, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id
	)
	select
			object_name(object_id) as TableName,
			partition_number,
			s.column_id,
			s.ColumnName, 
			s.ColumnType,
			(case s.ColumnType	when 'numeric' then 'Segment Elimination is not supported' 
								when 'datetimeoffset' then 'Segment Elimination is not supported' 
								when 'char' then 'Segment Elimination is not supported' 
								when 'nchar' then 'Segment Elimination is not supported' 
								when 'varchar' then 'Segment Elimination is not supported' 
								when 'nvarchar' then 'Segment Elimination is not supported' 
								when 'sysname' then 'Segment Elimination is not supported' 
								when 'binary' then 'Segment Elimination is not supported' 
								when 'varbinary' then 'Segment Elimination is not supported' 
								when 'uniqueidentifier' then 'Segment Elimination is not supported' 
								else 'OK'
			 end) as TypeSupport,
			sum(case when SegmentDifference > 0 then 1 else 0 end) as DealignedSegments,
			count(*) as TotalSegments,
			cast( sum(case when SegmentDifference > 0 then 1 else 0 end) * 100.0 / (count(*)) as Decimal(6,2)) as SegmentDealignment
	from segment s
	where object_id = @tabela or @tabela is null
	group by object_name(object_id), partition_number, s.column_id, s.ColumnName, s.ColumnType
	order by object_name(object_id), partition_number, s.column_id;
	GO
