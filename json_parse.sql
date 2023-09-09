
/*

To parse a JSON object of any structure and obtain a set of regular table records, just run a query using the sample as template:


select distinct	
	*
from
	--	standard PG function
	json_to_recordset (
		(
			select
				--	the function proposed here
				fn_convert_hierarchical_json_objects_to_array_of_objects (
					--	input JSON object including arrays and sub-objects 
					inp_json => '{"k1":1, "k2":[2,3], "k3":{"k31":"v1"}}'::json
				)
		)
	-- list of kets to be extracted into table records 	
	) as x("k1" int, "k2" int, "k3" text, "k31" text)
;


For details, see the comments to the function.

*/



create or replace function fn_convert_hierarchical_json_objects_to_array_of_objects (inp_json json)
returns 	json
language 	plpgsql
as 		$fn_convert_json_to_array_of_json_records$
---
/*
 * 
This function converts a hierarchical JSON object into an array of single-level objects and thereby allows you to present the data of the original object in the form of a flat table.

How it works:

1. 
Call the function: 
	select fn_convert_hierarchical_json_objects_to_array_of_simple_objects (inp_json => '{"k1":1, "k2":[2,3], "k3":{"k31":"v1"}}'::json);
returns: 
	[{"k1":"1","k2":"2","k3":"{\"k31\": \"v1\"}","k31":"v1"}, {"k1":"1","k2":"3","k3":"{\"k31\": \"v1\"}","k31":"v1"}]

2. 
In case the JSON input is complicated it could be useful to use two temporary tables created while the function execution.  

Request:
	select distinct * from tmp_tbl_data_with_json_analysis order by "iteration_num", "key", "value";
returns table:
---
"key" 	"value" 		"value_type" 	"rank_num" 	"row_num"
k1 	1 			number 		1 		1
k3 	{"k31": "v1"} 	object 			1 		4
k2 	2 			number 		2 		2
k2 	3 			number 		2 		3
k31 	"v1" 			string 		2 		5

Request:
	select * from tmp_tbl_with_data_from_json;
returns table:
---
k1	k2	k3		k31
1	2	{"k31": "v1"}	v1
1	3	{"k31": "v1"}	v1


3.
To convert the function return into records of some straight table
use request:
---
select distinct
	*
from
	json_to_recordset (
		(
			select
				fn_convert_hierarchical_json_objects_to_array_of_objects (
					inp_json => '{"k1":1, "k2":[2,3], "k3":{"k31":"v1"}}'::json
				)
		)
	) as x("k1" int, "k2" int, "k3" text, "k31" text)
;
which returns the table:
---
k1 	k2 	k3 		k31
1 	2 	{"k31": "v1"} 	v1
1 	3 	{"k31": "v1"} 	v1
*
*/
---
declare
	v_jsonb 			jsonb := inp_json::jsonb;
	---
	--	Set of variables to prepare dynemic request to assemple final return json array of records inside of json objects.
	v_tbl_rec 			record;																									
	v_counter  			integer := 	0;
	v_request			text	:= 	'create temp table tmp_tbl_with_data_from_json as ' 	|| 
									'select distinct * from ' 		|| 
									chr(10);
	v_request_order_by	text	:= 	' order by ';
	---
	--	Variable to keep final json before return from the function.
	v_execution_result	json;
begin
---
raise notice 'v_jsonb = %', v_jsonb;	
---
--	Let's go along with every branch of the json tree 
--	and insert result with fields like: "key" | "value" | "value_type" (json type) | "iteration_num"/"rank_num" | "row_num" (absolute),
--	into temporary table. 
--	The table might be useful for later (after the function execution) analyses. 
drop 		table if exists tmp_tbl_data_with_json_analysis;						 	 	
create  	table			tmp_tbl_data_with_json_analysis as	--	Create and feel in the table with key-data pairs from all branches of the JSON
---
with
recursive
cte_kv as (
	select 
		"key"			as "key",
		"value"			as "value",
		jsonb_typeof("value") 	as "value_type",
		1			as "iteration_num"
	from 
		jsonb_each(v_jsonb)
	---	
	union		
	---
	select 
		*
	from 
		(
		with 
		cte_iter_1 as (
			select cte_kv.* from cte_kv where cte_kv.value_type in ('array', 'object')	
		),
		cte_iter_11 as (
			select 
				cte_iter_1."key"				as "key",
				jsonb_array_elements(cte_iter_1."value") 	as "value",
				"iteration_num" + 1				as "iteration_num"
			from 
				cte_iter_1	
			where 
				cte_iter_1."value_type" in ('array')
		),
		cte_iter_12 as (
			select
				(jsonb_each(cte_iter_1."value"))."key"		as "key", 
				(jsonb_each(cte_iter_1."value"))."value"	as "value",
				"iteration_num" + 1				as "iteration_num"				
			from 
				cte_iter_1	
			where 
				cte_iter_1."value_type" in ('object')		
		),
		cte_iter_2 as (
			select cte_iter_11.* from cte_iter_11
			union
			select cte_iter_12.* from cte_iter_12
		)
			select 
				cte_iter_2."key"				as "key",
				cte_iter_2."value"				as "value",
				jsonb_typeof(cte_iter_2."value") 		as "value_type",
				"iteration_num"					as "iteration_num"
			from 
				cte_iter_2 
		) 
		tbl_iter			
)
--	
,cte11 as (
	select "key","value","value_type", "iteration_num" as "rank_num", row_number() OVER () as row_num 
	from ( select * from cte_kv where "value_type" not in ('array') 			
	order by "key", "value"	) 	tbl		--	
)
select * from cte11
;
---
------------------------------------------------------------------------------------
---
    FOR v_tbl_rec in select distinct "key" from tmp_tbl_data_with_json_analysis order by "key"
    loop
		v_counter := v_counter + 1;
		---
		v_request 			:= 	v_request 																				|| 
					case when v_counter > 1 then 'left join ' else '' end 												||
					'(select case when "value_type" = ''string'' then ("value"->>0)::text else "value"::text end  as "' || 
						v_tbl_rec."key" 																				|| 
						'" from tmp_tbl_data_with_json_analysis where "key" = ''' 										|| 
						v_tbl_rec."key" 																				|| 
						''') tbl_' 																						|| 
						v_tbl_rec."key"																					||
					case when v_counter > 1 then ' on 1=1' 	else '' end || 
					chr(10); 
		---		
		v_request_order_by 	:= v_request_order_by																		||
					case when v_counter > 1 then	', '		else ' '	end											||
					v_tbl_rec."key"
					;
  		---
	END LOOP;
	RAISE NOTICE 'v_request:          % ', v_request; 
	RAISE NOTICE 'v_request_order_by: % ', v_request_order_by;
	v_request 			:= 	v_request || v_request_order_by;
	RAISE NOTICE 'v_request:          % ', v_request;
---
drop 		table if exists tmp_tbl_with_data_from_json;
execute 	v_request;
---
------------------------------------------------------------------------------------
---
select 
	json_agg(row_to_json(tbl))  as "json_agg"
from 
	(
	SELECT 
		* 
	FROM 
		tmp_tbl_with_data_from_json
	) tbl
into v_execution_result;
---
return v_execution_result;
--
---
------------------------------------------------------------------------------------
---
end		$fn_convert_json_to_array_of_json_records$
;


