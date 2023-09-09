# json-to-straight-table-in-PostgreSQL
Unified method to parse JSON object into table


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
