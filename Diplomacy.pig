--load the orders
A = LOAD '/user/maria_dev/diplomacy/orders.csv'
	USING PigStorage(',')
    AS
    (game_id:chararray,
     unit_id:chararray,
     unit_order:chararray,
     location:chararray,
     target:chararray,
     target_dest:chararray,
     success:chararray,
     reason:chararray,
     turn_num:chararray);

/*Here we first filter all the orders that target Holland.
 Note '.*Holland.' doens't have the wildcard at the end to prevent it from picking up "Holland VIA CONVOY" */
filtered = FILTER A BY (target MATCHES '.*Holland.');
--Group by location and target so we can get the example output like "Adriatic Sea , Holland, 6"
grouped = GROUP filtered BY (location, target);
--Sort alphabetically by location and target
sort = ORDER grouped BY group ASC;
--For each group we count the number of orders
result = FOREACH sort GENERATE FLATTEN(group) as (location, target), COUNT($1);
--Store the result
STORE result INTO '/user/maria_dev/diplomacy/results';