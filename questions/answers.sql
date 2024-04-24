/*2ai*/

with succuss as (
select 
	p.id
	,p.name
	,count(o.status) as num_times
	,dense_rank() over(order by count(o.status) desc) as rank
from alt_school.orders as o
inner join alt_school.line_items as l		on o.order_id=l.order_id
inner join alt_school.products as p		on p.id = l.item_id
where o.status = 'success'
group by p.id
	,p.name
) /*cte to show the orderd item based on count of success status and dense rank to order them in descending order */
select 
	id
	,name
	,num_times
from succuss
where rank = 1; /*This shows the id, name and number of time filtering with the item that comes 1st on the rank column*/


/*2aii*/

with flat as (
select 
	event_id
	,customer_id
	,(event_data ->> 'item_id')::bigint as "item_id",
	(event_data->> 'quantity')::bigint as "quantity"
	,event_data->> 'timestamp' as "timestamp"
	,event_data->> 'event_type' as "event_type"
	,event_timestamp
from alt_school.events
), /*To flatten the Jsonb datatype into different columns with Headers*/
result as (
select 
	o.customer_id
	,location
	,sum(quantity*p.price) as total_spend
	,dense_rank() over(order by sum(quantity*p.price) desc) as rank
from flat as f
inner join alt_school.orders as o		on f.customer_id=o.customer_id
inner join alt_school.products as p		on f.item_id = p.ID
inner join alt_school.customers as c	on c.customer_id=o.customer_id
where (item_id is not null and quantity is not null) and status = 'success'
group by o.customer_id
	,location
)  /*It ranks the top spenders by the product of quantity and unit price*/
select 
	customer_id
	,location
	,total_spend
from result
where rank between 1 and 5; /*It returns the customer id, location and total spend by filtering with the rank in the
								above cte*/


/*2bi*/

with Event as (
select 
	event_id
	,customer_id
	,(event_data ->> 'item_id')::bigint as "item_id",
	(event_data->> 'quantity')::bigint as "quantity"
	,event_data->> 'timestamp' as "timestamp"
	,event_data->> 'event_type' as "event_type"
	,event_timestamp
from alt_school.events
),  /*To flatten the Jsonb datatype into different columns with Headers*/
cc as (
select 
	location
	,count(Event.event_type) as checkout_count
	,dense_rank() over(order by count(Event.event_type) desc) as rank	
from Event
inner join alt_school.customers as c		on c.customer_id=Event.customer_id
inner join alt_school.orders as o			on o.customer_id=c.customer_id
where event_type = 'add_to_cart' and status = 'success'
group by location
) 	/*To count the success in order status and rank them in descending order*/
select 
	location
	,checkout_count
from cc
order by rank; 	/*Gives the result by ordering it with the rank defined above*/


/*2bii*/

with Event as (
select	
	event_id
	,customer_id
	,(event_data ->> 'item_id')::bigint as "item_id",
	(event_data->> 'quantity')::bigint as "quantity"
	,event_data->> 'timestamp' as "timestamp"
	,event_data->> 'event_type' as "event_type"
	,event_timestamp
from alt_school.events
) 	/*To flatten the Jsonb datatype into different columns with Headers*/
select 
	o.customer_id
	,count(event_type) as num_events	
from alt_school.orders as o
inner join Event			on Event.customer_id=o.customer_id
where (event_type <> 'visit' and event_type <> 'checkout') and status = 'cancelled'
group by o.customer_id
order by 2 desc; 	/*This returns the customer_id and the count of those that abandon their carts
						by filtering on event type and status*/


/*2biii*/

with Event as (
select 
	event_id
	,customer_id
	,(event_data ->> 'item_id')::bigint as "item_id",
	(event_data->> 'quantity')::bigint as "quantity"
	,event_data->> 'timestamp' as "timestamp"
	,event_data->> 'event_type' as "event_type"
	,event_timestamp
from alt_school.events 
) 			/*To flatten the Jsonb datatype into different columns with Headers*/
,visit as (
select 
	o.customer_id
	,sum(case 
		when event_type = 'visit' and status = 'cancelled' then 1
		when event_type = 'visit' and status = 'failed' then 1
		when event_type = 'visit' and status = 'success' then 1
		else 0
	end) as visit_count
	,status
from alt_school.orders as o
inner join Event			on Event.customer_id=o.customer_id
group by o.customer_id
		,status	
)	/*This cte returns the number of time customers visit before checking out, the event type and status was used to filter the result*/
select 
	cast(avg(visit_count) as decimal (10,2)) as average_visits
from visit;			/*This returns the average count customer visits before a successful checkout*/

