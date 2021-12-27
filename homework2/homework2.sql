--1.
-------------------------------------------------------
select 
  c.name, 
  count(f.film_id) 
from film f 
  inner join film_category fc on f.film_id = fc.film_id 
  inner join category c on fc.category_id = c.category_id 
group by c.name
order by 2 desc;

--2. 
-------------------------------------------------------

select 
  concat(a.first_name, ' ', a.last_name),
  count(a.first_name)
from rental r 
  inner join inventory i on r.inventory_id = i.inventory_id
  inner join film_actor fa on i.film_id = fa.film_id 
  inner join actor a on fa.actor_id = a.actor_id 
group by a.first_name, a.last_name  
order by 2 desc
limit 10

--3. 
------------------------------------------------------------
  
select 
  fc.category_id ,
  sum(p.amount) 
from film_category fc 
  inner join inventory i on fc.film_id = i.film_id 
  inner join rental r on i.inventory_id = r.inventory_id 
  inner join payment p on r.rental_id = p.rental_id 	  
group by   fc.category_id
order by 2 desc 
limit 1

--4. 
------------------------------------------------------------

select f.title 
from film f
  left outer join inventory i on f.film_id = i.film_id 
where i.inventory_id is null 

--5. 
------------------------------------------------------------
select * from (
select 
  count(f.title) as count, 
  a.last_name as name,
  DENSE_RANK() OVER (ORDER BY count(f.title) desc) rank_number
from film f 
  inner join film_category fc on f.film_id = fc.film_id 
  inner join category c on fc.category_id = c.category_id 
  inner join film_actor fa on f.film_id = fa.film_id 
  inner join actor a on fa.actor_id = a.actor_id 
where c."name" = 'Children' 
group by a.last_name  
order by 1 desc) res where res.rank_number <= 3 

--6. 
------------------------------------------------------------

select 
  cit.city,
  sum(active),
  sum(inactive)
from
  (select 
    ci.city as city,
    case when c.active = 1 then 1 else 0 end as active, 
    case when c.active = 0 then 1 else 0 end as inactive
  from customer c 
    inner join address a on c.address_id = a.address_id 
    inner join city ci on a.city_id = ci.city_id) cit 
group by cit.city 
order by sum(inactive) desc

--7. 
------------------------------------------------------------

select
  tmp.city,
  tmp.category,
  tmp.duration  
from (
	select 
	  ct.city as city, 
	  c."name" as category, 
	  case when r.return_date is null  then CURRENT_DATE - r.rental_date else r.return_date - r.rental_date end as duration,
	   ROW_NUMBER () OVER (
		PARTITION BY ct.city
		ORDER BY
			case when r.return_date is null  then CURRENT_DATE - r.rental_date else r.return_date - r.rental_date end desc
	 ) as place
	from film_category fc 
	  inner join category c on fc.category_id = c.category_id 
	  inner join inventory i on fc.film_id = i.film_id 
	  inner join film f on i.film_id = f.film_id 
	  inner join rental r on i.inventory_id = r.inventory_id 
	  inner join customer c2 on r.customer_id = c2.customer_id 
	  inner join address a on c2.address_id = a.address_id 
	  inner join city ct on ct.city_id = a.city_id 
	where f.title like 'A%' and ct.city like '%-%'
	order by 1
) tmp where tmp.place = 1



 