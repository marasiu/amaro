--Query 1
select  order_seq_number, next_order_seq_number, avg(distance_days)
from    
 (
    select  t_src.*,
    lead(t_src.order_seq_number) over(partition by t_src.user_id order by order_date) next_order_seq_number
    from
    (
        select  t1.*,
        coalesce(lead(order_date, 1) over(partition by user_id order by order_date),
                t1.order_date) next_order_date,
                
        (next_order_date - order_date) day(4) as distance_days, 
        row_number() over(partition by t1.user_id order by order_date) as order_seq_number
        from    datalab.orders t1
    ) t_src
  ) t_tgt
where   next_order_seq_number is not null
group by 1,2
order by 1,2
;

--Query 2
select ref_month, 
       sum(num) as numerator, 
       sum(den) as denominator, 
       cast(sum(num) as decimal(15,2))/ nullifzero(cast(sum(den)as decimal(15,2))) as retention_rate
from (--users versus reference months
 select user_id,
      trim(extract(year from t_src.calendar_date)) || '-' || lpad(trim(extract(month from t_src.calendar_date)), 2,'0') as ref_month,
      case when max(t_src.first_purch_itval) = 1 and max(t_src.other_purchase) = 1 
           then 1 
      else 0 
      end as num,
      case when max(t_src.first_purch_itval) = 1 
           then 1 
      else 0 
      end as den
 from (
  --orders versus reference dates
  select t_cale.calendar_date,
      user_id,
      order_date,
      min(t_ord.order_date) over(partition by t_ord.user_id order by t_ord.order_date) as first_purchase,
      case when first_purchase >= add_months(t_cale.calendar_date, -5) and first_purchase < add_months(t_cale.calendar_date,-2) 
           then 1
      else 0 
      end as first_purch_itval, --first purchase interval between 5 and 2 months ago from ref_date,
      case when first_purchase <> t_ord.order_date 
         and t_ord.order_date >= add_months(t_cale.calendar_date, -2) and t_ord.order_date < t_cale.calendar_date
         then 1
      else 0 
      end as other_purchase --any other purchase between 2 and 0 months ago from ref_date
  from datalab.orders t_ord
  
  ,sys_calendar.calendar t_cale
  
  where t_cale.calendar_date between '2016-01-01' and '2016-12-31'
   and t_cale.day_of_month = 1
 ) t_src
 group by 1,2
)t1
group by 1
order by 1
;
