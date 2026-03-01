CREATE OR REPLACE VIEW p1_crypto.gold_layer.hourly_top_300_cap_24hr_trend AS
with top_300_history as (
 select
   last_updated,
   date(last_updated) as date,
   date_format(last_updated, 'HH:mm:ss') as time,
   name, id, current_price, high_24h, low_24h,
   price_change_percentage_24h, market_cap_rank, market_cap
 from p1_crypto.silver_layer.silver_crypto_streamtable
 where market_cap_rank <= 300
   and last_updated >= current_timestamp() - interval 1 day 1 hour
),
lag_cap_calc as (
  select *,
    lead(current_price) over (partition by id order by last_updated desc) as prev_price_1hr,
    CASE
        WHEN market_cap_rank <= 10 THEN 'mega cap'
        WHEN market_cap_rank <= 50 THEN 'large cap'
        WHEN market_cap_rank <= 200 THEN 'mid cap'
        ELSE 'long tail'
    END AS cap_segment
  from top_300_history
)
  select
    date, time, name, id, current_price, prev_price_1hr, 
    round(current_price - prev_price_1hr, 2) as price_change_1hr,
    round(((current_price - prev_price_1hr) / prev_price_1hr) * 100, 2) as price_change_perc_1hr,
    high_24h, low_24h, price_change_percentage_24h, market_cap_rank, market_cap, cap_segment, last_updated
  from lag_cap_calc
  where prev_price_1hr is not null
  order by market_cap_rank, date desc, time desc;