-- Mart: tipping behavior by payment type — answers QUESTIONS.md #6
-- (how does tip_pct differ between credit-card and cash trips, and does
-- cash tipping show up as clearly under-recorded?). One row per
-- payment_type_code, grain is plot-ready as an x=payment_type_label bar
-- chart of avg_tip_pct / pct_zero_tip.

select
    payment_type_code,
    case payment_type_code
        when 1 then 'credit_card'
        when 2 then 'cash'
        when 3 then 'no_charge'
        when 4 then 'dispute'
        when 5 then 'unknown'
        when 6 then 'voided_trip'
        else 'other'
    end                                                        as payment_type_label,
    count(*)                                                    as trip_count,
    round(avg(tip_pct) * 100, 2)                                as avg_tip_pct,
    round(avg(tip_amount), 2)                                   as avg_tip_amount,
    -- cash tips are typically not captured by the meter, so a high
    -- share of exact-zero tips is the expected signature of under-recording
    round(100.0 * sum(case when tip_amount = 0 then 1 else 0 end) / count(*), 2)
                                                                 as pct_zero_tip,
    round(avg(fare_amount), 2)                                  as avg_fare_amount
from {{ ref('fct_trips') }}
group by payment_type_code
order by trip_count desc
