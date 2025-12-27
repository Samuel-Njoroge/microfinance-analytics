{{
    config(
        materialized='table'
    )
}}

with par_analysis as (
    select
        aging_bucket,
        count(distinct loan_id) as loan_count,
        count(distinct user_id) as borrower_count,
        sum(outstanding_balance) as total_outstanding,
        sum(approved_amount) as total_loan_amount,
        avg(days_overdue) as avg_days_overdue,
        
        sum(case when is_non_performing then outstanding_balance else 0 end) as npl_amount,
        sum(case when is_par_30 then outstanding_balance else 0 end) as par_30_amount,
        
        sum(case when is_overdue then outstanding_balance else 0 end) as overdue_amount
        
    from {{ ref('int_loan_aging') }}
    group by aging_bucket
),

total_portfolio as (
    select
        sum(outstanding_balance) as total_portfolio_balance
    from {{ ref('int_loan_aging') }}
)

select
    pa.aging_bucket,
    pa.loan_count,
    pa.borrower_count,
    pa.total_outstanding,
    pa.total_loan_amount,
    pa.avg_days_overdue,
    pa.npl_amount,
    pa.par_30_amount,
    pa.overdue_amount,
    
    -- PAR percentages
    case 
        when pa.total_outstanding > 0 
        then (pa.overdue_amount / pa.total_outstanding) * 100 
        else 0 
    end as par_percentage,
    
    case 
        when tp.total_portfolio_balance > 0 
        then (pa.total_outstanding / tp.total_portfolio_balance) * 100 
        else 0 
    end as portfolio_concentration_pct,
    
    current_timestamp as last_updated
    
from par_analysis pa
cross join total_portfolio tp
order by 
    case aging_bucket
        when 'CURRENT' then 1
        when 'PAID_OFF' then 2
        when '1-30_DAYS' then 3
        when '31-60_DAYS' then 4
        when '61-90_DAYS' then 5
        when '91-180_DAYS' then 6
        when 'OVER_180_DAYS' then 7
    end