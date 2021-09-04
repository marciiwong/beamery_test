import sqlalchemy as s
import pandas as pd


def create_connection():
    host = 'tai.db.elephantsql.com'
    user = 'xbsstbyy'
    db = 'xbsstbyy'
    pwd = 'mpPWwkb3YjnyKUjYGgunh5C_3PZyjuls'
    engine = s.create_engine(f'postgresql://{user}:{pwd}@{host}/{db}')
    return engine


def query(q):
    conn = create_connection()
    if 'update' not in q and 'create' not in q and 'insert' not in q and 'select' in q:
        return pd.read_sql_query(q, con=conn)
    else:
        conn.execute(q)
        return True


def db_insert(data, target):
    conn = create_connection()
    insert_statement = ''
    cols = ','.join(data.columns)
    for i in range(0, data.shape[0]):
        values = tuple(list(data.iloc[i, :]))
        values = str(values) if len(values) > 1 else str(values).replace(',', '')
        values = values.replace("'null'", "null").replace('"', "'")
        insert_statement += f'insert into public.{target} ({cols}) values {values};'
    conn.execute(insert_statement)
    conn.dispose()
    return True


def create_table():
    tables = query('''select * from pg_catalog.pg_tables where schemaname='public' ''')
    if tables[tables['tablename']=='daily_fx_rate'].shape[0] == 0:
        query('''create table public.daily_fx_rate (
                    date date,
                    source_currency varchar(3),
                    target_currency varchar(3),
                    rate decimal(19, 4)
                 )''')
    if tables[tables['tablename']=='monthly_fx_rate'].shape[0] == 0:
        query('''create table public.monthly_fx_rate (
                    date date,
                    source_currency varchar(3),
                    target_currency varchar(3),
                    month_rate decimal(19, 4)
                 )''')
    if tables[tables['tablename']=='six_months_fixed_rate'].shape[0] == 0:
        query('''create table public.six_months_fixed_rate (
                    date date,
                    source_currency varchar(3),
                    target_currency varchar(3),
                    fixed_rate decimal(19, 4)
                 )''')
    return True


def create_monthly_fx_rate_usp():
    q = ''' create or replace procedure create_monthly_rate() 
            language plpgsql as $$
            begin
            
            --reset target table
            truncate table public.monthly_fx_rate;
            
            -- create common table expression to get a table a rate 
            -- for year_month source_currency, target_currency combination
            with monthly_data as 
            (select 
            	to_char(date, 'YYYY-MM') year_month,
            	source_currency,
            	target_currency,
            	round(avg(rate), 4) monthly_avg_rate
            from public.daily_fx_rate
            group by to_char(date, 'YYYY-MM'), source_currency, target_currency)
            
            insert into public.monthly_fx_rate
            (date, source_currency, target_currency, month_rate)
            select 
            	d.date,
            	d.source_currency,
            	d.target_currency,
            	m.monthly_avg_rate
            from public.daily_fx_rate d
            left join monthly_data m
            on 
                -- join on year_month from daily_fx_rate to get monthly avg rate
                to_char(d.date, 'YYYY-MM') = m.year_month 
            	and d.source_currency = m.source_currency 
            	and d.target_currency = m.target_currency
            order by source_currency, target_currency, date;
            
            end;$$ '''
    return query(q)


def create_fixed_rate_usp():
    q = ''' create or replace procedure create_fixed_rate()
            language plpgsql as $$
            begin
            
            --reset target table
            truncate table public.six_months_fixed_rate;
            
            -- create common table expression to get a table a rate 
            -- for each year, first_half_year_indicator, source_currency, target_currency combination
            with fixed_rate_data as
            (select
            	source_currency,
            	target_currency,
            	extract(year from date) as year,
            	case when extract(month from date) < 7 then 1 else 0 end as first_half,
            	round(avg(rate), 4) fixed_rate
            from public.daily_fx_rate
            group by 
            	source_currency,
            	target_currency,
            	extract(year from date),
            	case when extract(month from date) < 7 then 1 else 0 end)
            
            insert into public.six_months_fixed_rate
            (date, source_currency, target_currency, fixed_rate)
            select 
            d.date,
            d.source_currency,
            d.target_currency,
            f.fixed_rate
            from public.daily_fx_rate d
            left join fixed_rate_data f
            on 
            	-- join with the year which is 6 months ago
            	extract(year from d.date-interval'6 month') = f.year and 
            	-- join with the indicator which indicate the date is first half or second half of year 6 months ago
            	case when extract(month from d.date-interval'6 month') < 7 then 1 else 0 end = f.first_half and 
            	d.source_currency = f.source_currency and 
            	d.target_currency = f.target_currency
            where f.fixed_rate is not null
            order by target_currency, date;
            
            end;$$
             '''
    return query(q)
