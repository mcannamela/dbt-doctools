{{ config(materialized='table') }}

with source_data as (

    select
        old_source_id as old_id,
        old_source_value as old_value,
        1 as another_value
    from {{source('dummy_sources', 'some_old_source')}}

)

select *
from source_data
