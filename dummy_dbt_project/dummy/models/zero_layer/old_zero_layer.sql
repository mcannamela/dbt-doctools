{{ config(materialized='table') }}

with source_data as (

    select
        old_source_id as old_id,
        old_source_value as old_value
    from {{source('sources', 'some_old_source')}}

)

select *
from source_data
