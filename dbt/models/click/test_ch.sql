{{
    config(
        materialized='table'
    )
}}


select number as id
from numbers(10)~