{{
    config(
        materialized='table',
        distributed_by='id',
        appendoptimized=true,
        orientation='column',
        compresstype='ZLIB',
        compresslevel=1,
        blocksize=32768
    )
}}


with source_data as (

    select generate_series as id
    from generate_series(1, 100)

)

select *
from source_data