select old_id,
       old_value,
       old_value + 100 as transformed_value,
       another_value
from {{ ref('old_zero_layer') }}
