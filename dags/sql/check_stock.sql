SELECT stock_quantity 
FROM stock 
WHERE warehouse_id = '{{ var.value.default_warehouse }}'
AND processing_date = '{{ ds }}';
