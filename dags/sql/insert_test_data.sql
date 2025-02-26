INSERT INTO stock (warehouse_id, stock_quantity, processing_date)
VALUES ('{{ var.value.default_warehouse }}', 15, '{{ ds }}');
