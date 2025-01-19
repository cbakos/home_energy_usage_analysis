INSERT INTO meters (meter_identifier, connection_type, start_date, end_date)
    VALUES ('{data['meter_identifier']}', '{data['connection_type']}',
            '{data['start_date']}', {f"'{data['end_date']}'" if data['end_date'] else 'NULL'});