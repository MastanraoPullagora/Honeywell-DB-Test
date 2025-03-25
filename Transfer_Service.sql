CREATE OR REPLACE PROCEDURE TransferService(
    IN source_aircraft_id ,
    IN destination_aircraft_id,
    IN service_id 
)
LANGUAGE plpgsql
AS $$
DECLARE
    source_customer_id numeric;
    destination_customer_id numeric;
    service_status VARCHAR(20);
    asset_type VARCHAR(50);
BEGIN
    -- Start transaction
    BEGIN;

    -- Validate source and destination aircraft existence and airline association
    SELECT a1.customer_id, a2.customer_id INTO source_customer_id, destination_customer_id
    FROM Aircraft a1, Aircraft a2
    WHERE a1.aircraft_id = source_aircraft_id AND a2.aircraft_id = destination_aircraft_id;

    IF source_customer_id IS NULL OR destination_customer_id IS NULL THEN
        RAISE EXCEPTION 'Source or Destination Aircraft does not exist';
    ELSIF source_customer_id != destination_customer_id THEN
        RAISE EXCEPTION 'Aircraft must belong to the same airline';
    END IF;

    -- Validate service status
    SELECT status INTO service_status
    FROM Services
    WHERE service_id = service_id AND aircraft_id = source_aircraft_id;

    IF service_status IS NULL OR service_status = 'In Progress' THEN
        RAISE EXCEPTION 'Service is not active or is currently in progress';
    END IF;

    -- Check for compatible asset on the destination aircraft
    SELECT asset_type INTO asset_type
    FROM Assets
    WHERE asset_id = (SELECT asset_id FROM Services WHERE service_id = service_id);

    -- Assign a new compatible asset if none exists
    IF NOT EXISTS (
        SELECT 1
        FROM Assets
        WHERE aircraft_id = destination_aircraft_id AND asset_type = asset_type
    ) THEN
        INSERT INTO Assets (aircraft_id, asset_type, serial_number, installed_date)
        VALUES (destination_aircraft_id, asset_type, CONCAT('SN-', gen_random_uuid()), NOW());
    END IF;

    -- Transfer the service to the destination aircraft
    UPDATE Services
    SET aircraft_id = destination_aircraft_id
    WHERE service_id = service_id AND aircraft_id = source_aircraft_id;

    -- Log the transfer in the audit table
    INSERT INTO Audit_Log (action, details, timestamp)
    VALUES (
        'Service Transferred',
        FORMAT('Transferred service %s from aircraft %s to %s', service_id, source_aircraft_id, destination_aircraft_id),
        NOW()
    );

    -- Commit transaction
    COMMIT;

EXCEPTION WHEN OTHERS THEN
    
    -- Log the error message in an audit table or raise an exception with details
    INSERT INTO Audit_Log (action, details, timestamp)
    VALUES (
        'Service Transfer Failed',
        FORMAT('Error transferring service %s from aircraft %s to %s: %s',
               service_id, source_aircraft_id, destination_aircraft_id, SQLERRM),
        NOW()
    );
    ROLLBACK;
   RAISE;
END;
$$;