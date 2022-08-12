CREATE OR REPLACE FUNCTION egress(text_length int, num_rows int) RETURNS SETOF RECORD AS $$
DECLARE
    rec record;
    temp_string text;
BEGIN
    SELECT lpad('', text_length, 'x') INTO temp_string;
    FOR i IN 1..num_rows LOOP
        select temp_string into rec;
        return next rec;
    END LOOP;
END $$ language plpgsql;
