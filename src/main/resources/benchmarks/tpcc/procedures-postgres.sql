-- TPC-C stored procedures for PostgreSQL.
--
-- Loaded right after ddl-postgres.sql when the workload configuration sets
-- <useStoredProcedures>true</useStoredProcedures>.  Each function issues
-- exactly the statement sequence of the matching Java procedure in
-- com.oltpbenchmark.benchmarks.tpcc.procedures, so the two modes differ only
-- in how many client/server round trips a transaction costs.
--
-- Errors that the Java procedures signal with UserAbortException are raised
-- with SQLSTATE 'TPCC1' so the client can tell an expected abort (the 1% of
-- new orders that reference an unused item id) from a real failure.

CREATE OR REPLACE FUNCTION tpcc_new_order(
    in_w_id           int,
    in_d_id           int,
    in_c_id           int,
    in_ol_i_id        int[],
    in_ol_supply_w_id int[],
    in_ol_quantity    int[])
RETURNS TABLE (
    out_o_id       int,
    out_o_entry_d  timestamp,
    out_w_tax      decimal(4, 4),
    out_d_tax      decimal(4, 4),
    out_c_discount decimal(4, 4),
    out_c_last     varchar(16),
    out_c_credit   char(2),
    out_total      decimal(12, 2))
LANGUAGE plpgsql AS $$
DECLARE
    v_w_tax       decimal(4, 4);
    v_d_tax       decimal(4, 4);
    v_d_next_o_id int;
    v_c_discount  decimal(4, 4);
    v_c_last      varchar(16);
    v_c_credit    char(2);
    v_o_entry_d   timestamp := CURRENT_TIMESTAMP;
    v_ol_cnt      int := coalesce(array_length(in_ol_i_id, 1), 0);
    v_all_local   int := 1;
    v_total       decimal(12, 2) := 0;
    v_n           int;
    v_i_id        int;
    v_supply_w_id int;
    v_quantity    int;
    v_i_price     decimal(5, 2);
    v_i_name      varchar(24);
    v_i_data      varchar(50);
    v_s_quantity  int;
    v_s_data      varchar(50);
    v_s_dist      char(24);
    v_ol_amount   decimal(6, 2);
BEGIN
    SELECT c_discount, c_last, c_credit
      INTO v_c_discount, v_c_last, v_c_credit
      FROM customer
     WHERE c_w_id = in_w_id AND c_d_id = in_d_id AND c_id = in_c_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'C_D_ID=% C_ID=% not found!', in_d_id, in_c_id;
    END IF;

    SELECT w_tax INTO v_w_tax FROM warehouse WHERE w_id = in_w_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'W_ID=% not found!', in_w_id;
    END IF;

    SELECT d_next_o_id, d_tax INTO v_d_next_o_id, v_d_tax
      FROM district
     WHERE d_w_id = in_w_id AND d_id = in_d_id
       FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'D_ID=% D_W_ID=% not found!', in_d_id, in_w_id;
    END IF;

    UPDATE district SET d_next_o_id = d_next_o_id + 1
     WHERE d_w_id = in_w_id AND d_id = in_d_id;

    FOR v_n IN 1 .. v_ol_cnt LOOP
        IF in_ol_supply_w_id[v_n] <> in_w_id THEN
            v_all_local := 0;
        END IF;
    END LOOP;

    INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
    VALUES (v_d_next_o_id, in_d_id, in_w_id, in_c_id, v_o_entry_d, v_ol_cnt, v_all_local);

    INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
    VALUES (v_d_next_o_id, in_d_id, in_w_id);

    FOR v_n IN 1 .. v_ol_cnt LOOP
        v_i_id        := in_ol_i_id[v_n];
        v_supply_w_id := in_ol_supply_w_id[v_n];
        v_quantity    := in_ol_quantity[v_n];

        SELECT i_price, i_name, i_data
          INTO v_i_price, v_i_name, v_i_data
          FROM item WHERE i_id = v_i_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'EXPECTED new order rollback: I_ID=% not found!', v_i_id
                  USING ERRCODE = 'TPCC1';
        END IF;

        v_ol_amount := v_quantity * v_i_price;
        v_total := v_total + v_ol_amount;

        SELECT s_quantity, s_data,
               CASE in_d_id
                   WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02
                   WHEN 3 THEN s_dist_03 WHEN 4 THEN s_dist_04
                   WHEN 5 THEN s_dist_05 WHEN 6 THEN s_dist_06
                   WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08
                   WHEN 9 THEN s_dist_09 WHEN 10 THEN s_dist_10
               END
          INTO v_s_quantity, v_s_data, v_s_dist
          FROM stock
         WHERE s_i_id = v_i_id AND s_w_id = v_supply_w_id
           FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'S_I_ID=% not found!', v_i_id;
        END IF;

        IF v_s_quantity - v_quantity >= 10 THEN
            v_s_quantity := v_s_quantity - v_quantity;
        ELSE
            v_s_quantity := v_s_quantity - v_quantity + 91;
        END IF;

        INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id,
                                ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
        VALUES (v_d_next_o_id, in_d_id, in_w_id, v_n, v_i_id,
                v_supply_w_id, v_quantity, v_ol_amount, v_s_dist);

        UPDATE stock
           SET s_quantity = v_s_quantity,
               s_ytd = s_ytd + v_quantity,
               s_order_cnt = s_order_cnt + 1,
               s_remote_cnt = s_remote_cnt
                              + CASE WHEN v_supply_w_id = in_w_id THEN 0 ELSE 1 END
         WHERE s_i_id = v_i_id AND s_w_id = v_supply_w_id;
    END LOOP;

    RETURN QUERY SELECT v_d_next_o_id, v_o_entry_d, v_w_tax, v_d_tax,
                        v_c_discount, v_c_last, v_c_credit, v_total;
END;
$$;

-- Either in_c_id or in_c_last is supplied; the client picks by name for 60% of
-- the payments, exactly as the Java procedure does.
CREATE OR REPLACE FUNCTION tpcc_payment(
    in_w_id     int,
    in_d_id     int,
    in_c_w_id   int,
    in_c_d_id   int,
    in_c_id     int,
    in_c_last   varchar(16),
    in_h_amount decimal(6, 2))
RETURNS TABLE (
    out_c_id          int,
    out_c_first       varchar(16),
    out_c_middle      char(2),
    out_c_last        varchar(16),
    out_c_street_1    varchar(20),
    out_c_street_2    varchar(20),
    out_c_city        varchar(20),
    out_c_state       char(2),
    out_c_zip         char(9),
    out_c_phone       char(16),
    out_c_credit      char(2),
    out_c_credit_lim  decimal(12, 2),
    out_c_discount    decimal(4, 4),
    out_c_balance     decimal(12, 2),
    out_c_since       timestamp,
    out_w_street_1    varchar(20),
    out_w_street_2    varchar(20),
    out_w_city        varchar(20),
    out_w_state       char(2),
    out_w_zip         char(9),
    out_d_street_1    varchar(20),
    out_d_street_2    varchar(20),
    out_d_city        varchar(20),
    out_d_state       char(2),
    out_d_zip         char(9))
LANGUAGE plpgsql AS $$
DECLARE
    v_w        warehouse%ROWTYPE;
    v_d        district%ROWTYPE;
    v_c        customer%ROWTYPE;
    v_c_data   varchar(500);
    v_h_date   timestamp := CURRENT_TIMESTAMP;
BEGIN
    UPDATE warehouse SET w_ytd = w_ytd + in_h_amount WHERE w_id = in_w_id;

    SELECT * INTO v_w FROM warehouse WHERE w_id = in_w_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'W_ID=% not found!', in_w_id;
    END IF;

    UPDATE district SET d_ytd = d_ytd + in_h_amount
     WHERE d_w_id = in_w_id AND d_id = in_d_id;

    SELECT * INTO v_d FROM district WHERE d_w_id = in_w_id AND d_id = in_d_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'D_ID=% D_W_ID=% not found!', in_d_id, in_w_id;
    END IF;

    IF in_c_id IS NULL THEN
        -- The middle customer of those sharing the last name, ordered by c_first.
        SELECT c.* INTO v_c
          FROM (SELECT cust.*,
                       row_number() OVER (ORDER BY cust.c_first) AS rn,
                       count(*) OVER () AS cnt
                  FROM customer cust
                 WHERE cust.c_w_id = in_c_w_id
                   AND cust.c_d_id = in_c_d_id
                   AND cust.c_last = in_c_last) c
         WHERE c.rn = (c.cnt + 1) / 2;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'C_LAST=% C_D_ID=% C_W_ID=% not found!',
                  in_c_last, in_c_d_id, in_c_w_id;
        END IF;
    ELSE
        SELECT * INTO v_c FROM customer
         WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id AND c_id = in_c_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'C_ID=% C_D_ID=% C_W_ID=% not found!',
                  in_c_id, in_c_d_id, in_c_w_id;
        END IF;
    END IF;

    v_c.c_balance     := v_c.c_balance - in_h_amount;
    v_c.c_ytd_payment := v_c.c_ytd_payment + in_h_amount;
    v_c.c_payment_cnt := v_c.c_payment_cnt + 1;

    IF v_c.c_credit = 'BC' THEN
        SELECT c_data INTO v_c_data FROM customer
         WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id AND c_id = v_c.c_id;

        v_c_data := left(v_c.c_id || ' ' || in_c_d_id || ' ' || in_c_w_id || ' '
                         || in_d_id || ' ' || in_w_id || ' ' || in_h_amount
                         || ' | ' || v_c_data, 500);
        v_c.c_data := v_c_data;

        UPDATE customer
           SET c_balance = v_c.c_balance,
               c_ytd_payment = v_c.c_ytd_payment,
               c_payment_cnt = v_c.c_payment_cnt,
               c_data = v_c_data
         WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id AND c_id = v_c.c_id;
    ELSE
        UPDATE customer
           SET c_balance = v_c.c_balance,
               c_ytd_payment = v_c.c_ytd_payment,
               c_payment_cnt = v_c.c_payment_cnt
         WHERE c_w_id = in_c_w_id AND c_d_id = in_c_d_id AND c_id = v_c.c_id;
    END IF;

    INSERT INTO history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id,
                         h_date, h_amount, h_data)
    VALUES (in_c_d_id, in_c_w_id, v_c.c_id, in_d_id, in_w_id,
            v_h_date, in_h_amount, v_w.w_name || '    ' || v_d.d_name);

    RETURN QUERY SELECT v_c.c_id, v_c.c_first, v_c.c_middle, v_c.c_last,
                        v_c.c_street_1, v_c.c_street_2, v_c.c_city, v_c.c_state,
                        v_c.c_zip, v_c.c_phone, v_c.c_credit, v_c.c_credit_lim,
                        v_c.c_discount, v_c.c_balance, v_c.c_since,
                        v_w.w_street_1, v_w.w_street_2, v_w.w_city, v_w.w_state,
                        v_w.w_zip,
                        v_d.d_street_1, v_d.d_street_2, v_d.d_city, v_d.d_state,
                        v_d.d_zip;
END;
$$;

-- One row per order line of the customer's most recent order; the customer and
-- order header columns repeat on every row so the whole result the Java
-- procedure assembles still crosses the wire.
CREATE OR REPLACE FUNCTION tpcc_order_status(
    in_w_id   int,
    in_d_id   int,
    in_c_id   int,
    in_c_last varchar(16))
RETURNS TABLE (
    out_c_id           int,
    out_c_first        varchar(16),
    out_c_middle       char(2),
    out_c_last         varchar(16),
    out_c_balance      decimal(12, 2),
    out_o_id           int,
    out_o_entry_d      timestamp,
    out_o_carrier_id   int,
    out_ol_i_id        int,
    out_ol_supply_w_id int,
    out_ol_quantity    decimal(6, 2),
    out_ol_amount      decimal(6, 2),
    out_ol_delivery_d  timestamp)
LANGUAGE plpgsql AS $$
DECLARE
    v_c            customer%ROWTYPE;
    v_o_id         int;
    v_o_entry_d    timestamp;
    v_o_carrier_id int;
BEGIN
    IF in_c_id IS NULL THEN
        SELECT c.* INTO v_c
          FROM (SELECT cust.*,
                       row_number() OVER (ORDER BY cust.c_first) AS rn,
                       count(*) OVER () AS cnt
                  FROM customer cust
                 WHERE cust.c_w_id = in_w_id
                   AND cust.c_d_id = in_d_id
                   AND cust.c_last = in_c_last) c
         WHERE c.rn = (c.cnt + 1) / 2;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'C_LAST=% C_D_ID=% C_W_ID=% not found!',
                  in_c_last, in_d_id, in_w_id;
        END IF;
    ELSE
        SELECT * INTO v_c FROM customer
         WHERE c_w_id = in_w_id AND c_d_id = in_d_id AND c_id = in_c_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'C_ID=% C_D_ID=% C_W_ID=% not found!',
                  in_c_id, in_d_id, in_w_id;
        END IF;
    END IF;

    SELECT o_id, o_carrier_id, o_entry_d
      INTO v_o_id, v_o_carrier_id, v_o_entry_d
      FROM oorder
     WHERE o_w_id = in_w_id AND o_d_id = in_d_id AND o_c_id = v_c.c_id
     ORDER BY o_id DESC
     LIMIT 1;

    RETURN QUERY
    SELECT v_c.c_id, v_c.c_first, v_c.c_middle, v_c.c_last, v_c.c_balance,
           v_o_id, v_o_entry_d, v_o_carrier_id,
           ol.ol_i_id, ol.ol_supply_w_id, ol.ol_quantity, ol.ol_amount,
           ol.ol_delivery_d
      FROM order_line ol
     WHERE ol.ol_o_id = v_o_id AND ol.ol_d_id = in_d_id AND ol.ol_w_id = in_w_id;
END;
$$;

-- Returns the delivered order id per district, or NULL where the district had
-- no undelivered order (the Java procedure records -1 for those).
CREATE OR REPLACE FUNCTION tpcc_delivery(
    in_w_id         int,
    in_o_carrier_id int,
    in_d_id_max     int)
RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE
    v_result   int[] := array_fill(NULL::int, ARRAY[in_d_id_max]);
    v_d_id     int;
    v_no_o_id  int;
    v_c_id     int;
    v_ol_total decimal(12, 2);
    v_deliv_d  timestamp := CURRENT_TIMESTAMP;
BEGIN
    FOR v_d_id IN 1 .. in_d_id_max LOOP
        SELECT no_o_id INTO v_no_o_id
          FROM new_order
         WHERE no_d_id = v_d_id AND no_w_id = in_w_id
         ORDER BY no_o_id ASC
         LIMIT 1;
        CONTINUE WHEN NOT FOUND;

        DELETE FROM new_order
         WHERE no_o_id = v_no_o_id AND no_d_id = v_d_id AND no_w_id = in_w_id;

        SELECT o_c_id INTO v_c_id
          FROM oorder
         WHERE o_id = v_no_o_id AND o_d_id = v_d_id AND o_w_id = in_w_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'O_ID=% O_D_ID=% O_W_ID=% not found!',
                  v_no_o_id, v_d_id, in_w_id;
        END IF;

        UPDATE oorder SET o_carrier_id = in_o_carrier_id
         WHERE o_id = v_no_o_id AND o_d_id = v_d_id AND o_w_id = in_w_id;

        UPDATE order_line SET ol_delivery_d = v_deliv_d
         WHERE ol_o_id = v_no_o_id AND ol_d_id = v_d_id AND ol_w_id = in_w_id;

        SELECT sum(ol_amount) INTO v_ol_total
          FROM order_line
         WHERE ol_o_id = v_no_o_id AND ol_d_id = v_d_id AND ol_w_id = in_w_id;

        UPDATE customer
           SET c_balance = c_balance + v_ol_total,
               c_delivery_cnt = c_delivery_cnt + 1
         WHERE c_w_id = in_w_id AND c_d_id = v_d_id AND c_id = v_c_id;

        v_result[v_d_id] := v_no_o_id;
    END LOOP;

    RETURN v_result;
END;
$$;

CREATE OR REPLACE FUNCTION tpcc_stock_level(
    in_w_id      int,
    in_d_id      int,
    in_threshold int)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    v_d_next_o_id int;
    v_count       int;
BEGIN
    SELECT d_next_o_id INTO v_d_next_o_id
      FROM district WHERE d_w_id = in_w_id AND d_id = in_d_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'D_W_ID=% D_ID=% not found!', in_w_id, in_d_id;
    END IF;

    SELECT count(DISTINCT s_i_id) INTO v_count
      FROM order_line, stock
     WHERE ol_w_id = in_w_id
       AND ol_d_id = in_d_id
       AND ol_o_id < v_d_next_o_id
       AND ol_o_id >= v_d_next_o_id - 20
       AND s_w_id = in_w_id
       AND s_i_id = ol_i_id
       AND s_quantity < in_threshold;

    RETURN v_count;
END;
$$;
