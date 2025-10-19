-- HISTORY table
CREATE TABLE history (
  h_c_id   INT,
  h_c_d_id SMALLINT,
  h_c_w_id SMALLINT,
  h_d_id   SMALLINT,
  h_w_id   SMALLINT,
  h_date   TIMESTAMP,
  h_amount DECIMAL(6, 2),
  h_data   VARCHAR(24)
);

-- Foreign Keys
ALTER TABLE district ADD FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE;
ALTER TABLE customer ADD FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE;
ALTER TABLE history ADD FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE;
ALTER TABLE history ADD FOREIGN KEY (h_w_id, h_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE;
ALTER TABLE stock ADD FOREIGN KEY (s_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE;
ALTER TABLE stock ADD FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE;
ALTER TABLE orders ADD FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE;
ALTER TABLE new_order ADD FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES orders (o_w_id, o_d_id, o_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES orders (o_w_id, o_d_id, o_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock (s_w_id, s_i_id) ON DELETE CASCADE;