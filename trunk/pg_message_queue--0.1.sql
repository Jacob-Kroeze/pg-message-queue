\echo Use "CREATE EXTENSION pg_message_queue" to load this file. \quit

CREATE TABLE pg_mq_config_catalog (
    table_name name not null unique,
    channel name primary key,
    payload_type text not null,
    check (payload_type IN ('text', 'xml', 'bytea'))
);


SELECT pg_catalog.pg_extension_config_dump('pg_mq_config_catalog', '');

COMMENT ON TABLE pg_mq_config_catalog IS 
$$ The base message queue catalog.  $$;

CREATE TABLE pg_mq_base (
    msg_id bigserial not null,
    sent_by name not null default session_user,
    was_delivered bool not null default false
);

CREATE TABLE pg_mq_xml (
    payload xml not null,
    primary key (msg_id)
) inherits (pg_mq_base);

CREATE TABLE pg_mq_text (
    payload text not null,
    primary key (msg_id)
) inherits (pg_mq_base);

CREATE TABLE pg_mq_bytea (
    payload bytea not null,
    primary key (msg_id)
) inherits (pg_mq_base);

-- for 9.2 add JSON type as well.

CREATE OR REPLACE FUNCTION pg_mq_trigger_notify() RETURNS TRIGGER
LANGUAGE PLPGSQL AS
$$
DECLARE t_channel name;
BEGIN
   SELECT channel INTO t_channel FROM pg_mq_config_catalog 
    WHERE table_name = TG_RELNAME;

   EXECUTE 'NOTIFY ' || quote_ident(t_channel) || ', ' || NEW.msg_id;
   RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_create_queue(in_channel text, in_type text)
RETURNS pg_mq_config_catalog 
LANGUAGE PLPGSQL VOLATILE SECURITY DEFINER AS $$

DECLARE 
    out_val pg_mq_config_catalog%ROWTYPE;
    t_table_name name;
BEGIN

t_table_name := 'pg_mq_queue_' || in_channel;

INSERT INTO pg_mq_config_catalog (table_name, channel, payload_type)
VALUES (t_table_name, in_channel, in_type);

SELECT * INTO out_val FROM pg_mq_config_catalog 
 WHERE channel = in_channel;

EXECUTE 'CREATE TABLE ' || quote_ident(t_table_name) || '(
    like ' ||  quote_ident('pg_mq_' || in_type ) || ' INCLUDING ALL
  )';

EXECUTE 'CREATE TRIGGER pg_mq_notify
         AFTER INSERT ON ' || quote_ident(t_table_name) || '
         FOR EACH ROW EXECUTE PROCEDURE pg_mq_trigger_notify()';

RETURN out_val;

END;
$$;

REVOKE EXECUTE ON FUNCTION pg_mq_create_queue(text, text) FROM public;

CREATE OR REPLACE FUNCTION pg_mq_drop_queue(in_channel name) RETURNS bool
LANGUAGE plpgsql VOLATILE SECURITY DEFINER  AS $$

declare t_table_name name;

BEGIN

   SELECT table_name INTO t_table_name FROM pg_mq_config_catalog 
    WHERE channel = in_channel;

   EXECUTE 'DROP TABLE ' || quote_ident(t_table_name) || ' CASCADE';

   DELETE FROM pg_mq_config_catalog WHERE channel = in_channel;

   RETURN FOUND;

END;
$$;

REVOKE EXECUTE ON FUNCTION pg_mq_drop_queue(in_channel name) FROM public;

CREATE OR REPLACE FUNCTION pg_mq_send_message
(in_channel text, in_payload anyelement)
RETURNS pg_mq_base
LANGUAGE PLPGSQL VOLATILE AS $$
    DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
            out_val pg_mq_base%ROWTYPE;
    BEGIN
       SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;

       EXECUTE 'INSERT INTO ' || quote_ident(cat_entry.table_name)
               || ' (payload) VALUES (' || quote_literal(in_payload) || ')';

       EXECUTE 
               'SELECT (msg::pg_mq_base).* FROM ' || 
                       quote_ident(cat_entry.table_name) || ' msg 
                 WHERE msg_id = currval(''pg_mq_base_msg_id_seq'')' 
       INTO out_val ;
       RETURN out_val;
    END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_get_msg_text(in_channel name, in_num_msgs int)
RETURNS SETOF pg_mq_text
LANGUAGE PLPGSQL VOLATILE AS $$
   DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
   BEGIN
      SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;
      IF cat_entry.payload_type = 'bytea' THEN
         RAISE EXCEPTION 'Incorrect Type Called';
      END IF;
      RETURN QUERY EXECUTE
         $e$ UPDATE $e$ || quote_ident(cat_entry.table_name) || $e$
                SET was_delivered = true 
              WHERE msg_id IN (SELECT msg_id 
                                 FROM $e$ || quote_ident(cat_entry.table_name) || 
                         $e$    WHERE was_delivered is not true
                             ORDER BY msg_id LIMIT $e$ || in_num_msgs || $e$
                             )
          RETURNING msg_id, sent_by, was_delivered, payload::text $e$;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_get_msg_bin(in_channel name, in_num_msgs int)
RETURNS SETOF pg_mq_bytea 
LANGUAGE PLPGSQL AS $$
   DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
           out_val pg_mq_text%ROWTYPE;
   BEGIN
      SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;
      RETURN QUERY EXECUTE
         $e$ UPDATE $e$ || quote_ident(cat_entry.table_name) || $e$
                SET was_delivered = true 
              WHERE msg_id IN (SELECT msg_id 
                                 FROM $e$ || quote_ident(cat_entry.table_name) || 
                         $e$    WHERE was_delivered is not true
                             ORDER BY msg_id LIMIT $e$ || in_num_msgs || $e$
                             ) 
          RETURNING msg_id, sent_by, was_delivered, payload::bytea $e$;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_get_msg_id_text(in_channel name, in_id int)
RETURNS pg_mq_text 
LANGUAGE PLPGSQL AS $$
   DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
           out_val pg_mq_text%ROWTYPE;
   BEGIN
      SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;
      EXECUTE
         $e$ UPDATE $e$ || quote_ident(cat_entry.table_name) || $e$
                SET was_delivered = true 
              WHERE msg_id = $e$ || quote_literal(in_id) $e$
          RETURNING msg_id, sent_by, was_delivered, payload::text $e$
      INTO out_val;
      RETURN out_val;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_get_msg_id_bin(in_channel name, in_id int)
RETURNS pg_mq_bytea 
LANGUAGE PLPGSQL AS $$
   DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
           out_val pg_mq_text%ROWTYPE;
   BEGIN
      SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;
      EXECUTE
         $e$ UPDATE $e$ || quote_ident(cat_entry.table_name) || $e$
                SET was_delivered = true 
              WHERE msg_id = $e$ || quote_literal(in_id) $e$
          RETURNING msg_id, sent_by, was_delivered, payload::bytea $e$
      INTO out_val;
      RETURN out_val;
END;
$$;
