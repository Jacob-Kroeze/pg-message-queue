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
    sent_at timestamp not null default now(),
    sent_by name not null default session_user,
    delivered_at timestamp
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

   EXECUTE 'NOTIFY ' || quote_ident(t_channel) || ', ' 
            || quote_literal(NEW.msg_id);
   RETURN NEW;
END;
$$;

COMMENT ON FUNCTION pg_mq_trigger_notify() IS
$$ This function raises a notification on the channel specified in the 
pg_mq_config_catalog for this table.  It is looked up every time currently so
if the value is changed in that table it takes effect on db commit. $$;

CREATE OR REPLACE FUNCTION pg_mq_create_queue
(in_channel text, in_payload_type text)
RETURNS pg_mq_config_catalog 
LANGUAGE PLPGSQL VOLATILE SECURITY DEFINER AS $$

DECLARE 
    out_val pg_mq_config_catalog%ROWTYPE;
    t_table_name name;
BEGIN

t_table_name := 'pg_mq_queue_' || in_channel;

INSERT INTO pg_mq_config_catalog (table_name, channel, payload_type)
VALUES (t_table_name, in_channel, in_payload_type);

SELECT * INTO out_val FROM pg_mq_config_catalog 
 WHERE channel = in_channel;

EXECUTE 'CREATE TABLE ' || quote_ident(t_table_name) || '(
    like ' ||  quote_ident('pg_mq_' || in_payload_type ) || ' INCLUDING ALL
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
      IF NOT FOUND THEN
         RAISE EXCEPTION 'Channel Not Found';
      END IF;

       EXECUTE 'INSERT INTO ' || quote_ident(cat_entry.table_name)
               || ' (payload) VALUES ( 
                     cast (' || quote_literal(in_payload) || ' AS ' || 
                                quote_ident(cat_entry.payload_type) || '))
                RETURNING msg_id, sent_at, sent_by, delivered_at'
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
                SET delivered_at = now()
              WHERE msg_id IN (SELECT msg_id 
                                 FROM $e$ || quote_ident(cat_entry.table_name) || 
                         $e$    WHERE delivered_at IS NULL
                             ORDER BY msg_id LIMIT $e$ || in_num_msgs || $e$
                             )
          RETURNING msg_id, sent_at, sent_by, delivered_at, payload::text $e$;
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
                SET delivered_at = now()
              WHERE msg_id IN (SELECT msg_id 
                                 FROM $e$ || quote_ident(cat_entry.table_name) || 
                         $e$    WHERE delivered_at IS NULL
                             ORDER BY msg_id LIMIT $e$ || in_num_msgs || $e$
                             ) 
          RETURNING msg_id, sent_at, sent_by, delivered_at, payload::bytea $e$;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_get_msg_id_text(in_channel name, in_msg_id int)
RETURNS pg_mq_text 
LANGUAGE PLPGSQL AS $$
   DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
           out_val pg_mq_text%ROWTYPE;
   BEGIN
      SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;
      EXECUTE
         $e$ UPDATE $e$ || quote_ident(cat_entry.table_name) || $e$
                SET delivered_at = now() 
              WHERE msg_id = $e$ || quote_literal(in_id) $e$
          RETURNING msg_id, sent_at, sent_by, delivered_at, payload::text $e$
      INTO out_val;
      RETURN out_val;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_get_msg_id_bin(in_channel name, in_msg_id int)
RETURNS pg_mq_bytea 
LANGUAGE PLPGSQL AS $$
   DECLARE cat_entry pg_mq_config_catalog%ROWTYPE;
           out_val pg_mq_text%ROWTYPE;
   BEGIN
      SELECT * INTO cat_entry FROM pg_mq_config_catalog
        WHERE channel = in_channel;
      EXECUTE
         $e$ UPDATE $e$ || quote_ident(cat_entry.table_name) || $e$
                SET delivered_at = now() 
              WHERE msg_id = $e$ || quote_literal(in_id) $e$
          RETURNING msg_id, sent_at, sent_by, delivered_at, payload::bytea $e$
      INTO out_val;
      RETURN out_val;
END;
$$;

CREATE OR REPLACE FUNCTION pg_mq_rebuild_triggers() returns int
LANGUAGE plpgsql AS $$
DECLARE 
    cat_val pg_mq_config_catalog%ROWTYPE;
    retval int;
BEGIN
    retval := 0;
    FOR cat_val IN  SELECT * FROM pg_mq_config_catalog 
    LOOP
       EXECUTE 'CREATE TRIGGER pg_mq_notify
         AFTER INSERT ON ' || quote_ident(t_table_name) || '
         FOR EACH ROW EXECUTE PROCEDURE pg_mq_trigger_notify()';
       retval := retval + 1;
    END LOOP;
    RETURN retval;
END;
$$;
