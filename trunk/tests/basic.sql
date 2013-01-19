BEGIN;
CREATE TEMPORARY TABLE test_result (
        test_name text,
        success bool
);

INSERT INTO test_result
SELECT 'Create text queue successful', count(*) = 1
  FROM pg_mq_create_queue('test_queue_text', 'text');

INSERT INTO test_result
SELECT 'Create bytea queue successful', count(*) = 1
  FROM pg_mq_create_queue('test_queue_bin', 'bytea');

INSERT INTO test_result
SELECT 'Create xml queue successful', count(*) = 1
  FROM pg_mq_create_queue('test_queue_xml', 'xml');

-- 0.2 TESTS CREATE QUEUE

INSERT INTO test_result
SELECT 'Create inet queue successful', 
        (select count(*) = 1
                    from pg_mq_create_queue('test_queue_inet', 'inet'))
  FROM pg_extension 
 where extversion >= '0.2' and extname = 'pg_message_queue';

INSERT INTO test_result
SELECT 'Create date queue successful', 
        (select count(*) = 1
                    from pg_mq_create_queue('test_queue_date', 'date'))
  FROM pg_extension 
 where extversion >= '0.2' and extname = 'pg_message_queue';

-- SEND MSG TESTS 0.1 +

INSERT INTO test_result
SELECT 'sending text message successful', count(*) = 1
  FROM pg_mq_send_message('test_queue_text', 'test_msg'::text);

INSERT INTO test_result
SELECT 'sending bytea message successful', count(*) = 1
  FROM pg_mq_send_message('test_queue_bin', 'test_msg'::bytea);

INSERT INTO test_result
SELECT 'sending xml mssage successful', count(*) = 1
  FROM pg_mq_send_message('test_queue_xml', '<?lsmb test_msg ?>'::xml);

-- 0.2 +

INSERT INTO test_result
SELECT 'sending inet message successful',
       (select count(*) = 1 
          FROM pg_mq_send_message('test_queue_inet', '::1'::inet))
  FROM pg_extension 
 where extversion >= '0.2' and extname = 'pg_message_queue';

INSERT INTO test_result
SELECT 'sending date message successful',
       (select count(*) = 1 
          FROM pg_mq_send_message('test_queue_date', '2011-01-01'::date))
  FROM pg_extension 
 where extversion >= '0.2' and extname = 'pg_message_queue';

	/* */
-- RECEIVE MSG TESTS 0.1 +


-- text queue
INSERT INTO test_result
SELECT 'text message retrieved, fifo', payload = 'test_msg'
  FROM pg_mq_get_msg_text('test_queue_text', 1);

INSERT INTO test_result
SELECT 'text message retrieved by id', 
       (select payload = 'test_msg'
         FROM pg_mq_get_msg_id_text('test_queue_text', max(msg_id)::int))
  FROM pg_mq_queue_test_queue_text;

INSERT INTO test_result
SELECT 'text message retrieved by id, binary', 
       (select payload = 'test_msg'::bytea
         FROM pg_mq_get_msg_id_bin('test_queue_text', max(msg_id)))
  FROM pg_mq_queue_test_queue_text;

-- bin queue
INSERT INTO test_result
SELECT 'binary message retrieved, fifo', payload = 'test_msg'::bytea
  FROM pg_mq_get_msg_bin('test_queue_bin', 1);

INSERT INTO test_result
SELECT 'binary message retrieved by id', 
       (select payload = 'test_msg'::bytea::text
         FROM pg_mq_get_msg_id_text('test_queue_bin', max(msg_id)))
  FROM pg_mq_queue_test_queue_bin;

INSERT INTO test_result
SELECT 'binary message retrieved by id, binary', 
       (select payload = 'test_msg'::bytea
         FROM pg_mq_get_msg_id_bin('test_queue_bin', max(msg_id)))
  FROM pg_mq_queue_test_queue_bin;

-- xml queue
INSERT INTO test_result
SELECT 'xml message retrieved, fifo', payload = '<?lsmb test_msg ?>'
  FROM pg_mq_get_msg_text('test_queue_xml', 1);

INSERT INTO test_result
SELECT 'xml message retrieved by id', 
       (select payload = '<?lsmb test_msg ?>'
         FROM pg_mq_get_msg_id_text('test_queue_xml', max(msg_id)))
  FROM pg_mq_queue_test_queue_xml;

-- inet queue

INSERT INTO test_result
SELECT 'inet message retrieved, fifo', 
       (select payload::inet = '::1'
          FROM pg_mq_get_msg_text('test_queue_inet', 1))
  FROM pg_extension 
 where extversion >= '0.2' and extname = 'pg_message_queue';


INSERT INTO test_result
SELECT 'inet message retrieved by id', 
       (select payload::inet = '::1'
         FROM pg_mq_get_msg_id_text('test_queue_inet', max(msg_id)))
  FROM pg_mq_queue_test_queue_inet
  JOIN pg_extension ON extversion > '0.2' and extname = 'pg_message_queue';

-- date queue
INSERT INTO test_result
SELECT 'date message retrieved, fifo', 
       (select payload::date = '2011-01-01'
          FROM pg_mq_get_msg_text('test_queue_date', 1))
  FROM pg_extension
 WHERE extversion >= '0.2' and extname = 'pg_message_queue';;

INSERT INTO test_result
SELECT 'date message retrieved by id', 
       (select payload::date = '2011-01-01'
         FROM pg_mq_get_msg_id_text('test_queue_date', max(msg_id)))
  FROM pg_mq_queue_test_queue_date
  JOIN pg_extension ON extversion >= '0.2' and extname = 'pg_message_queue';

-- test no more messages returned fifo

INSERT INTO test_result
SELECT 'no more text messages', count(*) = 0
  FROM pg_mq_get_msg_text('test_queue_text', 1);

INSERT INTO test_result
SELECT 'no more bytea messages', count(*) = 0
  FROM pg_mq_get_msg_bin('test_queue_bin', 1);

INSERT INTO test_result
SELECT 'no more xml messages', count(*) = 0
  FROM pg_mq_get_msg_text('test_queue_xml', 1);

INSERT INTO test_result
SELECT 'no more inet messages', 
       (select count(*) = 0 
          FROM pg_mq_get_msg_text('test_queue_inet', 1))
  FROM pg_extension 
 WHERE extversion >= '0.2' and extname = 'pg_message_queue';

INSERT INTO test_result
SELECT 'no more date messages', 
       (select count(*) = 0 
          FROM pg_mq_get_msg_text('test_queue_date', 1))
  FROM pg_extension 
 WHERE extversion >= '0.2' and extname = 'pg_message_queue';


SELECT * FROM test_result;

SELECT (select count(*) from test_result where success is true)
|| ' tests passed and '
|| (select count(*) from test_result where success is not true)
|| ' failed' as message;

ROLLBACK;
