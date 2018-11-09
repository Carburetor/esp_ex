CREATE OR REPLACE FUNCTION stream_notify(
  _message messages
)
RETURNS void
AS $$
DECLARE
  stream_name varchar;
  _category varchar;
  positions varchar;
BEGIN
  stream_name = _message.stream_name;
  _category = category(stream_name);
  positions = format(
    '%s/%s',
    _message.position::text,
    _message.global_position::text
  )::varchar;

  if _category != stream_name then
    PERFORM pg_notify(stream_name::text, positions);
  end if;

  PERFORM pg_notify(_category::text, positions);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION stream_notify_on_insert()
RETURNS trigger
AS $$
BEGIN
  PERFORM stream_notify(NEW::messages);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS on_insert_stream_notify on messages;
CREATE TRIGGER on_insert_stream_notify
  AFTER INSERT
  ON messages
  FOR EACH ROW
  EXECUTE PROCEDURE stream_notify_on_insert();
