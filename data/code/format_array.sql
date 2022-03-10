create function format_array(p_array anyarray)
  returns text
as
$$
  select translate(p_array::text, '{}', '[]');
$$
language sql;
