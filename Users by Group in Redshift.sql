select
  groups_users.group_name             as group_name,
  groups_users.array_user_ids[index]  as user_id,
  pg_user.usename                     as user_name
from
  (
    select
      array_user_ids,
      generate_series( 1, array_upper(array_user_ids, 1) ) as index,
      group_name
    from
    (
      select
        grolist as array_user_ids,
        groname as group_name
      from
        pg_group
    ) as groups
  ) as groups_users
join pg_user on array_user_ids[index] = pg_user.usesysid;