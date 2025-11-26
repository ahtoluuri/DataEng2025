CREATE ROLE role_openmetadata;

CREATE USER service_openmetadata IDENTIFIED WITH sha256_password BY 'omd_very_secret_password';

GRANT role_openmetadata TO service_openmetadata;

GRANT SELECT, SHOW ON system.* to role_openmetadata;

GRANT SELECT ON dataeng.* TO role_openmetadata;
GRANT SELECT ON citibike.* TO role_openmetadata;

CREATE ROLE role_superset_full;

CREATE USER service_superset_full IDENTIFIED WITH sha256_password BY 'superset_very_secret_password';

GRANT role_superset_full TO service_superset_full;

GRANT SELECT ON dataeng.* TO role_superset_full;
GRANT SELECT ON citibike.* TO role_superset_full;
