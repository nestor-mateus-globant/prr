

use role SECURITYADMIN;

--schema level roles
create role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.reader.role";
create role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.writer.role";
create role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.owner.role";
create role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role";

grant role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.writer.role" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role";
grant role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role" to user AIRFLOW;
grant usage on warehouse AIRFLOW_WAREHOUSE to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.owner.role";
grant usage on warehouse AIRFLOW_WAREHOUSE to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role";

grant usage on database "RAW" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.owner.role";
grant usage on database "RAW" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.reader.role";
grant usage on database "RAW" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.writer.role";
grant usage on database "RAW" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role";

grant create schema on database "RAW" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.owner.role";
use role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.owner.role";
use database RAW;
create schema STRIPE;
use schema STRIPE;

grant usage on schema "RAW"."STRIPE" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role";
grant create table, create view, create stage on schema "RAW"."STRIPE" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.service.role";

grant usage on schema "RAW"."STRIPE" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.writer.role";
grant insert, update on all tables in schema "RAW"."STRIPE" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.writer.role";

grant usage on schema "RAW"."STRIPE" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.reader.role";
grant select on all tables in schema "RAW"."STRIPE" to role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.reader.role";