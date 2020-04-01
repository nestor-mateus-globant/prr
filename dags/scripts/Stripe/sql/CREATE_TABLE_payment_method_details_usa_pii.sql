

use role "AIRFLOW_WAREHOUSE.RAW.STRIPE.pii.owner.role";

CREATE OR REPLACE TABLE raw.stripe.payment_method_details_usa_pii (
	"EXP_YEAR" numeric,
	"FUNDING" varchar,
	"ISSUER" varchar,
	"DESCRIPTION" varchar,
	"FINGERPRINT" varchar,
	"CHARGE_ID" varchar,
	"BRAND" varchar,
	"CREATED" datetime,
	"IIN" numeric,
	"LAST4" numeric,
	"EXP_MONTH" numeric,
	"COUNTRY" varchar,
	"ADDRESS_LINE1_CHECK" varchar,
	"ADDRESS_POSTAL_CODE_CHECK" varchar,
	"CVC_CHECK" varchar
)