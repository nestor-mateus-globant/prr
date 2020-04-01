
MERGE INTO {0}.{1}.{2} t1
USING {0}.{1}.{2}_temp t2 ON t1.charge_id = t2.charge_id 

WHEN MATCHED THEN UPDATE SET
    charge_id = t2.charge_id,
    created = t2.created,
    brand = t2.brand,
    country = t2.country,
    description = t2.description,
    exp_month = t2.exp_month,
    exp_year = t2.exp_year,
    fingerprint = t2.fingerprint,
    funding = t2.funding,
    iin = t2.iin,
    issuer = t2.issuer,
    last4 = t2.last4,
    address_line1_check = t2.address_line1_check,
    address_postal_code_check = t2.address_postal_code_check,
    cvc_check = t2.cvc_check
WHEN NOT MATCHED THEN INSERT (
    charge_id,
    created,
    brand,
    country,
    description,
    exp_month,
    exp_year,
    fingerprint,
    funding,
    iin,
    issuer,
    last4,
    address_line1_check,
    address_postal_code_check,
    cvc_check
)
VALUES (
    charge_id,
    created,
    brand,
    country,
    description,
    exp_month,
    exp_year,
    fingerprint,
    funding,
    iin,
    issuer,
    last4,
    address_line1_check,
    address_postal_code_check,
    cvc_check
);