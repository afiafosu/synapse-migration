CREATE OR REPLACE TABLE `main`.`gold`.`person_contactability`
( 
	`business_entity_id` INT,
	`preferred_email`  STRING,
	`email_domain`  STRING,
	`email_domain_category`  STRING,
	`preferred_phone_e164`  STRING,
	`preferred_phone_type`  STRING,
	`has_password` BOOLEAN,
	`password_hash_length` INT,
	`password_salt_length` INT,
	`has_email` int  NOT NULL,
	`has_phone` int  NOT NULL
);
