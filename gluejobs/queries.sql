SELECT *
FROM c360view_stage.customer_pqt c
limit 10;




SELECT g.client_id,a.account_id, cd.card_id,
CASE
WHEN cd.card_id IS NOT NULL THEN 'CARD'
WHEN a.account_id IS NOT NULL THEN 'CHK_ACCOUNT'
END AS disp_type,
COALESCE( cd.card_id, a.account_id ) AS disposition_id,
COALESCE( cd.iss_date, a.cr_date ) AS acquisition_date
FROM customer_pqt c
JOIN gbank_pqt g ON c.client_id = g.client_id
LEFT JOIN account_pqt a ON g.account_id = a.account_id
LEFT JOIN card_pqt cd ON g.disp_id = cd.disp_id
ORDER BY acquisition_date ASC
