-- this is the reusable semantic layer
SELECT *, 
       CASE WHEN UPPER(transactiontype) = 'PURCHASE'
                 AND transnumber = originating_transnumber 
            THEN 'PURCHASE'
            WHEN UPPER(transactiontype) = 'PURCHASE'
                 AND transnumber <> originating_transnumber
            THEN 'REPOSTING'
            WHEN UPPER(transactiontype) = 'RETURN'
            THEN 'RETURN'
            WHEN UPPER(transactiontype) = 'EXCHANGE'
            THEN 'EXCHANGE'
            WHEN UPPER(transactiontype) = 'VOID'
            THEN 'VOID'
            WHEN UPPER(transactiontype) in ('REDEEM', 'COMMIT REDEEM')
            THEN 'REDEEM'
            ELSE transactiontype
        END AS normalized_event,

        CASE WHEN UPPER(transactiontype) = 'PURCHASE'
                 AND transnumber = originating_transnumber 
             THEN 1
             ELSE 0
        END AS root_purchase_flag,

        CASE WHEN TRIM(COALESCE(original_transaction_num, '')) <> ''
             THEN original_transaction_num
             WHEN TRIM(COALESCE(originating_transnumber, '')) <> ''
             THEN originating_transnumber
             ELSE transnumber
        END AS predecessor_transnumber

FROM base_extract