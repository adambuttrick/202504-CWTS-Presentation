COPY (
    WITH Constants AS (
        SELECT
            'proc_openalex_works_affiliation_ingest' AS openalex_process_id,
            'proc_crossref_data_file_full_ingest' AS crossref_process_id,
            '<NULL_AUTHOR_NAME_CONTENT>' AS null_author_content,
            '<NULL_AFFILIATION_CONTENT>' AS null_affiliation_content,
            '<NULL_ROR_ID_CONTENT>' AS null_ror_id_content,
            'author_name' AS author_type,
            'affiliation' AS affiliation_type,
            'ror_id' AS ror_type,
            'has_author' AS author_rel,
            'has_affiliation' AS affiliation_rel,
            'identified_by' AS ror_rel
    ),
    AuthorAssertions AS (
        SELECT
            rvr.record_id,
            rvr.value_id as author_value_id,
            v.value_content as author_name
        FROM record_value_relationships rvr
        JOIN values v ON rvr.value_id = v.value_id
        CROSS JOIN Constants c
        WHERE rvr.relationship_type = c.author_rel
          AND v.value_type = c.author_type
    ),
    AffiliationLinkAssertions AS (
        SELECT
            aa.record_id,
            aa.author_value_id,
            aa.author_name,
            vvr.process_id,
            vvr.target_value_id AS affiliation_value_id,
            v.value_content     AS affiliation_value_content,
            v.value_type        AS affiliation_value_type
        FROM value_value_relationships vvr
        JOIN values v ON vvr.target_value_id = v.value_id
        JOIN AuthorAssertions aa ON vvr.source_value_id = aa.author_value_id
        CROSS JOIN Constants c
        WHERE vvr.relationship_type = c.affiliation_rel
          AND v.value_type = c.affiliation_type
          AND vvr.process_id IN (c.openalex_process_id, c.crossref_process_id)
    ),
    RorLinkAssertions AS (
        SELECT
            ala.record_id,
            ala.author_value_id,
            ala.affiliation_value_id,
            vvr_ror.process_id,
            vvr_ror.target_value_id AS ror_value_id,
            v_ror.value_content     AS ror_value_content,
            v_ror.value_type        AS ror_value_type
        FROM value_value_relationships vvr_ror
        JOIN values v_ror ON vvr_ror.target_value_id = v_ror.value_id
        JOIN AffiliationLinkAssertions ala ON vvr_ror.source_value_id = ala.affiliation_value_id
        CROSS JOIN Constants c
        WHERE vvr_ror.relationship_type = c.ror_rel
          AND v_ror.value_type = c.ror_type
          AND vvr_ror.process_id IN (c.openalex_process_id, c.crossref_process_id)
    ),
    AffiliationConflictSummary AS (
        SELECT
            record_id,
            author_value_id,
            affiliation_value_id,
            MAX(CASE WHEN ala.process_id = c.openalex_process_id AND ala.affiliation_value_content != c.null_affiliation_content THEN 1 ELSE 0 END) AS has_oa_non_null_affil,
            MAX(CASE WHEN ala.process_id = c.crossref_process_id AND ala.affiliation_value_content = c.null_affiliation_content THEN 1 ELSE 0 END) AS has_cr_explicit_null_affil,
            MAX(CASE WHEN ala.process_id = c.crossref_process_id THEN 1 ELSE 0 END) AS has_cr_any_affil_assertion
        FROM AffiliationLinkAssertions ala
        CROSS JOIN Constants c
        GROUP BY record_id, author_value_id, affiliation_value_id
    ),
    RorConflictSummary AS (
         SELECT
             record_id,
             author_value_id,
             affiliation_value_id,
             MAX(CASE WHEN rla.process_id = c.openalex_process_id AND rla.ror_value_content != c.null_ror_id_content THEN 1 ELSE 0 END) AS has_oa_non_null_ror,
             MAX(CASE WHEN rla.process_id = c.crossref_process_id AND rla.ror_value_content = c.null_ror_id_content THEN 1 ELSE 0 END) AS has_cr_explicit_null_ror,
             MAX(CASE WHEN rla.process_id = c.crossref_process_id THEN 1 ELSE 0 END) AS has_cr_any_ror_assertion
         FROM RorLinkAssertions rla
         CROSS JOIN Constants c
         GROUP BY record_id, author_value_id, affiliation_value_id
     ),
    ConflictedLinks AS (
        SELECT record_id, author_value_id, affiliation_value_id
        FROM AffiliationConflictSummary
        WHERE has_oa_non_null_affil = 1
          AND (has_cr_explicit_null_affil = 1 OR has_cr_any_affil_assertion = 0)
        UNION
        SELECT record_id, author_value_id, affiliation_value_id
        FROM RorConflictSummary
        WHERE has_oa_non_null_ror = 1
          AND (has_cr_explicit_null_ror = 1 OR has_cr_any_ror_assertion = 0)
    )

    SELECT DISTINCT
        rec.doi,
        oa_affil.author_name,
        oa_affil.affiliation_value_content AS affiliation,
        oa_ror.ror_value_content           AS ror_id
    FROM ConflictedLinks cl
    JOIN records rec ON cl.record_id = rec.record_id
    CROSS JOIN Constants c
    JOIN AffiliationLinkAssertions oa_affil
         ON cl.record_id = oa_affil.record_id
         AND cl.author_value_id = oa_affil.author_value_id
         AND cl.affiliation_value_id = oa_affil.affiliation_value_id
         AND oa_affil.process_id = c.openalex_process_id
         AND oa_affil.affiliation_value_content != c.null_affiliation_content
    LEFT JOIN AffiliationLinkAssertions cr_affil
         ON cl.record_id = cr_affil.record_id
         AND cl.author_value_id = cr_affil.author_value_id
         AND cl.affiliation_value_id = cr_affil.affiliation_value_id
         AND cr_affil.process_id = c.crossref_process_id
    LEFT JOIN RorLinkAssertions oa_ror
         ON cl.record_id = oa_ror.record_id
         AND cl.author_value_id = oa_ror.author_value_id
         AND cl.affiliation_value_id = oa_ror.affiliation_value_id
         AND oa_ror.process_id = c.openalex_process_id
         AND oa_ror.ror_value_content != c.null_ror_id_content
    WHERE
        NOT (
            cr_affil.affiliation_value_content IS NOT NULL
            AND cr_affil.affiliation_value_content != c.null_affiliation_content
            AND POSITION(oa_affil.affiliation_value_content IN cr_affil.affiliation_value_content) > 0
        )
)
TO 'in_openalex_not_in_crossref.csv'
(HEADER, DELIMITER ',');