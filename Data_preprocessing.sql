-- ============================================================
-- MIMIC-III v1.4 | 6-hour sepsis early-warning rich features
-- Output:
--   - work.features6h_rich (wide feature table)
--   - CSVs: features6h_rich_train.csv / features6h_rich_test.csv
-- ============================================================

SET search_path TO mimiciii, public;
CREATE SCHEMA IF NOT EXISTS work;

-- ADULT FIRST-ICU COHORT  (temp_cohort → work.cohort_ctx)
DROP TABLE IF EXISTS temp_cohort;
CREATE TEMP TABLE temp_cohort AS
WITH first_icu AS (
  SELECT
    i.subject_id,
    i.hadm_id,
    i.icustay_id,
    i.intime,
    i.outtime,
    i.dbsource,
    ROW_NUMBER() OVER (PARTITION BY i.subject_id ORDER BY i.intime, i.icustay_id) AS rn
  FROM icustays i
),
adult_first AS (
  SELECT
    f.subject_id,
    f.hadm_id,
    f.icustay_id,
    f.intime,
    f.outtime,
    f.dbsource,
    a.admittime,
    a.dischtime,
    p.dob,
    CASE
      WHEN a.admittime >= p.dob + INTERVAL '89 years' THEN 90.0
      ELSE EXTRACT(EPOCH FROM (a.admittime - p.dob)) / 31557600.0
    END AS age_years_raw
  FROM first_icu f
  JOIN admissions a ON a.hadm_id   = f.hadm_id
  JOIN patients  p ON p.subject_id = f.subject_id
  WHERE f.rn = 1
)
SELECT
  subject_id,
  hadm_id,
  icustay_id,
  dbsource,
  intime,
  outtime,
  admittime,
  dischtime,
  dob,
  age_years_raw,
  FLOOR(age_years_raw + 0.5)::int AS age_years
FROM adult_first
WHERE age_years_raw >= 18.0;  

-- Persist as cohort context
DROP TABLE IF EXISTS work.cohort_ctx;
CREATE TABLE work.cohort_ctx AS
SELECT * FROM temp_cohort;

CREATE INDEX IF NOT EXISTS ix_work_cohort_ctx_ids ON work.cohort_ctx(subject_id, hadm_id, icustay_id);
CREATE INDEX IF NOT EXISTS ix_work_cohort_ctx_hadm ON work.cohort_ctx(hadm_id);
ANALYZE work.cohort_ctx;



-- SUSPICION OF INFECTION (temp_suspicion → work.suspicion_tsusp)
--    culture→abx (72h) OR abx→culture (24h)
DROP TABLE IF EXISTS temp_suspicion;
CREATE TEMP TABLE temp_suspicion AS
WITH names(drug_pat) AS (
  VALUES
    ('cef'),('ceph'),('ceftriaxone'),('ceftazidime'),('cefepime'),
    ('ampicillin'),('amoxicillin'),('piperacillin'),('tazobactam'),
    ('vancomycin'),('meropenem'),('imipenem'),('ertapenem'),('aztreonam'),
    ('linezolid'),('daptomycin'),
    ('levofloxacin'),('ciprofloxacin'),('moxifloxacin'),
    ('gentamicin'),('tobramycin'),('amikacin'),
    ('metronidazole'),('clindamycin'),
    ('trimethoprim'),('sulfamethoxazole'),('bactrim')
),
abx AS (
  SELECT p.hadm_id, p.startdate::timestamp AS abx_time
  FROM prescriptions p
  WHERE p.hadm_id IS NOT NULL
    AND EXISTS (SELECT 1 FROM names n WHERE LOWER(p.drug) LIKE '%' || n.drug_pat || '%')
),
cult AS (
  SELECT hadm_id, charttime::timestamp AS cult_time
  FROM microbiologyevents
  WHERE hadm_id IS NOT NULL
),
pairs AS (
  -- culture → abx within 72h
  SELECT c.hadm_id, LEAST(c.cult_time, a.abx_time) AS tsusp
  FROM cult c
  JOIN abx a USING (hadm_id)
  WHERE a.abx_time BETWEEN c.cult_time AND c.cult_time + INTERVAL '72 hour'
  UNION ALL
  -- abx → culture within 24h
  SELECT a.hadm_id, LEAST(c.cult_time, a.abx_time) AS tsusp
  FROM abx a
  JOIN cult c USING (hadm_id)
  WHERE c.cult_time BETWEEN a.abx_time AND a.abx_time + INTERVAL '24 hour'
)
SELECT hadm_id, MIN(tsusp) AS tsusp
FROM pairs
GROUP BY hadm_id;

DROP TABLE IF EXISTS work.suspicion_tsusp;
CREATE TABLE work.suspicion_tsusp AS
SELECT * FROM temp_suspicion;

CREATE INDEX IF NOT EXISTS ix_work_susp_tsusp ON work.suspicion_tsusp(hadm_id);
ANALYZE work.suspicion_tsusp;



-- HOURLY GRID & HOURLY SOFA (temp_hours, temp_sofa_hourly)
-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_ce_icu_item_time ON mimiciii.chartevents(icustay_id, itemid, charttime);
CREATE INDEX IF NOT EXISTS ix_le_hadm_item_time ON mimiciii.labevents(hadm_id, itemid, charttime);

-- Hourly grid per ICU stay
DROP TABLE IF EXISTS temp_hours;
CREATE TEMP TABLE temp_hours AS
SELECT c.subject_id, c.hadm_id, c.icustay_id,
       generate_series(date_trunc('hour', c.intime),
                       date_trunc('hour', c.outtime),
                       interval '1 hour') AS t
FROM work.cohort_ctx c;

CREATE INDEX ON temp_hours(icustay_id, t);

-- Item ID lookup for labs
DROP TABLE IF EXISTS temp_lab_ids;
CREATE TEMP TABLE temp_lab_ids AS
SELECT itemid, 'platelet' AS kind FROM d_labitems WHERE LOWER(label) LIKE 'platelet%'
UNION ALL
SELECT itemid, 'bili_total' FROM d_labitems WHERE LOWER(label) LIKE 'bilirubin%' AND LOWER(label) NOT LIKE '%direct%'
UNION ALL
SELECT itemid, 'creat' FROM d_labitems WHERE LOWER(label) LIKE 'creatinine%'
UNION ALL
SELECT itemid, 'pao2' FROM d_labitems WHERE LOWER(label) LIKE 'po2%';

-- Item ID lookup for charts
DROP TABLE IF EXISTS temp_chart_ids;
CREATE TEMP TABLE temp_chart_ids AS
SELECT itemid, 'fio2'  AS kind FROM d_items WHERE LOWER(label) LIKE 'fio2%'
UNION ALL
SELECT itemid, 'spo2'  FROM d_items WHERE LOWER(label) LIKE 'spo2%' OR LOWER(label) LIKE 'o2 saturation%'
UNION ALL
SELECT itemid, 'map'   FROM d_items WHERE LOWER(label) LIKE 'map%' AND LOWER(category) NOT LIKE '%resp%'
UNION ALL
SELECT itemid, 'gcs'   FROM d_items WHERE LOWER(label) LIKE 'gcs total%';

-- Restrict chartevents/labevents to cohort windows
DROP TABLE IF EXISTS temp_ce_small;
CREATE TEMP TABLE temp_ce_small AS
SELECT c.icustay_id, ce.charttime, ce.itemid, ce.valuenum
FROM work.cohort_ctx c
JOIN chartevents ce ON ce.icustay_id = c.icustay_id
JOIN temp_chart_ids ids ON ids.itemid = ce.itemid
WHERE ce.valuenum IS NOT NULL
  AND ce.charttime BETWEEN (c.intime - INTERVAL '48 hour') AND c.outtime;

CREATE INDEX ON temp_ce_small(icustay_id, itemid, charttime);

DROP TABLE IF EXISTS temp_le_small;
CREATE TEMP TABLE temp_le_small AS
SELECT c.hadm_id, l.charttime, l.itemid, l.valuenum
FROM work.cohort_ctx c
JOIN labevents l ON l.hadm_id = c.hadm_id
JOIN temp_lab_ids ids ON ids.itemid = l.itemid
WHERE l.valuenum IS NOT NULL
  AND l.charttime BETWEEN (c.admittime - INTERVAL '48 hour') AND c.dischtime;

CREATE INDEX ON temp_le_small(hadm_id, itemid, charttime);

-- FiO2 / SpO2 / MAP / GCS last 24h
-- (same as you had; omitted extra comments to save space)
DROP TABLE IF EXISTS temp_fio2;
CREATE TEMP TABLE temp_fio2 AS
SELECT h.icustay_id, h.t,
       (SELECT ce.valuenum
        FROM temp_ce_small ce
        JOIN temp_chart_ids ids ON ids.itemid = ce.itemid AND ids.kind = 'fio2'
        WHERE ce.icustay_id = h.icustay_id
          AND ce.charttime <= h.t
          AND ce.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY ce.charttime DESC
        LIMIT 1) AS fio2_pct
FROM temp_hours h;

DROP TABLE IF EXISTS temp_spo2;
CREATE TEMP TABLE temp_spo2 AS
SELECT h.icustay_id, h.t,
       (SELECT ce.valuenum
        FROM temp_ce_small ce
        JOIN temp_chart_ids ids ON ids.itemid = ce.itemid AND ids.kind = 'spo2'
        WHERE ce.icustay_id = h.icustay_id
          AND ce.charttime <= h.t
          AND ce.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY ce.charttime DESC
        LIMIT 1) AS spo2_pct
FROM temp_hours h;

DROP TABLE IF EXISTS temp_map;
CREATE TEMP TABLE temp_map AS
SELECT h.icustay_id, h.t,
       (SELECT ce.valuenum
        FROM temp_ce_small ce
        JOIN temp_chart_ids ids ON ids.itemid = ce.itemid AND ids.kind = 'map'
        WHERE ce.icustay_id = h.icustay_id
          AND ce.charttime <= h.t
          AND ce.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY ce.charttime DESC
        LIMIT 1) AS map_mmHg
FROM temp_hours h;

DROP TABLE IF EXISTS temp_gcs;
CREATE TEMP TABLE temp_gcs AS
SELECT h.icustay_id, h.t,
       (SELECT ce.valuenum
        FROM temp_ce_small ce
        JOIN temp_chart_ids ids ON ids.itemid = ce.itemid AND ids.kind = 'gcs'
        WHERE ce.icustay_id = h.icustay_id
          AND ce.charttime <= h.t
          AND ce.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY ce.charttime DESC
        LIMIT 1) AS gcs_total
FROM temp_hours h;

-- Platelets / Bilirubin / Creatinine / PaO2
DROP TABLE IF EXISTS temp_platelets;
CREATE TEMP TABLE temp_platelets AS
SELECT h.icustay_id, h.t,
       (SELECT l.valuenum
        FROM temp_le_small l
        JOIN temp_lab_ids ids ON ids.itemid = l.itemid AND ids.kind = 'platelet'
        WHERE l.hadm_id = h.hadm_id
          AND l.charttime <= h.t
          AND l.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY l.charttime DESC
        LIMIT 1) AS platelets
FROM temp_hours h;

DROP TABLE IF EXISTS temp_bili;
CREATE TEMP TABLE temp_bili AS
SELECT h.icustay_id, h.t,
       (SELECT l.valuenum
        FROM temp_le_small l
        JOIN temp_lab_ids ids ON ids.itemid = l.itemid AND ids.kind = 'bili_total'
        WHERE l.hadm_id = h.hadm_id
          AND l.charttime <= h.t
          AND l.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY l.charttime DESC
        LIMIT 1) AS bilirubin_mgdl
FROM temp_hours h;

DROP TABLE IF EXISTS temp_creat;
CREATE TEMP TABLE temp_creat AS
SELECT h.icustay_id, h.t,
       (SELECT l.valuenum
        FROM temp_le_small l
        JOIN temp_lab_ids ids ON ids.itemid = l.itemid AND ids.kind = 'creat'
        WHERE l.hadm_id = h.hadm_id
          AND l.charttime <= h.t
          AND l.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY l.charttime DESC
        LIMIT 1) AS creat_mgdl
FROM temp_hours h;

DROP TABLE IF EXISTS temp_pao2;
CREATE TEMP TABLE temp_pao2 AS
SELECT h.icustay_id, h.t,
       (SELECT l.valuenum
        FROM temp_le_small l
        JOIN temp_lab_ids ids ON ids.itemid = l.itemid AND ids.kind = 'pao2'
        WHERE l.hadm_id = h.hadm_id
          AND l.charttime <= h.t
          AND l.charttime >  h.t - INTERVAL '24 hour'
        ORDER BY l.charttime DESC
        LIMIT 1) AS pao2_mmHg
FROM temp_hours h;

-- Urine last 24h
DROP TABLE IF EXISTS temp_urine24;
CREATE TEMP TABLE temp_urine24 AS
SELECT h.icustay_id, h.t,
       (SELECT SUM(o.value)
        FROM outputevents o
        WHERE o.icustay_id = h.icustay_id
          AND o.charttime >  h.t - INTERVAL '24 hour'
          AND o.charttime <= h.t
          AND o.value IS NOT NULL
       ) AS urine_24h_ml
FROM temp_hours h;

-- Pressors on/off
DROP TABLE IF EXISTS temp_pressor_names;
CREATE TEMP TABLE temp_pressor_names AS
SELECT unnest(ARRAY[
  'norepinephrine','noradrenaline','epinephrine','adrenaline',
  'dopamine','dobutamine','vasopressin'
]) AS drugname;

DROP TABLE IF EXISTS temp_pressors;
CREATE TEMP TABLE temp_pressors AS
WITH mv AS (
  SELECT i.icustay_id,
         i.starttime,
         COALESCE(i.endtime, i.starttime + INTERVAL '1 hour') AS endtime,
         LOWER(d.label) AS druglabel
  FROM inputevents_mv i
  JOIN d_items d ON d.itemid = i.itemid
  WHERE EXISTS (SELECT 1 FROM temp_pressor_names n
                WHERE LOWER(d.label) LIKE n.drugname || '%')
),
cv AS (
  SELECT i.icustay_id,
         i.charttime AS starttime,
         i.charttime + INTERVAL '1 hour' AS endtime,
         LOWER(d.label) AS druglabel
  FROM inputevents_cv i
  JOIN d_items d ON d.itemid = i.itemid
  WHERE EXISTS (SELECT 1 FROM temp_pressor_names n
                WHERE LOWER(d.label) LIKE n.drugname || '%')
),
allp AS (
  SELECT * FROM mv
  UNION ALL
  SELECT * FROM cv
)
SELECT
  h.icustay_id,
  h.t,
  EXISTS (
    SELECT 1
    FROM allp p
    WHERE p.icustay_id = h.icustay_id
      AND p.starttime <= h.t
      AND (p.endtime IS NULL OR p.endtime > h.t)
  ) AS pressor_on
FROM temp_hours h;

-- Build hourly SOFA subscores + total
DROP TABLE IF EXISTS temp_sofa_hourly;
CREATE TEMP TABLE temp_sofa_hourly AS
WITH base AS (
  SELECT
    h.subject_id, h.hadm_id, h.icustay_id, h.t,
    pl.platelets,
    bi.bilirubin_mgdl,
    cr.creat_mgdl,
    pa.pao2_mmHg,
    fi.fio2_pct,
    sp.spo2_pct,
    mp.map_mmHg,
    gc.gcs_total,
    ur.urine_24h_ml,
    pr.pressor_on
  FROM temp_hours h
  LEFT JOIN temp_platelets pl USING (icustay_id, t)
  LEFT JOIN temp_bili     bi USING (icustay_id, t)
  LEFT JOIN temp_creat    cr USING (icustay_id, t)
  LEFT JOIN temp_pao2     pa USING (icustay_id, t)
  LEFT JOIN temp_fio2     fi USING (icustay_id, t)
  LEFT JOIN temp_spo2     sp USING (icustay_id, t)
  LEFT JOIN temp_map      mp USING (icustay_id, t)
  LEFT JOIN temp_gcs      gc USING (icustay_id, t)
  LEFT JOIN temp_urine24  ur USING (icustay_id, t)
  LEFT JOIN temp_pressors pr USING (icustay_id, t)
),
resp_calc AS (
  SELECT * ,
    CASE WHEN pao2_mmHg IS NOT NULL AND fio2_pct IS NOT NULL AND fio2_pct > 0
         THEN pao2_mmHg / (fio2_pct/100.0) END AS pf_ratio,
    CASE WHEN spo2_pct IS NOT NULL AND fio2_pct IS NOT NULL AND fio2_pct > 0
         THEN spo2_pct   / (fio2_pct/100.0) END AS sf_ratio
  FROM base
),
resp_sofa AS (
  SELECT * ,
    CASE
      WHEN (pf_ratio IS NOT NULL AND pf_ratio >= 400) OR (pf_ratio IS NULL AND sf_ratio >= 235) THEN 0
      WHEN (pf_ratio IS NOT NULL AND pf_ratio < 400  AND pf_ratio >= 300) OR (pf_ratio IS NULL AND sf_ratio < 235 AND sf_ratio >= 214) THEN 1
      WHEN (pf_ratio IS NOT NULL AND pf_ratio < 300  AND pf_ratio >= 200) OR (pf_ratio IS NULL AND sf_ratio < 214 AND sf_ratio >= 201) THEN 2
      WHEN (pf_ratio IS NOT NULL AND pf_ratio < 200  AND pf_ratio >= 100) OR (pf_ratio IS NULL AND sf_ratio < 201 AND sf_ratio >= 150) THEN 3
      WHEN (pf_ratio IS NOT NULL AND pf_ratio < 100) OR (pf_ratio IS NULL AND sf_ratio < 150) THEN 4
      ELSE NULL
    END AS sofa_resp
  FROM resp_calc
),
coag_sofa AS (
  SELECT * ,
    CASE
      WHEN platelets IS NULL THEN NULL
      WHEN platelets >= 150 THEN 0
      WHEN platelets >= 100 THEN 1
      WHEN platelets >= 50  THEN 2
      WHEN platelets >= 20  THEN 3
      ELSE 4
    END AS sofa_coag
  FROM resp_sofa
),
liver_sofa AS (
  SELECT * ,
    CASE
      WHEN bilirubin_mgdl IS NULL THEN NULL
      WHEN bilirubin_mgdl < 1.2  THEN 0
      WHEN bilirubin_mgdl < 2.0  THEN 1
      WHEN bilirubin_mgdl < 6.0  THEN 2
      WHEN bilirubin_mgdl < 12.0 THEN 3
      ELSE 4
    END AS sofa_liver
  FROM coag_sofa
),
cns_sofa AS (
  SELECT * ,
    CASE
      WHEN gcs_total IS NULL THEN NULL
      WHEN gcs_total >= 15 THEN 0
      WHEN gcs_total >= 13 THEN 1
      WHEN gcs_total >= 10 THEN 2
      WHEN gcs_total >= 6  THEN 3
      ELSE 4
    END AS sofa_cns
  FROM liver_sofa
),
renal_sofa AS (
  SELECT * ,
    CASE
      WHEN creat_mgdl IS NOT NULL THEN
        CASE
          WHEN creat_mgdl < 1.2 THEN 0
          WHEN creat_mgdl < 2.0 THEN 1
          WHEN creat_mgdl < 3.5 THEN 2
          WHEN creat_mgdl < 5.0 THEN 3
          ELSE 4
        END
      WHEN creat_mgdl IS NULL AND urine_24h_ml IS NOT NULL THEN
        CASE
          WHEN urine_24h_ml >= 500 THEN 0
          WHEN urine_24h_ml >= 200 THEN 3
          ELSE 4
        END
      ELSE NULL
    END AS sofa_renal
  FROM cns_sofa
),
cardio_sofa AS (
  SELECT * ,
    CASE
      WHEN pressor_on IS TRUE THEN 3
      WHEN pressor_on IS FALSE AND map_mmHg IS NOT NULL THEN CASE WHEN map_mmHg >= 70 THEN 0 ELSE 1 END
      ELSE NULL
    END AS sofa_cardio
  FROM renal_sofa
)
SELECT
  subject_id, hadm_id, icustay_id, t,
  sofa_resp, sofa_coag, sofa_liver, sofa_cns, sofa_renal, sofa_cardio,
  COALESCE(sofa_resp,0)
+ COALESCE(sofa_coag,0)
+ COALESCE(sofa_liver,0)
+ COALESCE(sofa_cns,0)
+ COALESCE(sofa_renal,0)
+ COALESCE(sofa_cardio,0) AS sofa_total
FROM cardio_sofa;

-- Persist as work.sofa_hourly
DROP TABLE IF EXISTS work.sofa_hourly;
CREATE TABLE work.sofa_hourly AS
SELECT * FROM temp_sofa_hourly;

CREATE INDEX ON work.sofa_hourly(icustay_id, t);


-- 4) SEPSIS-3 ONSET & 6h EARLY-WARNING LABELS (work.labels6h)
-- Onset (first hour with SOFA ≥2 in SI window)
DROP TABLE IF EXISTS temp_sepsis3_onset;
CREATE TEMP TABLE temp_sepsis3_onset AS
SELECT
  s.subject_id, s.hadm_id, s.icustay_id,
  MIN(s.t) FILTER (
    WHERE s.sofa_total >= 2
      AND EXISTS (
        SELECT 1 FROM temp_suspicion q
        WHERE q.hadm_id = s.hadm_id
          AND s.t BETWEEN q.tsusp - INTERVAL '48 hour'
                      AND q.tsusp + INTERVAL '24 hour'
      )
  ) AS t_onset
FROM temp_sofa_hourly s
GROUP BY s.subject_id, s.hadm_id, s.icustay_id;

-- Labels: positive if onset ∈ [t, t+6h), restricted to hours < onset
DROP TABLE IF EXISTS temp_labels6h;
CREATE TEMP TABLE temp_labels6h AS
WITH joined AS (
  SELECT h.subject_id, h.hadm_id, h.icustay_id, h.t, o.t_onset
  FROM temp_hours h
  LEFT JOIN temp_sepsis3_onset o USING (subject_id, hadm_id, icustay_id)
)
SELECT *,
  CASE
    WHEN t_onset IS NOT NULL AND t < t_onset AND t_onset < t + INTERVAL '6 hour' THEN 1
    WHEN t_onset IS NULL OR t + INTERVAL '6 hour' <= t_onset THEN 0
    ELSE NULL
  END AS label6h
FROM joined;

CREATE OR REPLACE VIEW vw_labels6h_clean AS
SELECT * FROM temp_labels6h WHERE label6h IS NOT NULL;

-- Persist labels + onset + sofa_hourly into work schema
DROP TABLE IF EXISTS work.sepsis_onset;
CREATE TABLE work.sepsis_onset AS
SELECT * FROM temp_sepsis3_onset;

DROP TABLE IF EXISTS work.labels6h;
CREATE TABLE work.labels6h AS
SELECT * FROM vw_labels6h_clean;

CREATE INDEX ON work.labels6h(icustay_id, t);



-- 5) RICH HOURLY FEATURES (cleaned S/F, etc.) → work.features6h_rich
-- Hourly rows (labels + SOFA)
DROP TABLE IF EXISTS _rx_hours;
CREATE TEMP TABLE _rx_hours AS
SELECT s.subject_id, s.hadm_id, s.icustay_id, s.t,
       l.label6h,
       s.sofa_resp, s.sofa_coag, s.sofa_liver, s.sofa_cns, s.sofa_renal, s.sofa_cardio, s.sofa_total
FROM work.sofa_hourly s
JOIN work.labels6h  l USING (subject_id, hadm_id, icustay_id, t);

CREATE INDEX ON _rx_hours(icustay_id, t);

-- ICU time windows
DROP TABLE IF EXISTS _rx_ranges;
CREATE TEMP TABLE _rx_ranges AS
SELECT icustay_id, MIN(t) AS t_min, MAX(t) AS t_max
FROM _rx_hours
GROUP BY icustay_id;

CREATE INDEX ON _rx_ranges(icustay_id);

-- Load discovered itemids
DROP TABLE IF EXISTS _ids_map;   CREATE TEMP TABLE _ids_map(itemid int PRIMARY KEY);
DROP TABLE IF EXISTS _ids_spo2;  CREATE TEMP TABLE _ids_spo2(itemid int PRIMARY KEY);
DROP TABLE IF EXISTS _ids_fio2;  CREATE TEMP TABLE _ids_fio2(itemid int PRIMARY KEY);
DROP TABLE IF EXISTS _ids_hr;    CREATE TEMP TABLE _ids_hr(itemid int PRIMARY KEY);
DROP TABLE IF EXISTS _ids_rr;    CREATE TEMP TABLE _ids_rr(itemid int PRIMARY KEY);

COPY _ids_map(itemid)  FROM '/exports/ids_map.csv'  CSV HEADER;
COPY _ids_spo2(itemid) FROM '/exports/ids_spo2.csv' CSV HEADER;
COPY _ids_fio2(itemid) FROM '/exports/ids_fio2.csv' CSV HEADER;
COPY _ids_hr(itemid)   FROM '/exports/ids_hr.csv'   CSV HEADER;
COPY _ids_rr(itemid)   FROM '/exports/ids_rr.csv'   CSV HEADER;

DROP TABLE IF EXISTS _ids_all;
CREATE TEMP TABLE _ids_all AS
SELECT itemid, 'map'  AS kind FROM _ids_map
UNION ALL SELECT itemid, 'spo2' FROM _ids_spo2
UNION ALL SELECT itemid, 'fio2' FROM _ids_fio2
UNION ALL SELECT itemid, 'hr'   FROM _ids_hr
UNION ALL SELECT itemid, 'rr'   FROM _ids_rr;

CREATE INDEX ON _ids_all(itemid);

-- Prefilter chartevents just for those ids in ICU windows
DROP TABLE IF EXISTS _rx_ce_small;
CREATE TEMP TABLE _rx_ce_small AS
SELECT ce.icustay_id, ce.charttime, ce.itemid, ce.valuenum
FROM chartevents ce
JOIN _rx_ranges r  ON r.icustay_id = ce.icustay_id
JOIN _ids_all  ids ON ids.itemid   = ce.itemid
WHERE ce.valuenum IS NOT NULL
  AND ce.charttime BETWEEN (r.t_min - INTERVAL '24 hour') AND r.t_max;

CREATE INDEX ON _rx_ce_small(icustay_id, itemid, charttime);

-- Labs: re-use your previous _rx_lab_ids / _rx_le_small / _rx_labs_wide

-- Lab item list
DROP TABLE IF EXISTS _rx_lab_ids;
CREATE TEMP TABLE _rx_lab_ids AS
SELECT itemid, 'wbc'   AS kind FROM d_labitems WHERE lower(label) LIKE 'wbc%'
UNION ALL
SELECT itemid, 'lact'  FROM d_labitems WHERE lower(label) LIKE 'lactate%'
UNION ALL
SELECT itemid, 'creat' FROM d_labitems WHERE lower(label) LIKE 'creatinine%'
UNION ALL
SELECT itemid, 'bili'  FROM d_labitems WHERE lower(label) LIKE 'bilirubin%' AND lower(label) NOT LIKE '%direct%'
UNION ALL
SELECT itemid, 'plt'   FROM d_labitems WHERE lower(label) LIKE 'platelet%'
UNION ALL
SELECT itemid, 'na'    FROM d_labitems WHERE lower(label) LIKE 'sodium%'
UNION ALL
SELECT itemid, 'k'     FROM d_labitems WHERE lower(label) LIKE 'potassium%'
UNION ALL
SELECT itemid, 'hco3'  FROM d_labitems WHERE lower(label) LIKE 'bicarbonate%' OR lower(label) LIKE 'hco3%'
UNION ALL
SELECT itemid, 'cl'    FROM d_labitems WHERE lower(label) LIKE 'chloride%'
UNION ALL
SELECT itemid, 'glu'   FROM d_labitems WHERE lower(label) LIKE 'glucose%';

-- Labs small
DROP TABLE IF EXISTS _rx_le_small;
CREATE TEMP TABLE _rx_le_small AS
SELECT l.hadm_id, l.charttime, l.itemid, l.valuenum
FROM labevents l
JOIN work.cohort_ctx c ON c.hadm_id = l.hadm_id
JOIN _rx_lab_ids ids  ON ids.itemid = l.itemid
WHERE l.valuenum IS NOT NULL
  AND l.charttime BETWEEN (c.admittime - INTERVAL '24 hour') AND c.dischtime;

CREATE INDEX ON _rx_le_small(hadm_id, itemid, charttime);

-- Labs wide
DROP TABLE IF EXISTS _rx_lab_curr;
CREATE TEMP TABLE _rx_lab_curr AS
SELECT DISTINCT ON (h.hadm_id, h.t, ids.kind)
       h.hadm_id, h.t, ids.kind, le.valuenum
FROM _rx_hours h
LEFT JOIN _rx_le_small le ON le.hadm_id = h.hadm_id
LEFT JOIN _rx_lab_ids ids ON ids.itemid = le.itemid
WHERE le.charttime >  h.t - INTERVAL '24 hour'
  AND le.charttime <= h.t
ORDER BY h.hadm_id, h.t, ids.kind, le.charttime DESC;

CREATE INDEX ON _rx_lab_curr(hadm_id, t);

DROP TABLE IF EXISTS _rx_labs_wide;
CREATE TEMP TABLE _rx_labs_wide AS
SELECT
  h.hadm_id, h.t,
  MAX(valuenum) FILTER (WHERE kind='wbc')   AS wbc,
  MAX(valuenum) FILTER (WHERE kind='lact')  AS lactate,
  MAX(valuenum) FILTER (WHERE kind='creat') AS creat_mgdl,
  MAX(valuenum) FILTER (WHERE kind='bili')  AS bili_mgdl,
  MAX(valuenum) FILTER (WHERE kind='plt')   AS platelets,
  MAX(valuenum) FILTER (WHERE kind='na')    AS na,
  MAX(valuenum) FILTER (WHERE kind='k')     AS k,
  MAX(valuenum) FILTER (WHERE kind='hco3')  AS hco3,
  MAX(valuenum) FILTER (WHERE kind='cl')    AS cl,
  MAX(valuenum) FILTER (WHERE kind='glu')   AS glucose
FROM _rx_lab_curr h
GROUP BY h.hadm_id, h.t;

CREATE INDEX ON _rx_labs_wide(hadm_id, t);

-- Urine & fluids rollups, pressor_on, vent_flag, dialysis_flag, context
-- Urine
DROP TABLE IF EXISTS _rx_urine_small;
CREATE TEMP TABLE _rx_urine_small AS
SELECT o.icustay_id, o.charttime, o.value::numeric AS value
FROM outputevents o
JOIN _rx_ranges r ON r.icustay_id = o.icustay_id
WHERE o.value IS NOT NULL
  AND o.charttime BETWEEN (r.t_min - INTERVAL '24 hour') AND r.t_max;

CREATE INDEX ON _rx_urine_small(icustay_id, charttime);

DROP TABLE IF EXISTS _rx_urine_roll;
CREATE TEMP TABLE _rx_urine_roll AS
SELECT h.icustay_id, h.t,
  SUM(u.value) FILTER (WHERE u.charttime > h.t - INTERVAL '6 hour'  AND u.charttime <= h.t)  AS urine_6h_ml,
  SUM(u.value) FILTER (WHERE u.charttime > h.t - INTERVAL '12 hour' AND u.charttime <= h.t)  AS urine_12h_ml,
  SUM(u.value) FILTER (WHERE u.charttime > h.t - INTERVAL '24 hour' AND u.charttime <= h.t)  AS urine_24h_ml
FROM _rx_hours h
LEFT JOIN _rx_urine_small u ON u.icustay_id=h.icustay_id
GROUP BY h.icustay_id, h.t;

CREATE INDEX ON _rx_urine_roll(icustay_id, t);

-- Fluids
DROP TABLE IF EXISTS _rx_fluids_small;
CREATE TEMP TABLE _rx_fluids_small AS
SELECT i.icustay_id, i.starttime AS charttime, COALESCE(i.amount, 0)::numeric AS amount
FROM inputevents_mv i
JOIN _rx_ranges r ON r.icustay_id = i.icustay_id
WHERE i.amount IS NOT NULL
  AND i.starttime BETWEEN (r.t_min - INTERVAL '24 hour') AND r.t_max
UNION ALL
SELECT i.icustay_id, i.charttime, COALESCE(i.amount, 0)::numeric AS amount
FROM inputevents_cv i
JOIN _rx_ranges r ON r.icustay_id = i.icustay_id
WHERE i.amount IS NOT NULL
  AND i.charttime BETWEEN (r.t_min - INTERVAL '24 hour') AND r.t_max;

CREATE INDEX ON _rx_fluids_small(icustay_id, charttime);

DROP TABLE IF EXISTS _rx_fluids_roll;
CREATE TEMP TABLE _rx_fluids_roll AS
SELECT h.icustay_id, h.t,
  SUM(f.amount) FILTER (WHERE f.charttime > h.t - INTERVAL '6 hour'  AND f.charttime <= h.t)  AS fluids_6h_ml,
  SUM(f.amount) FILTER (WHERE f.charttime > h.t - INTERVAL '12 hour' AND f.charttime <= h.t)  AS fluids_12h_ml,
  SUM(f.amount) FILTER (WHERE f.charttime > h.t - INTERVAL '24 hour' AND f.charttime <= h.t)  AS fluids_24h_ml
FROM _rx_hours h
LEFT JOIN _rx_fluids_small f ON f.icustay_id=h.icustay_id
GROUP BY h.icustay_id, h.t;

CREATE INDEX ON _rx_fluids_roll(icustay_id, t);

-- Pressors (for pressor_on)
DROP TABLE IF EXISTS _rx_pressor_names;
CREATE TEMP TABLE _rx_pressor_names AS
SELECT unnest(ARRAY[
  'norepinephrine','noradrenaline','epinephrine','adrenaline',
  'dopamine','dobutamine','vasopressin'
]) AS drugname;

DROP TABLE IF EXISTS _rx_pressors_all;
CREATE TEMP TABLE _rx_pressors_all AS
WITH mv AS (
  SELECT i.icustay_id, i.starttime,
         COALESCE(i.endtime, i.starttime + INTERVAL '1 hour') AS endtime,
         lower(d.label) AS druglabel
  FROM inputevents_mv i
  JOIN d_items d  ON d.itemid = i.itemid
  JOIN _rx_ranges r ON r.icustay_id = i.icustay_id
  WHERE EXISTS (SELECT 1 FROM _rx_pressor_names n WHERE lower(d.label) LIKE n.drugname || '%')
    AND i.starttime <= r.t_max
    AND COALESCE(i.endtime, i.starttime + INTERVAL '1 hour') >= r.t_min - INTERVAL '24 hour'
),
cv AS (
  SELECT i.icustay_id, i.charttime AS starttime,
         i.charttime + INTERVAL '1 hour' AS endtime,
         lower(d.label) AS druglabel
  FROM inputevents_cv i
  JOIN d_items d  ON d.itemid = i.itemid
  JOIN _rx_ranges r ON r.icustay_id = i.icustay_id
  WHERE EXISTS (SELECT 1 FROM _rx_pressor_names n WHERE lower(d.label) LIKE n.drugname || '%')
    AND i.charttime BETWEEN (r.t_min - INTERVAL '24 hour') AND r.t_max
)
SELECT * FROM mv
UNION ALL
SELECT * FROM cv;

CREATE INDEX ON _rx_pressors_all(icustay_id, starttime, endtime);

DROP TABLE IF EXISTS _rx_pressor_on;
CREATE TEMP TABLE _rx_pressor_on AS
SELECT h.icustay_id, h.t,
       EXISTS (
         SELECT 1 FROM _rx_pressors_all p
         WHERE p.icustay_id=h.icustay_id AND p.starttime <= h.t AND (p.endtime IS NULL OR p.endtime > h.t)
       ) AS pressor_on
FROM _rx_hours h;

CREATE INDEX ON _rx_pressor_on(icustay_id, t);

-- Vent flag
DROP TABLE IF EXISTS _rx_vent_labels;
CREATE TEMP TABLE _rx_vent_labels AS
SELECT unnest(ARRAY['ventilator','vent mode','assist-control','SIMV','PEEP','tidal volume','respiratory rate set']) AS labelpat;

DROP TABLE IF EXISTS _rx_vent_flag;
CREATE TEMP TABLE _rx_vent_flag AS
SELECT h.icustay_id, h.t,
       EXISTS (
         SELECT 1
         FROM chartevents ce
         JOIN d_items di ON di.itemid = ce.itemid
         JOIN _rx_vent_labels vl ON lower(di.label) LIKE '%'||vl.labelpat||'%'
         WHERE ce.icustay_id = h.icustay_id
           AND ce.charttime >  h.t - INTERVAL '24 hour'
           AND ce.charttime <= h.t
       ) AS vent_flag
FROM _rx_hours h;

CREATE INDEX ON _rx_vent_flag(icustay_id, t);

-- Dialysis flag
DROP TABLE IF EXISTS _rx_dialysis_labels;
CREATE TEMP TABLE _rx_dialysis_labels AS
SELECT unnest(ARRAY['dialysis','cvvh','cvvhd','cvvhdf','hemodialysis','crrt']) AS labelpat;

DROP TABLE IF EXISTS _rx_dialysis_flag;
CREATE TEMP TABLE _rx_dialysis_flag AS
SELECT h.icustay_id, h.t,
       (
         EXISTS (
           SELECT 1
           FROM inputevents_mv mv JOIN d_items di ON di.itemid = mv.itemid
           WHERE mv.icustay_id = h.icustay_id
             AND (SELECT COUNT(*) FROM _rx_dialysis_labels dl WHERE lower(di.label) LIKE '%'||dl.labelpat||'%') > 0
             AND mv.starttime >  h.t - INTERVAL '24 hour'
             AND mv.starttime <= h.t
         )
         OR EXISTS (
           SELECT 1
           FROM inputevents_cv cv JOIN d_items di ON di.itemid = cv.itemid
           WHERE cv.icustay_id = h.icustay_id
             AND (SELECT COUNT(*) FROM _rx_dialysis_labels dl WHERE lower(di.label) LIKE '%'||dl.labelpat||'%') > 0
             AND cv.charttime >  h.t - INTERVAL '24 hour'
             AND cv.charttime <= h.t
         )
       ) AS dialysis_flag
FROM _rx_hours h;

CREATE INDEX ON _rx_dialysis_flag(icustay_id, t);

-- Context (age, sex, dbsource, hours_since_icu/tsusp)
DROP TABLE IF EXISTS _rx_context;
CREATE TEMP TABLE _rx_context AS
SELECT
  h.subject_id, h.hadm_id, h.icustay_id, h.t,
  c.dbsource,
  c.age_years, c.age_years_raw,
  p.gender AS sex,
  EXTRACT(EPOCH FROM (h.t - c.intime))/3600.0 AS hours_since_icu,
  CASE WHEN s.tsusp IS NOT NULL THEN EXTRACT(EPOCH FROM (h.t - s.tsusp))/3600.0 END AS hours_since_si
FROM _rx_hours h
JOIN work.cohort_ctx      c USING (subject_id, hadm_id, icustay_id)
LEFT JOIN work.suspicion_tsusp s USING (hadm_id)
JOIN patients             p USING (subject_id);

CREATE INDEX ON _rx_context(icustay_id, t);

-- Current vitals using discovered ids
DROP TABLE IF EXISTS _rx_map;
CREATE TEMP TABLE _rx_map AS
SELECT DISTINCT ON (h.icustay_id, h.t)
       h.icustay_id, h.t, ce.valuenum AS map_mmHg
FROM _rx_hours h
LEFT JOIN _rx_ce_small ce
  ON ce.icustay_id = h.icustay_id
 AND ce.itemid IN (SELECT itemid FROM _ids_map)
 AND ce.charttime > h.t - INTERVAL '24 hour'
 AND ce.charttime <= h.t
ORDER BY h.icustay_id, h.t, ce.charttime DESC;

CREATE INDEX ON _rx_map(icustay_id, t);

DROP TABLE IF EXISTS _rx_hr;
CREATE TEMP TABLE _rx_hr AS
SELECT DISTINCT ON (h.icustay_id, h.t)
       h.icustay_id, h.t, ce.valuenum AS hr
FROM _rx_hours h
LEFT JOIN _rx_ce_small ce
  ON ce.icustay_id = h.icustay_id
 AND ce.itemid IN (SELECT itemid FROM _ids_hr)
 AND ce.charttime > h.t - INTERVAL '24 hour'
 AND ce.charttime <= h.t
ORDER BY h.icustay_id, h.t, ce.charttime DESC;

CREATE INDEX ON _rx_hr(icustay_id, t);

DROP TABLE IF EXISTS _rx_rr;
CREATE TEMP TABLE _rx_rr AS
SELECT DISTINCT ON (h.icustay_id, h.t)
       h.icustay_id, h.t, ce.valuenum AS rr
FROM _rx_hours h
LEFT JOIN _rx_ce_small ce
  ON ce.icustay_id = h.icustay_id
 AND ce.itemid IN (SELECT itemid FROM _ids_rr)
 AND ce.charttime > h.t - INTERVAL '24 hour'
 AND ce.charttime <= h.t
ORDER BY h.icustay_id, h.t, ce.charttime DESC;

CREATE INDEX ON _rx_rr(icustay_id, t);

DROP TABLE IF EXISTS _rx_spo2;
CREATE TEMP TABLE _rx_spo2 AS
SELECT DISTINCT ON (h.icustay_id, h.t)
       h.icustay_id, h.t, ce.valuenum AS spo2_pct
FROM _rx_hours h
LEFT JOIN _rx_ce_small ce
  ON ce.icustay_id = h.icustay_id
 AND ce.itemid IN (SELECT itemid FROM _ids_spo2)
 AND ce.charttime > h.t - INTERVAL '24 hour'
 AND ce.charttime <= h.t
ORDER BY h.icustay_id, h.t, ce.charttime DESC;

CREATE INDEX ON _rx_spo2(icustay_id, t);

DROP TABLE IF EXISTS _rx_fio2;
CREATE TEMP TABLE _rx_fio2 AS
SELECT DISTINCT ON (h.icustay_id, h.t)
       h.icustay_id, h.t, ce.valuenum AS fio2_pct
FROM _rx_hours h
LEFT JOIN _rx_ce_small ce
  ON ce.icustay_id = h.icustay_id
 AND ce.itemid IN (SELECT itemid FROM _ids_fio2)
 AND ce.charttime > h.t - INTERVAL '24 hour'
 AND ce.charttime <= h.t
ORDER BY h.icustay_id, h.t, ce.charttime DESC;

CREATE INDEX ON _rx_fio2(icustay_id, t);

-- Clean FiO2 & SpO2 and compute S/F
DROP TABLE IF EXISTS _rx_fio2_norm;
CREATE TEMP TABLE _rx_fio2_norm AS
SELECT icustay_id, t,
       CASE
         WHEN fio2_pct BETWEEN 0.15 AND 1.5 THEN fio2_pct * 100.0
         WHEN fio2_pct BETWEEN 15   AND 100 THEN fio2_pct
         ELSE NULL
       END AS fio2_pct_norm
FROM _rx_fio2;

CREATE INDEX ON _rx_fio2_norm(icustay_id, t);

DROP TABLE IF EXISTS _rx_spo2_clean;
CREATE TEMP TABLE _rx_spo2_clean AS
SELECT icustay_id, t,
       CASE
         WHEN spo2_pct BETWEEN 50 AND 100 THEN spo2_pct
         ELSE NULL
       END AS spo2_pct_clean
FROM _rx_spo2;

CREATE INDEX ON _rx_spo2_clean(icustay_id, t);

-- Assemble current signals
DROP TABLE IF EXISTS _rx_signals_current;
CREATE TEMP TABLE _rx_signals_current AS
SELECT
  h.subject_id, h.hadm_id, h.icustay_id, h.t, h.label6h,
  h.sofa_resp, h.sofa_coag, h.sofa_liver, h.sofa_cns, h.sofa_renal, h.sofa_cardio, h.sofa_total,
  m.map_mmHg,
  hr.hr, rr.rr,
  sp.spo2_pct_clean       AS spo2_pct,
  fi.fio2_pct_norm        AS fio2_pct,
  lw.wbc, lw.lactate, lw.creat_mgdl, lw.bili_mgdl, lw.platelets,
  lw.na, lw.k, lw.hco3, lw.cl, lw.glucose,
  ur.urine_6h_ml, ur.urine_12h_ml, ur.urine_24h_ml,
  fl.fluids_6h_ml, fl.fluids_12h_ml, fl.fluids_24h_ml,
  (pr.pressor_on)::int     AS pressor_on,
  (vf.vent_flag)::int      AS vent_flag,
  (df.dialysis_flag)::int  AS dialysis_flag,
  cx.age_years, cx.sex, cx.dbsource,
  cx.hours_since_icu, cx.hours_since_si,
  CASE
    WHEN sp.spo2_pct_clean BETWEEN 50 AND 100
     AND fi.fio2_pct_norm  BETWEEN 15 AND 100
    THEN sp.spo2_pct_clean / (fi.fio2_pct_norm / 100.0)
  END AS sfratio
FROM _rx_hours h
LEFT JOIN _rx_map           m   USING (icustay_id, t)
LEFT JOIN _rx_hr            hr  USING (icustay_id, t)
LEFT JOIN _rx_rr            rr  USING (icustay_id, t)
LEFT JOIN _rx_spo2_clean    sp  USING (icustay_id, t)
LEFT JOIN _rx_fio2_norm     fi  USING (icustay_id, t)
LEFT JOIN _rx_labs_wide     lw  USING (hadm_id, t)
LEFT JOIN _rx_urine_roll    ur  USING (icustay_id, t)
LEFT JOIN _rx_fluids_roll   fl  USING (icustay_id, t)
LEFT JOIN _rx_pressor_on    pr  USING (icustay_id, t)
LEFT JOIN _rx_vent_flag     vf  USING (icustay_id, t)
LEFT JOIN _rx_dialysis_flag df  USING (icustay_id, t)
LEFT JOIN _rx_context       cx  USING (subject_id, hadm_id, icustay_id, t);

CREATE INDEX ON _rx_signals_current(icustay_id, t);

-- Final rich features with lags, diffs, rolling stats
DROP TABLE IF EXISTS work.features6h_rich;
CREATE TABLE work.features6h_rich AS
WITH base AS (
  SELECT sc.*,
         LAG(map_mmHg,1) OVER (PARTITION BY icustay_id ORDER BY t) AS map_lag1,
         LAG(map_mmHg,2) OVER (PARTITION BY icustay_id ORDER BY t) AS map_lag2,
         LAG(hr,1)       OVER (PARTITION BY icustay_id ORDER BY t) AS hr_lag1,
         LAG(hr,2)       OVER (PARTITION BY icustay_id ORDER BY t) AS hr_lag2,
         LAG(rr,1)       OVER (PARTITION BY icustay_id ORDER BY t) AS rr_lag1,
         LAG(rr,2)       OVER (PARTITION BY icustay_id ORDER BY t) AS rr_lag2,
         LAG(sfratio,1)  OVER (PARTITION BY icustay_id ORDER BY t) AS sfratio_lag1,
         LAG(sfratio,2)  OVER (PARTITION BY icustay_id ORDER BY t) AS sfratio_lag2,
         LAG(wbc,1)      OVER (PARTITION BY icustay_id ORDER BY t) AS wbc_lag1,
         LAG(wbc,2)      OVER (PARTITION BY icustay_id ORDER BY t) AS wbc_lag2,
         LAG(lactate,1)  OVER (PARTITION BY icustay_id ORDER BY t) AS lactate_lag1,
         LAG(lactate,2)  OVER (PARTITION BY icustay_id ORDER BY t) AS lactate_lag2
  FROM _rx_signals_current sc
),
deltas AS (
  SELECT b.*,
         (b.map_mmHg - b.map_lag1)         AS map_diff1,
         (b.map_lag1 - b.map_lag2)         AS map_diff2,
         (b.hr - b.hr_lag1)                AS hr_diff1,
         (b.hr_lag1 - b.hr_lag2)           AS hr_diff2,
         (b.rr - b.rr_lag1)                AS rr_diff1,
         (b.rr_lag1 - b.rr_lag2)           AS rr_diff2,
         (b.sfratio - b.sfratio_lag1)      AS sfratio_diff1,
         (b.sfratio_lag1 - b.sfratio_lag2) AS sfratio_diff2,
         (b.wbc - b.wbc_lag1)              AS wbc_diff1,
         (b.wbc_lag1 - b.wbc_lag2)         AS wbc_diff2,
         (b.lactate - b.lactate_lag1)      AS lactate_diff1,
         (b.lactate_lag1 - b.lactate_lag2) AS lactate_diff2
  FROM base b
),
roll AS (
  SELECT d.*,
         AVG(map_mmHg)   OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS map_mean_6h,
         STDDEV_SAMP(map_mmHg) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS map_std_6h,
         MIN(map_mmHg)   OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS map_min_6h,
         MAX(map_mmHg)   OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS map_max_6h,
         AVG(map_mmHg)   OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS map_mean_12h,
         MIN(map_mmHg)   OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS map_min_12h,
         AVG(hr)         OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS hr_mean_6h,
         STDDEV_SAMP(hr) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS hr_std_6h,
         AVG(rr)         OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS rr_mean_6h,
         STDDEV_SAMP(rr) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)  AS rr_std_6h,
         REGR_SLOPE(map_mmHg, EXTRACT(EPOCH FROM t)) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS map_slope_6h,
         REGR_SLOPE(hr,       EXTRACT(EPOCH FROM t)) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS hr_slope_6h,
         REGR_SLOPE(rr,       EXTRACT(EPOCH FROM t)) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rr_slope_6h,
         CASE WHEN COUNT(map_mmHg) OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)=0 THEN 1 ELSE 0 END AS map_miss_6h,
         CASE WHEN COUNT(hr)       OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)=0 THEN 1 ELSE 0 END AS hr_miss_6h,
         CASE WHEN COUNT(rr)       OVER (PARTITION BY icustay_id ORDER BY t ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)=0 THEN 1 ELSE 0 END AS rr_miss_6h
  FROM deltas d
)
SELECT * FROM roll;

CREATE INDEX IF NOT EXISTS ix_features6h_rich ON work.features6h_rich (icustay_id, t);

-- Train/test split by admission year 
ALTER TABLE work.features6h_rich
  ADD COLUMN IF NOT EXISTS split text;

UPDATE work.features6h_rich f
SET split = CASE
  WHEN EXTRACT(YEAR FROM a.admittime) <= 2176 THEN 'train'
  ELSE 'test'
END
FROM admissions a
WHERE a.hadm_id = f.hadm_id;

SELECT split, COUNT(*) FROM work.features6h_rich GROUP BY split ORDER BY split;



COPY (
  SELECT *
  FROM work.features6h_rich
  WHERE split = 'train'
) TO '/exports/features6h_rich_train.csv'
  WITH (FORMAT CSV, HEADER, ENCODING 'UTF8');

COPY (
  SELECT *
  FROM work.features6h_rich
  WHERE split = 'test'
) TO '/exports/features6h_rich_test.csv'
  WITH (FORMAT CSV, HEADER, ENCODING 'UTF8');

--- for bash
--docker exec -u root mimic-postgres mkdir -p /exports
--docker exec -u root mimic-postgres chown postgres:postgres /exports

--docker cp mimic-postgres:/exports/features6h_rich_train.csv "C:\Users\lyuti\Downloads\"
--docker cp mimic-postgres:/exports/features6h_rich_test.csv  "C:\Users\lyuti\Downloads\"
