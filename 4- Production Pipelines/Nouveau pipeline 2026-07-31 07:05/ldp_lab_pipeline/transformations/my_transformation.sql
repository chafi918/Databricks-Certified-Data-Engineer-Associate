CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Commandes brutes — aucune transformation"
AS SELECT
     *,
     _metadata.file_name               AS _source_file,
     _metadata.file_modification_time  AS _file_modified_at,
     current_timestamp()               AS _ingested_at
   FROM STREAM read_files(
     '/Volumes/workspace/ldp_lab/landing/orders/',
     format => 'json'
   );

CREATE OR REFRESH STREAMING TABLE bronze_customers_cdc
COMMENT "Événements CDC clients — bruts"
AS SELECT
     *,
     _metadata.file_name               AS _source_file,
     _metadata.file_modification_time  AS _file_modified_at,
     current_timestamp()               AS _ingested_at
   FROM STREAM read_files(
     '/Volumes/workspace/ldp_lab/landing/customers_cdc/',
     format => 'json'
   );



CREATE OR REFRESH STREAMING TABLE silver_orders
(
    CONSTRAINT valid_id_commande EXPECT (id_commande IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_montant EXPECT (montant > 0),
    CONSTRAINT valid_date_commande EXPECT (date_commande is not null) on violation fail update
)
COMMENT "Commandes Silver — avec nettoyage des données"
AS SELECT
     cast(id_commande as bigint) as id_commande,
     cast(date_commande as timestamp) as date_commande,
     upper(trim(pays)) as pays,
     cast(montant as decimal(12,2)) as montant,
     id_client,
     canal,
    _ingested_at                       AS _bronze_at,
     current_timestamp()                AS _silver_at
   FROM STREAM bronze_orders;

CREATE OR REFRESH STREAMING TABLE silver_orders_rejets
COMMENT "Commandes Silver — commandes rejetés"
AS SELECT
     *,
     current_timestamp as _rejected_at
   FROM STREAM bronze_orders
   where id_commande is null;



CREATE OR REFRESH STREAMING TABLE silver_customers
COMMENT "Clients — état courant (SCD 1)";

CREATE FLOW customers_cdc AS AUTO CDC INTO silver_customers
FROM STREAM bronze_customers_cdc
KEYS (id_client)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY lsn
COLUMNS * EXCEPT (op, lsn, ts, _source_file, _file_modified_at, _ingested_at)
STORED AS SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE silver_customers_history
COMMENT "Clients — historique des versions (SCD 2)";

CREATE FLOW customers_cdc_history AS AUTO CDC INTO silver_customers_history
FROM STREAM bronze_customers_cdc
KEYS (id_client)
APPLY AS DELETE WHEN op = 'd'
SEQUENCE BY lsn
COLUMNS * EXCEPT (op, lsn, ts, _source_file, _file_modified_at, _ingested_at)
STORED AS SCD TYPE 2
TRACK HISTORY ON segment, ville;


CREATE OR REFRESH PRIVATE MATERIALIZED VIEW enriched_orders
COMMENT "Commandes enrichies du profil client courant"
AS SELECT
     so.*,
     coalesce(sc.segment, 'inconnu') AS segment,
     coalesce(sc.ville,   'inconnue') AS ville_client
   FROM silver_orders so
   LEFT JOIN silver_customers sc
     ON so.id_client = sc.id_client;

CREATE OR REFRESH MATERIALIZED VIEW gold_ca_par_pays_segment
COMMENT "CA et volume par pays et segment"
AS SELECT
     pays,
     segment,
     sum(montant)  AS total_ca,
     count(*)      AS total_commandes,
     round(avg(montant), 2) AS panier_moyen
   FROM enriched_orders
   GROUP BY pays, segment;


CREATE OR REFRESH MATERIALIZED VIEW gold_ca_mensuel
COMMENT "CA mensuel par canal"
AS SELECT
     date_trunc('month', date_commande) AS mois,
     canal,
     sum(montant) AS total_ca,
     count(*)     AS total_commandes
   FROM enriched_orders
   GROUP BY mois, canal;