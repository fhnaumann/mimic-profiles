# MIMIC pipeline

Every modification applied to the MIMIC-on-FHIR data, as an ordered sequence of
steps. One script per step, named `step<N>_<what>.py`.

| # | Script | What it does |
|---|--------|--------------|
| 1 | `step1_ndjson_to_delta.py` | NDJSON distribution → one Delta table per FHIR resource type |

**Step 1 is the only step allowed to use the Pathling library directly** — it
creates the warehouse the server reads from, so there is no server to go
through yet. Every later step goes through the Pathling server's FHIR API.

## Step 1

```bash
uv run python step1_ndjson_to_delta.py \
    --source /Users/nau025/warehouses/mimic-iv-demo \
    --target /Users/nau025/warehouses/mimic-iv-demo/delta
```

At full scale on Petrichor, `mimic-fhir/hpc/warehouse/rebuild_delta_warehouse.sh`
wraps this: it decompresses into `/scratch3` as its own job (checked against the
per-file sizes in `mimic-fhir/metrics/raw-zstd.tsv`), then runs step 1 on an
exclusive node into `spark_warehouse.new`, leaving the swap to a human.

MIMIC ships one file per profile flavour (`MimicObservationChartevents`,
`MimicMedicationAdministrationICU`, …) and Pathling stores one table per
resource type, so the demo's 30 files collapse into 13 tables. The resource
type is read from each file's first line rather than matched off the file name
— same `file_name_mapper` mechanism the Pathling MIMIC tutorial uses, without a
list of Mimic suffixes to maintain. The flavour survives on `meta.profile`,
which `../merged_profile_provisioning/` reads later.

`save_mode="overwrite"` replaces the resource tables this run writes and leaves
every other table alone, so a server's own `Library`, `ViewDefinition` and
`jobs/` content survives a data refresh.

### Codecs

`--extension` defaults to `zst`, which is what the DuckDB builds ship. **Spark
cannot read zstd**: the pyspark wheel carries no native Hadoop codecs, and the
read fails with `FAILED_READ_FILE` at execution time — *after*
`resource_types()` has already reported success. So the script decompresses
`.zst` to plain NDJSON first and points Pathling at that. Budget ~19x the
compressed size: 41 MB → 767 MB for the demo, 24 GiB → 470 GiB for the full
corpus. Without `--staging` it uses a temporary directory and deletes it on
exit; pass an explicit path at full scale to keep the staged copy for a re-run.

`gz` and `ndjson` are read in place. Prefer plain `ndjson` at full scale
regardless of what you hold: gzip is not splittable, so a 12 GiB
`MimicObservationChartevents.ndjson.gz` is one Spark task on one core, where the
uncompressed file is split across all of them.

Note that Pathling's `extension` argument globs on the **last** extension
segment only, so it is `zst`/`gz`/`ndjson` — passing `ndjson.zst` matches no
files. The script sniffs the directory itself before calling Pathling and exits
non-zero when nothing matches, rather than writing an empty warehouse.

The Pathling server serves whatever is mounted at
`/usr/share/warehouse/default`; check it matches `--target` with
`docker inspect pathling --format '{{json .Mounts}}'`.
