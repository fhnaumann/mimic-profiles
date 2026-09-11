// Hand-maintained thesis table. Coverage values are from mapping-statistics.csv.
#let items(..entries) = stack(spacing: 1pt, ..entries)
#let coverage(value, weight) = stack(
  spacing: 1pt,
  value,
  text(size: 7pt, fill: gray)[weight: #weight],
)

#figure(
  table(
    columns: (0.55fr, 0.45fr, 1.25fr, 0.55fr, 0.65fr),
    align: (left + top, left + top, left + top, left + top, left + top),
    stroke: none,
    inset: (x: 6pt, y: 5pt),

    table.hline(),
    table.header(
      [*Stream*],
      [*Target(s)*],
      [*Target resource*],
      [*Code coverage*],
      [*Occurrence coverage*],
    ),
    table.hline(stroke: 0.5pt),

    [chartevents],
    items([LOINC], [SNOMED CT]),
    [Observation],
    coverage([48.76\%], [6.65\%]),
    coverage([58.85\%], [58.90\%]),

    [d-items],
    [SNOMED CT],
    [Procedure],
    coverage([65.60\%], [0.47\%]),
    coverage([64.64\%], [0.13\%]),

    [datetimeevents],
    [SNOMED CT],
    [Observation],
    coverage([53.52\%], [0.51\%]),
    coverage([52.16\%], [1.34\%]),

    [formulary-drug],
    items([SNOMED CT], [RxNorm]),
    items(
      [`MedicationRequest.medication[x]`],
      [`MedicationAdministration.medication[x]`],
      [`Medication.code`],
      [`MedicationDispense.medication[x]`],
    ),
    coverage([81.36\%], [7.48\%]),
    coverage([95.41\%], [5.06\%]),

    [lab-fluid],
    [SNOMED CT],
    [Specimen],
    coverage([88.88\%], [0.03\%]),
    coverage([99.82\%], [2.51\%]),

    [labevents],
    [LOINC],
    [Observation],
    coverage([78.03\%], [2.73\%]),
    coverage([90.18\%], [22.19\%]),

    [medication-etc],
    [SNOMED CT],
    [`MedicationStatement.medication[x]`],
    coverage([52.12\%], [3.59\%]),
    coverage([70.69\%], [0.56\%]),

    [medication-gsn],
    [RxNorm],
    items(
      [`MedicationDispense.medication[x]`],
      [`MedicationStatement.medication[x]`],
    ),
    coverage([83.19\%], [27.91\%]),
    coverage([91.62\%], [0.78\%]),

    [medication-icu],
    items([SNOMED CT], [RxNorm]),
    items(
      [`MedicationRequest.medication[x]`],
      [`MedicationAdministration.medication[x]`],
      [`Medication.code`],
      [`MedicationDispense.medication[x]`],
    ),
    coverage([66.04\%], [0.97\%]),
    coverage([78.15\%], [1.69\%]),

    [medication-name],
    items([SNOMED CT], [RxNorm]),
    items(
      [`MedicationRequest.medication[x]`],
      [`MedicationAdministration.medication[x]`],
      [`Medication.code`],
      [`MedicationDispense.medication[x]`],
    ),
    coverage([37.01\%], [29.49\%]),
    coverage([96.30\%], [2.94\%]),

    [medication-ndc],
    items([SNOMED CT], [RxNorm]),
    items(
      [`MedicationRequest.medication[x]`],
      [`MedicationAdministration.medication[x]`],
      [`Medication.code`],
      [`MedicationDispense.medication[x]`],
    ),
    coverage([89.13\%], [17.12\%]),
    coverage([90.93\%], [2.07\%]),

    [medication-poe-iv],
    items([SNOMED CT], [RxNorm]),
    items(
      [`MedicationRequest.medication[x]`],
      [`MedicationAdministration.medication[x]`],
      [`Medication.code`],
      [`MedicationDispense.medication[x]`],
    ),
    coverage([0.00\%], [0.01\%]),
    coverage([0.00\%], [0.07\%]),

    [micro-org],
    [SNOMED CT],
    [Observation],
    coverage([96.28\%], [1.93\%]),
    coverage([84.79\%], [0.05\%]),

    [micro-susc],
    [LOINC],
    [Observation],
    coverage([100.00\%], [0.08\%]),
    coverage([100.00\%], [0.21\%]),

    [micro-test],
    [LOINC],
    [Observation],
    coverage([65.90\%], [0.53\%]),
    coverage([68.71\%], [0.41\%]),

    [outputevents],
    [LOINC],
    [Observation],
    coverage([84.50\%], [0.21\%]),
    coverage([98.24\%], [0.80\%]),

    [spec-type],
    [SNOMED CT],
    [Specimen],
    coverage([77.88\%], [0.31\%]),
    coverage([90.79\%], [0.30\%]),

    [units],
    [UCUM],
    [`Quantity.code`],
    [#sym.dash.en],
    [#sym.dash.en],

    table.hline(),
  ),
  caption: [Coverage of streams resolved through committed mapping tables. The smaller weight value is the stream's share of all counted table-stream codes or code occurrences, respectively. The `units` stream has no warehouse occurrence counts and is excluded from both weight denominators.],
  placement: auto,
) <tab:table-stream-mappings>
