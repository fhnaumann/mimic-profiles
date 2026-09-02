ValueSet: MimicMedicationDispenseMergedCode
Id: mimic-medication-dispense-merged-code
Title: "MIMIC MedicationDispense code (merged sub-types)"
Description: "Union of the per-sub-type MedicationDispense medication[x] / medication[x].coding binding ValueSets (ED, ICU). Grouping ValueSet — references only, no own codes. NEVER BOUND BY ANY PROFILE: it exists so the ConceptMap that translates MedicationDispense.medication[x] in fhnaumann/mimic-iv-terminology has exactly one sourceCanonical to name, and deleting it as unused would break that map. Originally written for the MimicMedicationDispenseMerged facade profile, which was scrapped. Not part of the upstream MIMIC IG."

* include codes from valueset $MimicMedicationCodes
* include codes from valueset $GSN_VS
