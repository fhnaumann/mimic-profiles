ValueSet: MimicMedicationAdministrationMergedCode
Id: mimic-medication-administration-merged-code
Title: "MIMIC MedicationAdministration code (merged sub-types)"
Description: "Union of the per-sub-type MedicationAdministration.medication[x] binding ValueSets (ED, ICU). Grouping ValueSet — references only, no own codes. NEVER BOUND BY ANY PROFILE: it exists so the ConceptMap that translates MedicationAdministration.medication[x] in fhnaumann/mimic-iv-terminology has exactly one sourceCanonical to name, and deleting it as unused would break that map. Originally written for the MimicMedicationAdministrationMerged facade profile, which was scrapped. Not part of the upstream MIMIC IG."

* include codes from valueset $MimicMedicationWithUnknown
* include codes from valueset $MimicMedicationCodes
