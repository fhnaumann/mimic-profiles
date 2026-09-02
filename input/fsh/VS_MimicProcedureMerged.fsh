ValueSet: MimicProcedureMergedCode
Id: mimic-procedure-merged-code
Title: "MIMIC Procedure code (merged sub-types)"
Description: "Union of the per-sub-type Procedure.code binding ValueSets (ED, ICD, ICU). Grouping ValueSet — references only, no own codes. NEVER BOUND BY ANY PROFILE: it exists so the ConceptMap that translates Procedure.code in fhnaumann/mimic-iv-terminology has exactly one sourceCanonical to name, and deleting it as unused would break that map. Originally written for the MimicProcedureMerged facade profile, which was scrapped. Not part of the upstream MIMIC IG."

* include codes from valueset $MimicProcedureIcd
* include codes from valueset MimicProcedureTypesED
* include codes from valueset $MimicProcedureeventsDItems
