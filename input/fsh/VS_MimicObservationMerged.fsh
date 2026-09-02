ValueSet: MimicObservationMergedCode
Id: mimic-observation-merged-code
Title: "MIMIC Observation code (merged sub-types)"
Description: "Union of the per-sub-type Observation.code binding ValueSets (chartevents, datetimeevents, ED, labevents, microbiology organism/antibiotic/test, outputevents, vital signs). Grouping ValueSet — references only, no own codes. NEVER BOUND BY ANY PROFILE: it exists so the ConceptMap that translates Observation.code in fhnaumann/mimic-iv-terminology has exactly one sourceCanonical to name, and deleting it as unused would break that map. Originally written for the MimicObservationMerged facade profile, which was scrapped. Not part of the upstream MIMIC IG."

* include codes from valueset MimicCharteventsDItems
* include codes from valueset MimicDatetimeeventsDItems
* include codes from valueset MimicObservationTypeED
* include codes from valueset MimicDLabitems
* include codes from valueset MimicMicrobiologyOrganism
* include codes from valueset MimicMicrobiologyAntibiotic
* include codes from valueset MimicMicrobiologyTest
* include codes from valueset MimicOutputeventsDItems
* include codes from valueset MimicObservationTypeVital
