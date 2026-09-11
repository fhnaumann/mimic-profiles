"""Reading source codes out of the IG's own CodeSystems and ValueSets.

Source codes are ALWAYS read from the IG, never written out in a builder. A
literal would drift from the profiles the map serves, and nothing would notice.
Every stage that needs to know "what is the source side" comes through
source_concepts, so no two of them can hold a private copy of it.

Every stage that asks "what is the source side" — build_groups, the stream
reports, verify_mappings — comes through here, so no two of them can disagree
about a stream's population. The per-element `observed_only` narrowing that
used to live here is gone: a stream resolves identically wherever it is
consumed (see lib/assemble.py). "Used in the data" still narrows what resolves,
but UNION-WIDE and in the resolver, so the answer for a code cannot depend on
which bound element the Coding sat on — which is exactly what the per-element
version got wrong.
"""

import json
import sys

from .canonical import RESOURCES


def resource_path(source):
    """The IG resource a source's codes come from.

    `file` names a MIMIC CodeSystem: the ValueSets bound over those are bare
    composes carrying no enumerated concepts, so the CodeSystem is the only
    enumeration there is. `valueset_file` names an IG ValueSet that does
    enumerate its concepts.

    Both resolve against the one committed snapshot directory. There used to be
    two — input/resources/ and fsh-generated/resources/ of a sibling IG
    checkout, tried in that order — and which one answered decided whether a
    build worked at all, because the second was gitignored. See
    sync_ig_resources.py.
    """
    return RESOURCES / (source["file"] if "file" in source
                        else source["valueset_file"])


def source_concepts(source):
    """(code, display) for every code a source contributes.

    A missing file is FATAL, without the CodeSystem/ValueSet distinction this
    used to draw. That distinction encoded where the resource came from — a
    CodeSystem shipped with the IG, a ValueSet had to be built by SUSHI — and
    the snapshot ends it: every one of these files is committed and verified by
    `make verify-ig`, so any absence means a corrupted snapshot rather than a
    step someone has not run yet. Continuing would silently emit a map that
    drops every code in the population.
    """
    path = resource_path(source)
    if not path.is_file():
        sys.exit(f"  {path} not found in the IG snapshot. Run `make verify-ig` "
                 f"to check it, and `make sync-ig` to rebuild it from an IG "
                 f"checkout. Continuing would silently drop every "
                 f"{source['system']} code from the map.")
    resource = json.loads(path.read_text())
    if resource.get("resourceType") == "ValueSet":
        for include in resource.get("compose", {}).get("include", []):
            if include.get("system") != source["system"]:
                continue
            for concept in include.get("concept", []):
                yield concept["code"], concept.get("display", "")
    else:
        for concept in resource.get("concept", []):
            yield concept["code"], concept.get("display", "")


def source_paths(sources):
    """Every IG resource and table a declaration reads. Used for dating."""
    paths = [p for s in sources if (p := resource_path(s)).is_file()]
    # Mapping tables are inputs too: editing one is a real change to the map,
    # and the resource date has to move with it.
    paths += [s["table"] for s in sources
              if "table" in s and s["table"].is_file()]
    return paths
