import re
import logging

logger = logging.getLogger(__name__)


def resolve_name_key(name: str) -> str:
    """
    Canonicalizes team names for cross-source matching.
    Maps 'Saint', 'St', and 'State' to 'st' to reconcile 'Iowa State' vs 'Iowa St'.
    """
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r'[^a-z0-9\s&]', '', n)
    # Standardize Saint / St / State to 'st' for resolution
    n = re.sub(r'\bst\b|\bsaint\b|\bstate\b', 'st', n)
    n = n.replace(" university", "").replace(" univ", "").replace(" and ", " & ")
    return " ".join(n.split())


class TeamNameResolver:
    """
    Collision-safe resolver that builds a lookup table from a master list (e.g. KenPom).
    Ambiguous normalized keys (collisions) are invalidated to prevent mis-mapping.
    """

    def __init__(self, master_names: list[str], logger_instance=None):
        self.logger = logger_instance or logger
        self.master_map = {name: name for name in master_names}
        self.norm_map = {}
        self.collisions = set()

        for name in master_names:
            key = resolve_name_key(name)
            if key in self.norm_map and self.norm_map[key] != name:
                self.logger.warning(
                    f"Name Resolution Collision: '{key}' maps to both '{self.norm_map[key]}' "
                    f"and '{name}'. Marking as ambiguous."
                )
                self.collisions.add(key)
            else:
                self.norm_map[key] = name

        for key in self.collisions:
            self.norm_map.pop(key, None)

    def resolve(self, name: str) -> str | None:
        """Exact match first, then safe normalized match."""
        if name in self.master_map:
            return name
        key = resolve_name_key(name)
        return self.norm_map.get(key)
