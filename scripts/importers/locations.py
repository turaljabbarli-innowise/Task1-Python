"""Location importer for hierarchical location data.

This module handles importing locations that have parent-child relationships,
requiring special insertion order to satisfy foreign key constraints.
"""

import logging
from typing import Dict, Any, List, Set
from .base import BaseImporter


class LocationImporter(BaseImporter):
    """Importer for location entities with hierarchical relationships.

    Handles the special case of self-referencing foreign keys where
    parent locations must be inserted before their children. Uses
    a deferred insertion strategy to resolve dependency order.
    """

    def get_table_name(self) -> str:
        """Return the locations table name.

        Returns:
            String 'locations'.
        """
        return "locations"

    def get_conflict_column(self) -> str:
        """Return the primary key column for conflict resolution.

        Returns:
            String 'location_id'.
        """
        return "location_id"

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw location data for database insertion.

        Args:
            raw_data: Dictionary with location_id, parent_location_id,
                and location_name fields.

        Returns:
            Dictionary formatted for the locations table.
        """
        return {
            "location_id": raw_data.get("location_id"),
            "parent_location_id": raw_data.get("parent_location_id"),
            "location_name": raw_data.get("location_name")
        }

    def process_entities(self, data: List[Dict[str, Any]]) -> None:
        """Process locations respecting hierarchical dependencies.

        Overrides the base implementation to handle self-referencing
        foreign keys. Locations are inserted in waves: first those
        with no parent, then those whose parents were just inserted,
        and so on until all locations are processed.

        Args:
            data: List of location dictionaries to import.

        Raises:
            Exception: If insertion fails. Also logs warning if some
                locations cannot be inserted due to missing parents.
        """
        inserted_ids: Set[str] = set()
        to_insert = data

        while to_insert:
            deferred = []
            progress = False

            for item in to_insert:
                loc_id = str(item.get('location_id'))
                p_id = item.get('parent_location_id')
                p_id = str(p_id) if p_id is not None else None

                if p_id == loc_id:
                    p_id = None

                if p_id is None or p_id in inserted_ids:
                    try:
                        transformed_data = self.transform_data(item)
                        self.db.insert(
                            table=self.get_table_name(),
                            data=transformed_data,
                            conflict_column=self.get_conflict_column()
                        )
                        inserted_ids.add(loc_id)
                        progress = True
                    except Exception:
                        self.db.rollback()
                        raise
                else:
                    deferred.append(item)

            if not progress and deferred:
                logging.warning(f"Could not insert {len(deferred)} locations due to missing parents")
                break

            to_insert = deferred

        self.db.commit()