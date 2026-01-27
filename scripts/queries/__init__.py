"""Query package for IoT data analysis.

This package provides query classes for analyzing IoT data stored
in PostgreSQL. Each query class encapsulates a single analytical query
following the Single Responsibility Principle.
"""

from .leaf_locations import LeafLocationsQuery  # noqa: F401
from .lowest_sublocations import LowestSublocationsQuery  # noqa: F401
from .smart_lamp_events import SmartLampEventsQuery  # noqa: F401
from .avg_brightness import AvgBrightnessQuery  # noqa: F401
from .leak_locations import LeakLocationsQuery  # noqa: F401
from .devices_no_events import DevicesNoEventsQuery  # noqa: F401
from .top_smart_lamp_locations import TopSmartLampLocationsQuery  # noqa: F401
