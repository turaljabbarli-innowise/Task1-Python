"""Query package for IoT data analysis.

This package provides query classes for analyzing IoT data stored
in PostgreSQL. Each query class encapsulates a single analytical query
following the Single Responsibility Principle.
"""

from .leaf_locations import LeafLocationsQuery
from .lowest_sublocations import LowestSublocationsQuery
from .smart_lamp_events import SmartLampEventsQuery
from .avg_brightness import AvgBrightnessQuery
from .leak_locations import LeakLocationsQuery
from .devices_no_events import DevicesNoEventsQuery
from .top_smart_lamp_locations import TopSmartLampLocationsQuery