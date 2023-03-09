"""
Make a diagram of the Pydantic model

Using 'erdantic' a diagram of the pydantic modles can easily be made.
This is then used in the Readme.md for the repository.

Author: Peter Dudfield
Date: 2022-01-19
"""

import erdantic as erd

from nowcasting_datamodel.models import GSPYield, Metric, MetricValue, PVYield
from nowcasting_datamodel.models.forecast import ManyForecasts

diagram = erd.create(ManyForecasts, GSPYield, Metric, MetricValue)
diagram.draw("diagram.png")

diagram = erd.create(PVYield)
diagram.draw("diagram_pv.png")
