# (file intentionally left blank)
"""wb_welrok package

Provide a __version__ variable so packaging tools can determine the
version without depending on files outside the source package (e.g.
debian/changelog). This makes isolated PEP 517 builds / pip wheel
builds work inside dh_virtualenv, build isolation, and CI where the
debian/ tree may not be present.
"""

__version__ = "0.0.16"
