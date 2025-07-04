"""
Amptek MCA module for controlling Amptek digital pulse processors.

This module provides:
- AmptekMCA: Single device control
- MultiAmptekMCA: Multi-device control
- AmptekMCAError, AmptekMCAAckError: Exception classes
"""

from .amptek_mca import AmptekMCA, AmptekMCAError, AmptekMCAAckError
from .multi_amptek_mca import MultiAmptekMCA

__all__ = [
    'AmptekMCA',
    'MultiAmptekMCA', 
    'AmptekMCAError',
    'AmptekMCAAckError'
]