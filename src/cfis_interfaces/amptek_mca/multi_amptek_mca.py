# Standard libraries
import logging
from typing import Optional, Dict, List, Any
from collections import OrderedDict

# CFIS libraries
from cfis_utils import UsbUtils, LoggerUtils, Spectrum

# Third-party libraries
import usb.core

# Local imports
from .amptek_mca import AmptekMCA, AmptekMCAError, AmptekMCAAckError

# Logging prefix constant
LOG_PREFIX = "[MultiAmptekMCA]"


class MultiAmptekMCA:
    """
    Multi-device wrapper for AmptekMCA class.
    
    Automatically discovers and manages multiple Amptek MCA devices connected via USB.
    Provides broadcast methods that operate on all devices simultaneously.
    """
    
    def __init__(self, 
                 logger: Optional[logging.Logger] = None,
                 logger_name: str = "MultiAmptekMCA",
                 logger_level: int = logging.INFO):
        """
        Initialize MultiAmptekMCA by discovering all connected Amptek devices.
        
        Args:
            logger: Optional logger instance. If None, a new logger will be created.
            logger_name: Name for the new logger. Defaults to "MultiAmptekMCA".
            logger_level: Logging level for the new logger. Defaults to logging.INFO.
        """
        self.logger = logger if logger else LoggerUtils.get_logger(logger_name, level=logger_level)
        self.mcas: List[AmptekMCA] = []
        self.device_count = 0
        
        # Discover available devices
        self._discover_devices()
        
        if self.logger:
            self.logger.info(f"{LOG_PREFIX}  Initialized with {self.device_count} device(s)")
    
    def _discover_devices(self) -> None:
        """Discover all connected Amptek MCA devices and create instances."""
        try:
            backend = UsbUtils.get_libusb_backend()
            devices = list(usb.core.find(
                find_all=True, 
                idVendor=AmptekMCA.VENDOR_ID,
                idProduct=AmptekMCA.PRODUCT_ID, 
                backend=backend
            ))
            
            self.device_count = len(devices)
            
            # Create AmptekMCA instance for each device found
            for i in range(self.device_count):
                mca = AmptekMCA(logger=self.logger, device_index=i+1)
                self.mcas.append(mca)
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"{LOG_PREFIX}  Error discovering devices: {e}")
            raise AmptekMCAError(f"Failed to discover Amptek devices: {e}")
    
    @property
    def count(self) -> int:
        """Get the number of discovered devices."""
        return self.device_count
    
    def get_device(self, index: int) -> AmptekMCA:
        """
        Get specific AmptekMCA instance by index.
        
        Args:
            index: Device index (0-based)
            
        Returns:
            AmptekMCA instance for the specified device
            
        Raises:
            IndexError: If index is out of range
        """
        if not (0 <= index < self.device_count):
            raise IndexError(f"Device index {index} out of range. Available: 0-{self.device_count-1}")
        return self.mcas[index]
    
    # Connection methods
    def connect_all(self) -> Dict[int, bool]:
        """
        Connect to all discovered devices.
        
        Returns:
            Dictionary mapping device index to connection success (True/False)
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.connect(device_index=i)
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to connect device {i}: {e}")
                results[i] = False
        return results
    
    def disconnect_all(self) -> None:
        """Disconnect from all devices."""
        for mca in self.mcas:
            try:
                mca.disconnect()
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"{LOG_PREFIX}  Error disconnecting device: {e}")
    
    # Status methods
    def get_status(self, silent: bool = False) -> Dict[int, Dict[str, Any]]:
        """
        Get status from all connected devices.
        
        Args:
            silent: If True, suppress info-level logging
            
        Returns:
            Dictionary mapping device index to status dictionary
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                results[i] = mca.get_status(silent=silent)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to get status from device {i}: {e}")
                results[i] = None
        return results
    
    def get_models(self) -> Dict[int, str]:
        """
        Get device models from all devices.
        
        Returns:
            Dictionary mapping device index to model string
        """
        return {i: mca.get_model() for i, mca in enumerate(self.mcas)}
    
    # Spectrum methods
    def get_spectrum(self) -> Dict[int, Optional[Spectrum]]:
        """
        Get spectrum from all connected devices.
        
        Returns:
            Dictionary mapping device index to Spectrum object (None if failed)
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                results[i] = mca.get_spectrum()
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to get spectrum from device {i}: {e}")
                results[i] = None
        return results
    
    def clear_spectrum(self) -> Dict[int, bool]:
        """
        Clear spectrum on all connected devices.
        
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.clear_spectrum()
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to clear spectrum on device {i}: {e}")
                results[i] = False
        return results
    
    # MCA control methods
    def enable_mca(self) -> Dict[int, bool]:
        """
        Enable MCA on all connected devices.
        
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.enable_mca()
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to enable MCA on device {i}: {e}")
                results[i] = False
        return results
    
    def disable_mca(self) -> Dict[int, bool]:
        """
        Disable MCA on all connected devices.
        
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.disable_mca()
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to disable MCA on device {i}: {e}")
                results[i] = False
        return results
    
    # Configuration methods
    def send_configuration(self, config_dict: Dict[str, Any], save_to_flash: bool = False) -> Dict[int, bool]:
        """
        Send configuration to all connected devices.
        
        Args:
            config_dict: Configuration dictionary
            save_to_flash: Whether to save configuration to flash memory
            
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.send_configuration(config_dict, save_to_flash=save_to_flash)
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to send configuration to device {i}: {e}")
                results[i] = False
        return results
    
    def apply_default_configuration(self, device_type: str, config_name: str, 
                                  save_to_flash: bool = False, skip_hvse: bool = False) -> Dict[int, bool]:
        """
        Apply default configuration to all connected devices.
        
        Args:
            device_type: Device type string
            config_name: Configuration name
            save_to_flash: Whether to save to flash memory
            skip_hvse: Whether to skip HVSE parameter
            
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.apply_default_configuration(device_type, config_name, 
                                              save_to_flash=save_to_flash, skip_hvse=skip_hvse)
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to apply default config to device {i}: {e}")
                results[i] = False
        return results
    
    def apply_configuration_from_file(self, config_file_path: str, device_type: Optional[str] = None,
                                    save_to_flash: bool = False, skip_hvse: bool = False) -> Dict[int, bool]:
        """
        Apply configuration from file to all connected devices.
        
        Args:
            config_file_path: Path to configuration file
            device_type: Optional device type for validation
            save_to_flash: Whether to save to flash memory
            skip_hvse: Whether to skip HVSE parameter
            
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.apply_configuration_from_file(config_file_path, device_type=device_type,
                                                save_to_flash=save_to_flash, skip_hvse=skip_hvse)
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to apply config file to device {i}: {e}")
                results[i] = False
        return results
    
    # High-level acquisition methods
    def acquire_spectrum(self, **kwargs) -> Dict[int, Optional[Spectrum]]:
        """
        Acquire spectrum from all connected devices.
        
        Args:
            **kwargs: Arguments passed to AmptekMCA.acquire_spectrum()
            
        Returns:
            Dictionary mapping device index to Spectrum object (None if failed)
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                results[i] = mca.acquire_spectrum(**kwargs)
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Failed to acquire spectrum from device {i}: {e}")
                results[i] = None
        return results
    
    def wait_until_mca_is_closed(self, time_between_checks: float = 1.0) -> Dict[int, bool]:
        """
        Wait until MCA is closed on all devices.
        
        Args:
            time_between_checks: Time in seconds between status checks
            
        Returns:
            Dictionary mapping device index to success status
        """
        results = {}
        for i, mca in enumerate(self.mcas):
            try:
                mca.wait_until_mca_is_closed(time_between_checks=time_between_checks)
                results[i] = True
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Device {i} wait failed: {e}")
                results[i] = False
        return results
    
    # Static methods (delegated to AmptekMCA)
    @staticmethod
    def install_libusb(logger: Optional[logging.Logger] = None) -> None:
        """Install libusb backend. Delegates to AmptekMCA."""
        AmptekMCA.install_libusb(logger=logger)
    
    @staticmethod
    def add_udev_rule(logger: Optional[logging.Logger] = None) -> None:
        """Add udev rules. Delegates to AmptekMCA."""
        AmptekMCA.add_udev_rule(logger=logger)
    
    def get_available_default_configurations(self) -> Dict[str, List[str]]:
        """Get available default configurations. Delegates to AmptekMCA."""
        temp_mca = AmptekMCA(logger=self.logger, logger_name="TempAmptekMCA")
        return temp_mca.get_available_default_configurations()
    
    def get_default_configuration(self, device_type: str, config_name: str):
        """Get default configuration. Delegates to AmptekMCA."""
        temp_mca = AmptekMCA(logger=self.logger, logger_name="TempAmptekMCA")
        return temp_mca.get_default_configuration(device_type, config_name)
    
    def get_configuration_from_file(self, config_file_path: str, device_type: Optional[str] = None):
        """Get configuration from file. Delegates to AmptekMCA."""
        temp_mca = AmptekMCA(logger=self.logger, logger_name="TempAmptekMCA")
        return temp_mca.get_configuration_from_file(config_file_path, device_type=device_type)
    
    # Context manager support
    def __enter__(self):
        """Context manager entry."""
        self.connect_all()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect_all()
    
    def __len__(self):
        """Return number of devices."""
        return self.device_count
    
    def __getitem__(self, index: int) -> AmptekMCA:
        """Allow indexing to get specific device."""
        return self.get_device(index)