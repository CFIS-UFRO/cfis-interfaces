# Standard libraries
import logging
from typing import Optional, Dict, List, Any, Union
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

# CFIS libraries
from cfis_utils import UsbUtils, LoggerUtils, Spectrum

# Third-party libraries
import usb.core

# Local imports
from .amptek_mca import AmptekMCA, AmptekMCAError

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
    def connect(self) -> Dict[int, bool]:
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
    
    def disconnect(self) -> None:
        """Disconnect from all devices."""
        for mca in self.mcas:
            try:
                mca.disconnect()
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"{LOG_PREFIX}  Error disconnecting device: {e}")

    # Generic broadcast utility
    def broadcast(self,
                  method_name: str,
                  *args,
                  device_type: Optional[str] = None,
                  parallel: bool = True,
                  **kwargs) -> Dict[int, Dict[str, Any]]:
        """
        Call an AmptekMCA method on all (or filtered) devices and collect results.

        Args:
            method_name: Name of the AmptekMCA method to call (e.g., 'read_configuration', 'send_configuration').
            *args: Positional arguments to pass to the method.
            device_type: If provided, only devices whose model matches this string are targeted.
            parallel: If True, execute calls in parallel using threads. If False, run sequentially.
            **kwargs: Keyword arguments to pass to the method.

        Returns:
            Dict mapping device index to a result dict with keys:
              - 'ok': True on success, False on error, None if skipped by filter
              - 'result': return value from the method (None if error/skip)
              - 'error': error message string if an exception occurred, else None
        """

        def _call_single(idx: int, mca: AmptekMCA) -> Dict[str, Any]:
            try:
                # Filter by device type if requested
                if device_type is not None and mca.get_model() != device_type:
                    if self.logger:
                        self.logger.debug(f"{LOG_PREFIX}  Skipping device {idx} (type: {mca.get_model()}, target: {device_type})")
                    return {"ok": None, "result": None, "error": None}

                # Resolve and call method
                target = getattr(mca, method_name, None)
                if target is None or not callable(target):
                    msg = f"Method '{method_name}' not found or not callable on AmptekMCA"
                    if self.logger:
                        self.logger.error(f"{LOG_PREFIX}  {msg}")
                    return {"ok": False, "result": None, "error": msg}

                value = target(*args, **kwargs)
                return {"ok": True, "result": value, "error": None}
            except Exception as e:
                if self.logger:
                    self.logger.error(f"{LOG_PREFIX}  Error calling '{method_name}' on device {idx}: {e}")
                return {"ok": False, "result": None, "error": str(e)}

        results: Dict[int, Dict[str, Any]] = {}
        if parallel and self.device_count > 1:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=self.device_count) as executor:
                future_map = {executor.submit(_call_single, i, mca): i for i, mca in enumerate(self.mcas)}
                for fut in as_completed(future_map):
                    i = future_map[fut]
                    results[i] = fut.result()
        else:
            for i, mca in enumerate(self.mcas):
                results[i] = _call_single(i, mca)

        return results
    
    # Status methods
    def get_status(self, silent: bool = False) -> Dict[int, Dict[str, Any]]:
        """
        Get status from all connected devices.
        
        Args:
            silent: If True, suppress info-level logging
            
        Returns:
            Dictionary mapping device index to status dictionary
        """
        br = self.broadcast("get_status", silent=silent, parallel=True)
        return {i: (v["result"] if v["ok"] else None) for i, v in br.items()}
    
    def get_model(self) -> Dict[int, str]:
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
        br = self.broadcast("get_spectrum", parallel=True)
        return {i: (v["result"] if v["ok"] else None) for i, v in br.items()}
    
    def clear_spectrum(self) -> Dict[int, bool]:
        """
        Clear spectrum on all connected devices.
        
        Returns:
            Dictionary mapping device index to success status
        """
        br = self.broadcast("clear_spectrum", parallel=True)
        return {i: (v["ok"] is True) for i, v in br.items()}
    
    # MCA control methods
    def enable_mca(self) -> Dict[int, bool]:
        """
        Enable MCA on all connected devices.
        
        Returns:
            Dictionary mapping device index to success status
        """
        br = self.broadcast("enable_mca", parallel=True)
        return {i: (v["ok"] is True) for i, v in br.items()}
    
    def disable_mca(self) -> Dict[int, bool]:
        """
        Disable MCA on all connected devices.
        
        Returns:
            Dictionary mapping device index to success status
        """
        br = self.broadcast("disable_mca", parallel=True)
        return {i: (v["ok"] is True) for i, v in br.items()}
    
    # High voltage control methods
    def set_HVSE(self, device_type: Optional[str] = None, target_voltage: Union[float, int, str] = None, 
                 step: float = 50.0, delay_sec: float = 0.5, save_to_flash: bool = False) -> Dict[int, bool]:
        """
        Set high voltage supply on connected devices of specified type in parallel.
        
        Args:
            device_type: Device type to apply HVSE to. If None, applies to all devices.
            target_voltage: The desired final voltage (float or int) or "OFF" (string)
            step: The approximate voltage step size for ramping (default: 50.0 V)
            delay_sec: The delay in seconds between sending each voltage step command (default: 0.5 s)
            save_to_flash: If True, the final target voltage will be saved to flash
            
        Returns:
            Dictionary mapping device index to success status (None = skipped)
        """
        br = self.broadcast(
            "set_HVSE",
            target_voltage,
            step=step,
            delay_sec=delay_sec,
            save_to_flash=save_to_flash,
            device_type=device_type,
            parallel=True,
        )
        return {i: (v["ok"] if v["ok"] is None else (v["ok"] is True)) for i, v in br.items()}
    
    # Configuration methods
    def send_configuration(self, device_type: Optional[str] = None, config_dict: Dict[str, Any] = None, save_to_flash: bool = False) -> Dict[int, bool]:
        """
        Send configuration to connected devices of specified type.
        
        Args:
            device_type: Device type to apply configuration to. If None, applies to all devices.
            config_dict: Configuration dictionary
            save_to_flash: Whether to save configuration to flash memory
            
        Returns:
            Dictionary mapping device index to success status
        """
        br = self.broadcast(
            "send_configuration",
            config_dict,
            save_to_flash=save_to_flash,
            device_type=device_type,
            parallel=False,
        )
        return {i: (v["ok"] if v["ok"] is None else (v["ok"] is True)) for i, v in br.items()}
    
    def apply_default_configuration(self, device_type: str, config_name: str, 
                                  save_to_flash: bool = False, skip_hvse: bool = False) -> Dict[int, bool]:
        """
        Apply default configuration to connected devices of specified type.
        
        Args:
            device_type: Device type string (only devices of this type will be configured)
            config_name: Configuration name
            save_to_flash: Whether to save to flash memory
            skip_hvse: Whether to skip HVSE parameter
            
        Returns:
            Dictionary mapping device index to success status (None = skipped)
        """
        br = self.broadcast(
            "apply_default_configuration",
            device_type,
            config_name,
            save_to_flash=save_to_flash,
            skip_hvse=skip_hvse,
            device_type=device_type,
            parallel=False,
        )
        return {i: (v["ok"] if v["ok"] is None else (v["ok"] is True)) for i, v in br.items()}
    
    def apply_configuration_from_file(self, device_type: Optional[str] = None, config_file_path: str = None,
                                    save_to_flash: bool = False, skip_hvse: bool = False) -> Dict[int, bool]:
        """
        Apply configuration from file to connected devices of specified type.
        
        Args:
            device_type: Device type to apply configuration to. If None, applies to all devices.
            config_file_path: Path to configuration file
            save_to_flash: Whether to save to flash memory
            skip_hvse: Whether to skip HVSE parameter
            
        Returns:
            Dictionary mapping device index to success status (None = skipped)
        """
        br = self.broadcast(
            "apply_configuration_from_file",
            config_file_path,
            device_type=device_type,
            save_to_flash=save_to_flash,
            skip_hvse=skip_hvse,
            parallel=False,
        )
        return {i: (v["ok"] if v["ok"] is None else (v["ok"] is True)) for i, v in br.items()}
    
    # High-level acquisition methods
    def acquire_spectrum(self,
                         channels: Optional[int] = None,
                         preset_acq_time: Optional[Union[float, str]] = None,
                         preset_real_time: Optional[Union[float, str]] = None,
                         preset_counts: Optional[Union[int, str]] = None,
                         preset_live_time: Optional[Union[float, str]] = None,
                         gain: Optional[float] = None,
                         save_config_to_flash: bool = False,
                         time_between_checks: float = 1.0) -> Dict[int, Optional[Spectrum]]:
        """
        Acquire spectrum from all connected devices in parallel.
        
        Args:
            channels: Number of channels for the spectrum
            preset_acq_time: Acquisition time preset
            preset_real_time: Real time preset
            preset_counts: Counts preset
            preset_live_time: Live time preset
            gain: Gain setting
            save_config_to_flash: Whether to save configuration to flash memory
            time_between_checks: Time between status checks during acquisition
            
        Returns:
            Dictionary mapping device index to Spectrum object (None if failed)
        """
        br = self.broadcast(
            "acquire_spectrum",
            channels=channels,
            preset_acq_time=preset_acq_time,
            preset_real_time=preset_real_time,
            preset_counts=preset_counts,
            preset_live_time=preset_live_time,
            gain=gain,
            save_config_to_flash=save_config_to_flash,
            time_between_checks=time_between_checks,
            parallel=True,
        )
        return {i: (v["result"] if v["ok"] else None) for i, v in br.items()}
    
    def wait_until_mca_is_closed(self, time_between_checks: float = 1.0) -> Dict[int, bool]:
        """
        Wait until MCA is closed on all devices in parallel.
        
        Args:
            time_between_checks: Time in seconds between status checks
            
        Returns:
            Dictionary mapping device index to success status
        """
        br = self.broadcast(
            "wait_until_mca_is_closed",
            time_between_checks=time_between_checks,
            parallel=True,
        )
        return {i: (v["ok"] is True) for i, v in br.items()}
    
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
    
    # Static methods (delegated to AmptekMCA)
    @staticmethod
    def install_libusb(logger: Optional[logging.Logger] = None) -> None:
        """Install libusb backend. Delegates to AmptekMCA."""
        AmptekMCA.install_libusb(logger=logger)
    
    @staticmethod
    def add_udev_rule(logger: Optional[logging.Logger] = None) -> None:
        """Add udev rules. Delegates to AmptekMCA."""
        AmptekMCA.add_udev_rule(logger=logger)

    # Context manager support
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
    
    def __len__(self):
        """Return number of devices."""
        return self.device_count
    
    def __getitem__(self, index: int) -> AmptekMCA:
        """Allow indexing to get specific device."""
        return self.get_device(index)
