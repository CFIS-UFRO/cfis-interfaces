#!/usr/bin/env python3
"""
Example usage of MultiAmptekMCA for controlling multiple Amptek devices.

This example demonstrates:
- Automatic device discovery
- Connecting to all devices
- Broadcasting commands to all devices
- Individual device access
- Error handling for partial failures
"""

import sys
import time
from pathlib import Path

# Add src to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from cfis_interfaces.amptek_mca import MultiAmptekMCA, AmptekMCA


def main():
    print("=== MultiAmptekMCA Example ===")
    
    # Initialize MultiAmptekMCA (discovers all devices)
    try:
        multi_mca = MultiAmptekMCA()
        print(f"✅ Discovered {len(multi_mca)} Amptek devices")
        
        if len(multi_mca) == 0:
            print("❌ No devices found. Connect Amptek devices and try again.")
            return
            
    except Exception as e:
        print(f"❌ Failed to initialize MultiAmptekMCA: {e}")
        return
    
    try:
        # Method 1: Using context manager (recommended)
        print("\n--- Using Context Manager ---")
        with multi_mca:
            # Get device models
            models = multi_mca.get_models()
            print(f"Device models: {models}")
            
            # Get status from all devices
            statuses = multi_mca.get_status()
            for device_id, status in statuses.items():
                if status:
                    print(f"Device {device_id}: {status.get('device_id', 'Unknown')}")
                else:
                    print(f"Device {device_id}: Failed to get status")
            
            # Clear spectrum on all devices
            clear_results = multi_mca.clear_spectrum()
            print(f"Clear spectrum results: {clear_results}")
            
            # Enable MCA on all devices
            enable_results = multi_mca.enable_mca()
            print(f"Enable MCA results: {enable_results}")
            
            # Wait a bit and then get spectra
            print("Acquiring for 5 seconds...")
            time.sleep(5)
            
            # Get spectra from all devices
            spectra = multi_mca.get_spectrum()
            for device_id, spectrum in spectra.items():
                if spectrum:
                    print(f"Device {device_id}: Got spectrum with {len(spectrum.counts)} channels")
                else:
                    print(f"Device {device_id}: Failed to get spectrum")
            
            # Disable MCA on all devices
            disable_results = multi_mca.disable_mca()
            print(f"Disable MCA results: {disable_results}")
        
        # Method 2: Manual connection management
        print("\n--- Manual Connection Management ---")
        
        # Connect to all devices
        connect_results = multi_mca.connect_all()
        print(f"Connect results: {connect_results}")
        
        # Access individual devices
        if len(multi_mca) > 0:
            device_0 = multi_mca.get_device(0)  # or multi_mca[0]
            print(f"Device 0 model: {device_0.get_model()}")
            
            # You can also use the device directly
            device_0.clear_spectrum()
            print("✅ Cleared spectrum on device 0 individually")
        
        # Apply configuration to all devices
        config = {
            'MCAS': '8192',  # Number of channels
            'MCAC': '8192',  # Channel count
            'TPEA': '6.4',   # Peaking time
        }
        config_results = multi_mca.send_configuration(config, save_to_flash=False)
        print(f"Configuration results: {config_results}")
        
        # Disconnect all devices
        multi_mca.disconnect_all()
        print("✅ Disconnected all devices")
        
    except Exception as e:
        print(f"❌ Error during operation: {e}")
        # Ensure cleanup
        try:
            multi_mca.disconnect_all()
        except:
            pass


def example_static_methods():
    """Example of using static methods that don't require device instances."""
    print("\n=== Static Methods Example ===")
    
    # Get available default configurations
    try:
        configs = MultiAmptekMCA.get_available_default_configurations()
        print(f"Available device types: {list(configs.keys())}")
        
        if 'DP5' in configs:
            print(f"DP5 configurations: {configs['DP5']}")
    except Exception as e:
        print(f"Failed to get configurations: {e}")
    
    # Install libusb if needed
    try:
        MultiAmptekMCA.install_libusb()
        print("✅ Libusb installation check completed")
    except Exception as e:
        print(f"Libusb installation failed: {e}")


if __name__ == "__main__":
    main()
    example_static_methods()