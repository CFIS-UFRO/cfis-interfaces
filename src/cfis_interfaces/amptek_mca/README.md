# Amptek MCA

This library provides an interface to control the following Amptek MCA devices: DP5, PX5, DP5X, DP5G, and MCA8000D.

## Available Classes

The library provides two main classes:

### AmptekMCA
- **Purpose**: Control a single Amptek MCA device
- **Use case**: When you need to work with one device at a time
- **Features**: Full control over a single device including configuration, spectrum acquisition, and HV management

### MultiAmptekMCA
- **Purpose**: Control multiple Amptek MCA devices simultaneously
- **Use case**: When you have multiple devices connected and need to coordinate operations across all of them
- **Features**: 
  - Automatic device discovery
  - Broadcast operations to all devices
  - Parallel execution for time-consuming operations (spectrum acquisition, waiting for MCA close)
  - Device-type filtering for selective operations

## Implementation Details

It is implemented based on the official specifications found in the Amptek [Digital Products Programmer's Guide](https://www.amptek.com/-/media/ametekamptek/documents/resources/products/user-manuals/amptek-digital-products-programmers-guide-b3.pdf?la=en&revision=70db147d-b3c2-4d44-aaa2-374f648a4bc7).

**Note:** Only the PX5 and PC5 devices were tested.

# Requirements

*   Python 3.8 or higher.
    *   Windows: [https://www.python.org/](https://www.python.org/)
    *   Linux and MacOS: Usually pre-installed.
*   Git.
    *   Windows: [https://git-scm.com/download/win](https://git-scm.com/download/win)
    *   Linux: Install it through your package manager, e.g., `sudo apt install git`.
    *   MacOS: Install it through the Xcode command line tools using `xcode-select --install`.

# Getting the library

To get the library, install `cfis-interfaces` directly from the GitHub repository using `pip`:

```bash
pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git
```

# Libusb installation and configuration

The Amptek MCA library relies on `libusb` for generic USB access to communicate with the devices. Follow the instructions below to install and configure `libusb` for your operating system:

* Windows:
    1.  Open the terminal and run the built-in method to install the `libusb` dependency:
        ```python
        python -c "from cfis_interfaces import AmptekMCA; AmptekMCA.install_libusb()"
        ```
    2.  Download Zadig from the official website: [https://zadig.akeo.ie/](https://zadig.akeo.ie/).
    3.  Connect the Amptek device to your computer. **If using a hub**, first connect the USB hub to your computer, then connect the Amptek device to the hub.
    4.  Open Zadig **as Administrator** (right-click on the executable and select "Run as administrator").
    5.  **If using a USB hub**, make sure to **deselect** `Options > Ignore Hubs or Composite Parents` to ensure the device is properly detected.
    6.  Go to `Options > List All Devices`.
    7.  In the Zadig window, select the Amptek device (VID `10C4`, PID `842A`) and choose the `WinUSB` driver from the target driver list.
    8.  Click the `Install Driver` (or `Replace Driver`) button to install the driver.
    9.  Repeat steps 3-8 for any additional Amptek devices you want to connect.
    10. Connect the Amptek devices **always** to the same USB port.
* Linux:
    1.  Open the terminal and run the built-in method to install the `libusb` dependency:
        ```python
        python -c "from cfis_interfaces import AmptekMCA; AmptekMCA.install_libusb()"
        ```
    2. Optionally (but recommended), run the built-in method to add a udev rule for non-root access:
        ```python
        python -c "from cfis_interfaces import AmptekMCA; AmptekMCA.add_udev_rule()"
        ```
*  MacOS:
    1.  Open the terminal and run the built-in method to install the `libusb` dependency:
        ```python
        python -c "from cfis_interfaces import AmptekMCA; AmptekMCA.install_libusb()"
        ```

# Example usage

The library is not documented yet, but you can find examples here to get started.

## Single Device (AmptekMCA)

The example demonstrates how to connect to the device, apply a default configuration, acquire a spectrum, and safely ramp down the high voltage (HV) before disconnecting.

```python
from cfis_interfaces import AmptekMCA
import time

amptek = AmptekMCA()

print("Connecting to device...")
amptek.connect()

# --- Apply a default configuration ---
# You can get a list of available default configurations using:
# amptek.get_available_default_configurations()
# HVSE is applied ramped until the default value
print(f"Applying default configuration ...")
amptek.apply_default_configuration("PX5", "CdTe Default PX5") # Example for PX5

# --- Basic Configuration Example ---
print("Setting channels to 2048...")
amptek.send_configuration("MCAC", 2048)

# --- Basic Acquisition Example ---
print("Getting spectrum...")
spectrum = amptek.acquire_spectrum(preset_real_time = 10)

# --- Safely Ramp Down HV to 0V before disconnecting ---
print("Setting HV to 0V (ramped)...")
amptek.set_HVSE(0, save_to_flash = True) # Ramps down, saves to flash for safety start on next power on

# --- Disconnecting ---
print("Disconnecting...")
amptek.disconnect()
print("Disconnected.")
```

## Multiple Devices (MultiAmptekMCA)

The example demonstrates how to work with multiple Amptek devices simultaneously, applying configurations and acquiring spectra from all devices in parallel.

```python
from cfis_interfaces import MultiAmptekMCA
import time

# Initialize and auto-discover devices
multi_amptek = MultiAmptekMCA()

print(f"Found {multi_amptek.count} devices")

# Connect to all devices
connection_results = multi_amptek.connect()
print(f"Connection results: {connection_results}")

# Get device models
models = multi_amptek.get_model()
print(f"Device models: {models}")

# --- Apply default configuration to specific device type ---
# Only devices of type "PX5" will receive this configuration
print("Applying default configuration to PX5 devices...")
config_results = multi_amptek.apply_default_configuration("PX5", "CdTe Default PX5")
print(f"Configuration results: {config_results}")

# --- Configure all devices ---
print("Setting channels to 2048 on all devices...")
channel_results = multi_amptek.send_configuration(config_dict={"MCAC": 2048})
print(f"Channel configuration results: {channel_results}")

# --- Parallel spectrum acquisition ---
print("Acquiring spectra from all devices in parallel...")
spectra = multi_amptek.acquire_spectrum(preset_real_time=10)
print(f"Spectrum acquisition results: {spectra}")

# --- Safely ramp down HV on all devices ---
print("Setting HV to 0V on all devices...")
hv_results = multi_amptek.set_HVSE(target_voltage=0, save_to_flash=True)
print(f"HV shutdown results: {hv_results}")

print("Disconnecting from all devices...")
multi_amptek.disconnect()
print("All devices disconnected.")
```
