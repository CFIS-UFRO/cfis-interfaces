# Amptek MCA

This library provides an interface to control the following Amptek MCA devices: DP5, PX5, DP5X, DP5G, and MCA8000D.

It is implemented based on the official specifications found in the Amptek [Digital Products Programmer's Guide](https://www.amptek.com/-/media/ametekamptek/documents/resources/products/user-manuals/amptek-digital-products-programmers-guide-b3.pdf?la=en&revision=70db147d-b3c2-4d44-aaa2-374f648a4bc7).

**Note:** Only the PX5 device was tested.

# Getting Started

1.  Install the `cfis_interfaces` package:
    ```bash
    pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git
    ```
2.  Import the library in your Python script:
    ```python
    from cfis_interfaces import AmptekMCA
    ```
3.  Use the built-in method to install the `libusb` dependency:
    ```python
    AmptekMCA.install_libusb()
    ```
4.  On Linux, add a udev rule to allow non-root access:
    ```python
    AmptekMCA.add_udev_rule()
    ```
3.  Create an instance of the `AmptekMCA` class:
    ```python
    amptek = AmptekMCA()
    ```
6.  Connect, interact, and disconnect as needed:
    ```python
    import time

    amptek = AmptekMCA()

    print("Connecting to device...")
    amptek.connect()
    print(f"Connected to {DEVICE_TYPE}.")

    # --- Apply a default configuration ---
    # You can get a list of available default configurations using:
    # amptek.get_available_default_configurations()
    # HVSE is applied ramped until the default value
    print(f"Applying default configuration ...")
    amptek.apply_default_configuration("PX5", "CdTe Default PX5") # Example for PX5
    print("Default configuration applied.")

    # --- Basic Acquisition Example ---
    print("Clearing spectrum...")
    amptek.clear_spectrum()

    print("Enabling MCA...")
    amptek.enable_mca()

    print("Aquiring spectrum...")
    time.sleep(10) # Short acquisition for example

    print("Disabling MCA...")
    amptek.disable_mca()

    print("Reading spectrum...")
    spectrum = amptek.get_spectrum()
    print(f"Spectrum received ({len(spectrum)} channels).")

    # --- Safely Ramp Down HV to 0V before disconnecting ---
    print("Setting HV to 0V (ramped)...")
    amptek.set_HVSE(0, save_to_flash = True) # Ramps down, saves to flash for safety start on next power on
    print("HV set to 0V.")
    ```
