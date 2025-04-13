# Standard libraries
from pathlib import Path
# Third-party libraries
from cfis_utils import UsbUtils


class AmptekPX5():

    VENDOR_ID = 0x10C4
    PRODUCT_ID = 0x842A
    VENDOR_ID_STR = "10C4"
    PRODUCT_ID_STR = "842A"

    @staticmethod
    def install_libusb() -> None:
        """
        Install the libusb backend for pyusb.
        """
        UsbUtils.install_libusb()

    @staticmethod
    def add_udev_rule() -> None:
        """
        Add udev rules for the Amptek PX5 device.
        """
        UsbUtils.add_udev_rule(AmptekPX5.VENDOR_ID_STR, AmptekPX5.PRODUCT_ID_STR)


if __name__ == "__main__":
    amptek = AmptekPX5()
    amptek.install_libusb()
    amptek.add_udev_rule()