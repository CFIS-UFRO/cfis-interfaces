# Standard libraries
import time
import struct
from typing import Optional, Tuple, Union, Dict, Any, List, OrderedDict as OrderedDictType
import math
from collections import OrderedDict
from pathlib import Path
import logging
# CFIS libraries
from cfis_utils import UsbUtils, LoggerUtils
# Third-party libraries
import usb.core
import usb.util

# Constants from Amptek Programmer's Guide
SYNC_BYTE_1 = 0xF5
SYNC_BYTE_2 = 0xFA

# ACK Packet PIDs (PID1 = 0xFF)
ACK_OK = 0x00
ACK_SYNC_ERROR = 0x01
ACK_PID_ERROR = 0x02
ACK_LEN_ERROR = 0x03
ACK_CHECKSUM_ERROR = 0x04
ACK_BAD_PARAMETER = 0x05
ACK_BAD_HEX_RECORD = 0x06
ACK_UNRECOGNIZED_CMD = 0x07
ACK_FPGA_ERROR = 0x08
ACK_CP2201_NOT_FOUND = 0x09
ACK_SCOPE_DATA_NA = 0x0A
ACK_PC5_NOT_PRESENT = 0x0B
ACK_OK_SHARING_REQ = 0x0C
ACK_BUSY = 0x0D
ACK_I2C_ERROR = 0x0E
ACK_OK_FPGA_UPLOAD_ADDR = 0x0F
ACK_FEATURE_NOT_SUPPORTED = 0x10
ACK_CAL_DATA_NOT_PRESENT = 0x11

# Request Packet PIDs
REQ_STATUS = (0x01, 0x01)
REQ_SPECTRUM = (0x02, 0x01)
REQ_SPECTRUM_CLEAR = (0x02, 0x02)
REQ_SPECTRUM_STATUS = (0x02, 0x03)
REQ_SPECTRUM_STATUS_CLEAR = (0x02, 0x04)
REQ_BUFFER_SPECTRUM = (0x02, 0x05)
REQ_BUFFER_CLEAR_SPECTRUM = (0x02, 0x06)
REQ_REQUEST_BUFFER = (0x02, 0x07)
REQ_SCOPE_DATA = (0x03, 0x01)
REQ_MISC_DATA = (0x03, 0x02)
REQ_SCOPE_DATA_REARM = (0x03, 0x03)
REQ_ETHERNET_SETTINGS = (0x03, 0x04)
REQ_DIAGNOSTIC_DATA = (0x03, 0x05)
REQ_NETFINDER_PACKET = (0x03, 0x07)
REQ_I2C_TRANSFER = (0x03, 0x08)
REQ_LIST_MODE_DATA = (0x03, 0x09)
REQ_OPTION_PA_CAL = (0x03, 0x0A) # MCA8000D
REQ_MINIX_TUBE_INTERLOCK = (0x03, 0x0B) # Mini-X2
REQ_MINIX_WARMUP_TABLE = (0x03, 0x0C) # Mini-X2
REQ_MINIX_TIMESTAMP = (0x03, 0x0D) # Mini-X2
REQ_MINIX_FAULT = (0x03, 0x0E) # Mini-X2
REQ_SCA_32BIT = (0x04, 0x01)
REQ_SCA_32BIT_LATCH = (0x04, 0x02)
REQ_SCA_32BIT_LATCH_CLEAR = (0x04, 0x03)
REQ_TEXT_CONFIG = (0x20, 0x02)
REQ_TEXT_CONFIG_READBACK = (0x20, 0x03)
REQ_TEXT_CONFIG_NO_SAVE = (0x20, 0x04)
REQ_CLEAR_SPECTRUM = (0xF0, 0x01)
REQ_ENABLE_MCA = (0xF0, 0x02)
REQ_DISABLE_MCA = (0xF0, 0x03)
REQ_ARM_SCOPE = (0xF0, 0x04)
REQ_AUTOSET_OFFSET = (0xF0, 0x05)
REQ_AUTOSET_FAST_THRESH = (0xF0, 0x06)
REQ_WRITE_IO = (0xF0, 0x08)
REQ_WRITE_MISC_DATA = (0xF0, 0x09)
REQ_COMM_TEST_ECHO = (0xF1, 0x7F)

# Response Packet PIDs (PID1 indicates category)
RESP_STATUS = (0x80, 0x01)
RESP_MINIX_STATUS = (0x80, 0x02) # Mini-X2
# PID1 = 0x81 -> Spectrum Data (PID2 varies by channel count/status)
# PID1 = 0x82 -> Other Data (Scope, Misc, Ethernet, Diag, Config, Netfinder, I2C, List, Cal, MiniX tables)
# PID1 = 0x83 -> SCA Data
RESP_COMM_TEST_ECHO = (0x8F, 0x7F)


class AmptekMCAError(Exception):
    """Custom exception for Amptek PX5 errors."""
    pass

class AmptekMCAAckError(AmptekMCAError):
    """Exception for receiving an error ACK from the device."""
    def __init__(self, pid1: int, pid2: int, data: Optional[bytes] = None):
        self.pid1 = pid1
        self.pid2 = pid2
        self.data = data
        ack_map = {
            ACK_SYNC_ERROR: "Sync Error",
            ACK_PID_ERROR: "PID Error",
            ACK_LEN_ERROR: "Length Error",
            ACK_CHECKSUM_ERROR: "Checksum Error",
            ACK_BAD_PARAMETER: "Bad Parameter",
            ACK_BAD_HEX_RECORD: "Bad Hex Record",
            ACK_UNRECOGNIZED_CMD: "Unrecognized Command",
            ACK_FPGA_ERROR: "FPGA Error",
            ACK_CP2201_NOT_FOUND: "Ethernet Controller Not Found",
            ACK_SCOPE_DATA_NA: "Scope Data Not Available",
            ACK_PC5_NOT_PRESENT: "PC5 Not Present",
            ACK_BUSY: "Device Busy",
            ACK_I2C_ERROR: "I2C Error",
            ACK_FEATURE_NOT_SUPPORTED: "Feature Not Supported by FPGA",
            ACK_CAL_DATA_NOT_PRESENT: "Calibration Data Not Present",
        }
        error_message = ack_map.get(pid2, f"Unknown ACK Error (PID2={pid2})")
        if data:
            try:
                # Attempt to decode ASCII command echo for specific errors
                if pid2 in [ACK_BAD_PARAMETER, ACK_UNRECOGNIZED_CMD, ACK_PC5_NOT_PRESENT]:
                     error_message += f": {data.decode('ascii', errors='ignore')}"
            except Exception:
                error_message += f": {data.hex()}" # Show hex if decode fails

        super().__init__(f"[Amptek MCA] ACK Error received: {error_message} (PID1={pid1}, PID2={pid2})")


class AmptekMCA():
    """
    Class to control various Amptek MCA and related devices via USB using pyusb.

    This class provides the USB communication handling according to the protocol defined
    in the Amptek Digital Products Programmer's Guide Rev B3.

    It supports the following devices:
        - DP5
        - PX5
        - DP5X
        - DP5G
        - MCA8000D
    """
    # USB Vendor and Product IDs
    VENDOR_ID = 0x10C4
    PRODUCT_ID = 0x842A
    VENDOR_ID_STR = "10C4"
    PRODUCT_ID_STR = "842A"
    # Default USB timeout in milliseconds
    DEFAULT_TIMEOUT = 2000
    # Longer timeout for potentially slow operations like diagnostics or spectrum reads
    LONG_TIMEOUT = 5000
    # Device ID mapping from status byte 39
    DEVICE_ID_MAP = {
        0: "DP5",
        1: "PX5",
        2: "DP5G",
        3: "MCA8000D",
        4: "TB5",
        5: "DP5-X",
    }

    def __init__(self,
                logger: Optional[logging.Logger] = None,
                logger_name: str = "AmptekMCA",
                logger_level: int = logging.INFO
            ) -> None:
        """
        Initialize the Amptek MCA communication class.

        Args:
            logger (Optional[logging.Logger]): An optional logger instance. If None,
                                       a new logger will be created with the provided name and level.
            logger_name (str): The name of the new logger. Defaults to "AmptekMCA".
            logger_level (int): The logging level for the new logger. Defaults to logging.INFO.
        """
        self.logger = logger if logger else LoggerUtils.get_logger(logger_name, level=logger_level)
        self.dev: Optional[usb.core.Device] = None
        self.ep_out: Optional[usb.core.Endpoint] = None
        self.ep_in: Optional[usb.core.Endpoint] = None
        self.last_status: Dict[str, Any] = {}
        self.model: str = None
        self.logger.info("[Amptek MCA] Amptek MCA class initialized.")

    def connect(self, device_index: int = 0) -> None:
        """
        Find the Amptek MCA device and establish a USB connection.
        If multiple devices match VID/PID, connect to the one at device_index.
        Claims the interface and finds the IN and OUT endpoints.
        Calls get_status() to save the status for later use.

        Args:
            device_index (int, optional): The 0-based index of the device to connect to.
                                        Defaults to 0 (the first device found).
        Raises:
            ValueError: If device_index is out of range.
            usb.core.NoDeviceError: If no matching devices are found.
            # Other exceptions like NoBackendError, USBError might be raised
        """
        if self.dev:
            self.logger.warning("[Amptek MCA] Already connected.")
            return

        self.logger.info(f"[Amptek MCA] Searching for devices (VID={self.VENDOR_ID:#06x}, PID={self.PRODUCT_ID:#06x})...")

        # Get the backend first
        backend = UsbUtils.get_libusb_backend()

        # Find *all* devices matching VID and PID
        devices = list(usb.core.find(find_all=True, idVendor=self.VENDOR_ID,  idProduct=self.PRODUCT_ID, backend=backend))

        # Check if devices were found
        if not devices:
            self.logger.error("[Amptek MCA] No matching devices found.")
            raise RuntimeError("No matching Amptek MCA devices found.")

        self.logger.info(f"[Amptek MCA] Found {len(devices)} matching device(s).")

        # Validate the index
        if not (0 <= device_index < len(devices)):
            self.logger.error(f"[Amptek MCA] Device index {device_index} is out of range (found {len(devices)} devices).")
            raise ValueError(f"Device index {device_index} is out of range. Valid indices: 0 to {len(devices) - 1}.")

        # Select the device using the index
        self.dev = devices[device_index]
        self.logger.info(f"[Amptek MCA] Selected device at index {device_index} (Bus: {self.dev.bus}, Address: {self.dev.address}).")

        if self.dev is None:
            self.logger.error("[Amptek MCA] Device not found.")
            raise AmptekMCAError("Amptek MCA device not found.")

        self.logger.info("[Amptek MCA] Device found.")

        try:
            # Detach kernel driver if active (Linux/macOS)
            if self.dev.is_kernel_driver_active(0):
                self.logger.debug("[Amptek MCA] Detaching kernel driver.")
                self.dev.detach_kernel_driver(0)
        except usb.core.USBError as e:
             # This can happen if the driver is already detached or on Windows
             self.logger.warning(f"[Amptek MCA] Could not detach kernel driver (might be okay): {e}")
        except NotImplementedError:
             # is_kernel_driver_active not implemented on all platforms (e.g., Windows)
             self.logger.debug("[Amptek MCA] Kernel driver check not applicable on this platform.")


        try:
            # Set the active configuration. Needs to be done before claiming interface.
            self.dev.set_configuration()
            self.logger.debug("[Amptek MCA] Set USB configuration.")
        except usb.core.USBError as e:
            self.logger.error(f"[Amptek MCA] Could not set configuration: {e}")
            self.dev = None # Ensure disconnect releases nothing
            raise AmptekMCAError(f"Failed to set USB configuration: {e}")

        # Get an endpoint instance
        cfg = self.dev.get_active_configuration()
        intf = cfg[(0,0)] # First interface

        # Find IN and OUT endpoints (based on Programmer's Guide EP1 IN, EP2 OUT)
        # EP1 IN -> Address 0x81
        # EP2 OUT -> Address 0x02
        self.ep_in = usb.util.find_descriptor(
            intf,
            # Match the first IN endpoint
            custom_match = \
            lambda e: usb.util.endpoint_direction(e.bEndpointAddress) == \
            usb.util.ENDPOINT_IN and e.bEndpointAddress == 0x81
        )

        self.ep_out = usb.util.find_descriptor(
            intf,
            # Match the first OUT endpoint
            custom_match = \
            lambda e: usb.util.endpoint_direction(e.bEndpointAddress) == \
            usb.util.ENDPOINT_OUT and e.bEndpointAddress == 0x02
        )

        if self.ep_out is None or self.ep_in is None:
            self.logger.error("[Amptek MCA] Could not find IN or OUT endpoint.")
            self.dev = None # Ensure disconnect releases nothing
            raise AmptekMCAError("Could not find required USB endpoints (0x81 IN, 0x02 OUT).")

        self.logger.debug(f"[Amptek MCA] Found EP IN address: {self.ep_in.bEndpointAddress:#04x}")
        self.logger.debug(f"[Amptek MCA] Found EP OUT address: {self.ep_out.bEndpointAddress:#04x}")
        self.logger.info("[Amptek MCA] Connection established.")

        # Get the status after connecting
        self.logger.info("[Amptek MCA] Requesting initial status...")
        self.get_status()  # Error handling is done in get_status()
        self.logger.info("[Amptek MCA] Initial status received.")
        self.logger.info(f"[Amptek MCA] Device model: {self.model}")

    def disconnect(self) -> None:
        """
        Release the USB interface and close the device connection.
        """
        if self.dev is not None:
            self.logger.info("[Amptek MCA] Disconnecting...")
            try:
                usb.util.dispose_resources(self.dev)
                self.logger.debug("[Amptek MCA] Disposed USB resources.")
            except usb.core.USBError as e:
                 self.logger.warning(f"[Amptek MCA] Error during disconnect/resource disposal: {e}")
            except Exception as e:
                 self.logger.error(f"[Amptek MCA] Unexpected error during disconnect: {e}")
            finally:
                self.dev = None
                self.ep_in = None
                self.ep_out = None
                self.logger.info("[Amptek MCA] Disconnected.")
        else:
            self.logger.info("[Amptek MCA] Already disconnected.")

    def _calculate_checksum(self, packet_bytes: bytes) -> int:
        """
        Calculates the 16-bit checksum for a given packet (excluding checksum bytes).
        Checksum is the two's complement of the 16-bit sum of all bytes prior
        to the checksum itself.
        Args:
            packet_bytes: The bytes of the packet header and data (if any).
        Returns:
            The 16-bit checksum value.
        """
        current_sum = sum(packet_bytes)
        # Calculate two's complement for 16 bits
        checksum = (~current_sum + 1) & 0xFFFF
        return checksum

    def _build_request_packet(self, pid1: int, pid2: int, data: Optional[bytes] = None) -> bytes:
        """
        Builds a request packet according to the Amptek protocol.
        Args:
            pid1: Packet ID byte 1.
            pid2: Packet ID byte 2.
            data: Optional data payload for the packet.
        Returns:
            The complete packet as bytes, including header and checksum.
        """
        data_len = len(data) if data else 0
        if data_len > 512: # Max data size for request packets
             raise ValueError("Request data field cannot exceed 512 bytes.")

        # Header: SYNC1, SYNC2, PID1, PID2, LEN_MSB, LEN_LSB
        header = struct.pack('>BBBBH', SYNC_BYTE_1, SYNC_BYTE_2, pid1, pid2, data_len)

        # Combine header and data for checksum calculation
        packet_base = header + (data if data else b'')

        # Calculate checksum
        checksum = self._calculate_checksum(packet_base)

        # Append checksum (MSB, LSB)
        full_packet = packet_base + struct.pack('>H', checksum)
        return full_packet

    def _send_request(self, pid1: int, pid2: int, data: Optional[bytes] = None, timeout: Optional[int] = None) -> None:
        """
        Builds and sends a request packet to the device's OUT endpoint.
        Args:
            pid1: Packet ID byte 1.
            pid2: Packet ID byte 2.
            data: Optional data payload for the packet.
            timeout: Optional USB write timeout in milliseconds. Uses DEFAULT_TIMEOUT if None.
        Raises:
            AmptekMCAError: If not connected or if USB write fails.
            ValueError: If data payload is too large.
        """
        if not self.dev or not self.ep_out:
            raise AmptekMCAError("Not connected to the device.")

        packet = self._build_request_packet(pid1, pid2, data)
        write_timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT

        self.logger.debug(f"[Amptek MCA] Sending packet (PID1={pid1}, PID2={pid2}, LEN={len(data) if data else 0}): {packet.hex()}")

        try:
            bytes_written = self.ep_out.write(packet, timeout=write_timeout)
            if bytes_written != len(packet):
                 raise AmptekMCAError(f"USB write error: Tried to write {len(packet)} bytes, but wrote {bytes_written}.")
            self.logger.debug(f"[Amptek MCA] Wrote {bytes_written} bytes.")
        except usb.core.USBTimeoutError:
            self.logger.error(f"[Amptek MCA] USB write timed out after {write_timeout}ms.")
            raise AmptekMCAError("USB write timed out.")
        except usb.core.USBError as e:
            self.logger.error(f"[Amptek MCA] USB write error: {e}")
            raise AmptekMCAError(f"USB write failed: {e}")

    def _read_response(self, timeout: Optional[int] = None) -> Tuple[int, int, Optional[bytes]]:
        """
        Reads a response packet from the device's IN endpoint.
        Parses the header, reads data and checksum, validates the packet.
        Args:
            timeout: Optional USB read timeout in milliseconds. Uses DEFAULT_TIMEOUT if None.
        Returns:
            A tuple containing (PID1, PID2, data_payload). data_payload is None
            if the packet has no data field (LEN=0).
        Raises:
            AmptekMCAError: If not connected, USB read fails, timeout occurs,
                            or packet validation fails (sync, checksum).
            AmptekMCAAckError: If a valid ACK error packet is received.
        """
        if not self.dev or not self.ep_in:
            raise AmptekMCAError("Not connected to the device.")

        read_timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT
        header = b''
        data_payload = None

        try:
            # Read the 6-byte header first
            # Max response size is 32775 bytes (header + data + checksum).
            self.logger.debug(f"[Amptek MCA] Reading header (expecting 6 bytes...")

            header = bytes(self.ep_in.read(6, timeout=read_timeout))
            if len(header) < 6:
                 raise AmptekMCAError(f"USB read error: Expected 6 header bytes, got {len(header)}.")

            self.logger.debug(f"[Amptek MCA] Read header: {header.hex()}")

            # Parse header
            sync1, sync2, pid1, pid2, data_len = struct.unpack('>BBBBH', header)

            # Validate sync bytes
            if sync1 != SYNC_BYTE_1 or sync2 != SYNC_BYTE_2:
                self.logger.error(f"[Amptek MCA] Invalid sync bytes received: {sync1:#04x} {sync2:#04x}")
                raise AmptekMCAError(f"Invalid sync bytes: Expected {SYNC_BYTE_1:#04x} {SYNC_BYTE_2:#04x}, got {sync1:#04x} {sync2:#04x}")

            self.logger.debug(f"[Amptek MCA] Received packet header: PID1={pid1}, PID2={pid2}, LEN={data_len}")

            # Read data payload (if any) and checksum (2 bytes)
            bytes_to_read = data_len + 2
            full_response_data = b''
            if bytes_to_read > 0:
                self.logger.debug(f"[Amptek MCA] Reading data ({data_len} bytes) and checksum (2 bytes)...")
                full_response_data = bytes(self.ep_in.read(bytes_to_read, timeout=read_timeout))

                if len(full_response_data) < bytes_to_read:
                    raise AmptekMCAError(f"USB read error: Expected {bytes_to_read} data+checksum bytes, got {len(full_response_data)}.")

                self.logger.debug(f"[Amptek MCA] Read {len(full_response_data)} data+checksum bytes.")
                if data_len > 0:
                    data_payload = full_response_data[:data_len]
                received_checksum = struct.unpack('>H', full_response_data[data_len:])[0]
            else:
                 # Read the checksum
                 self.logger.debug("[Amptek MCA] Reading checksum (2 bytes) for LEN=0 packet...")
                 checksum_bytes = bytes(self.ep_in.read(2, timeout=read_timeout))
                 if len(checksum_bytes) < 2:
                     raise AmptekMCAError(f"USB read error: Expected 2 checksum bytes, got {len(checksum_bytes)}.")
                 received_checksum = struct.unpack('>H', checksum_bytes)[0]

            # Validate checksum
            packet_base = header + (data_payload if data_payload else b'')
            calculated_checksum = self._calculate_checksum(packet_base)

            if received_checksum != calculated_checksum:
                self.logger.error(f"[Amptek MCA] Checksum error! Received={received_checksum:#06x}, Calculated={calculated_checksum:#06x}")
                raise AmptekMCAError(f"Checksum mismatch: Received={received_checksum:#06x}, Calculated={calculated_checksum:#06x}")

            self.logger.debug(f"[Amptek MCA] Checksum OK ({received_checksum:#06x}).")

            # Check if it's an ACK Error packet (PID1 = 0xFF, PID2 != 0x00, 0x0C, 0x0F)
            if pid1 == 0xFF and pid2 not in [ACK_OK, ACK_OK_SHARING_REQ, ACK_OK_FPGA_UPLOAD_ADDR]:
                self.logger.warning(f"[Amptek MCA] Received ACK Error: PID2={pid2}")
                raise AmptekMCAAckError(pid1, pid2, data_payload)

            # Return PID1, PID2, and the data payload
            return pid1, pid2, data_payload

        except usb.core.USBTimeoutError:
            self.logger.error(f"[Amptek MCA] USB read timed out after {read_timeout}ms.")
            raise AmptekMCAError("USB read timed out.")
        except usb.core.USBError as e:
            # Handle potential pipe errors (e.g., stall) which might indicate device state issues
            if e.errno == 32: # errno.EPIPE Broken pipe - often indicates stall
                 self.logger.error(f"[Amptek MCA] USB pipe error (stall?): {e}. Clearing stall if possible.")
                 try:
                     # Attempt to clear stall on the IN endpoint
                     self.dev.clear_halt(self.ep_in.bEndpointAddress)
                     self.logger.info("[Amptek MCA] Cleared stall on EP IN.")
                 except Exception as clear_err:
                     self.logger.error(f"[Amptek MCA] Failed to clear stall: {clear_err}")
                 raise AmptekMCAError(f"USB pipe error (stall?): {e}")
            else:
                 self.logger.error(f"[Amptek MCA] USB read error: {e}")
                 raise AmptekMCAError(f"USB read failed: {e}")
        except AmptekMCAAckError as e:
            # Propagate ACK errors directly
            raise e
        except Exception as e:
            # Catch any other unexpected errors during read/parse
            self.logger.error(f"[Amptek MCA] Unexpected error during response read/parse: {e}")
            raise AmptekMCAError(f"Unexpected error reading response: {e}")


    # --- Public Command Methods ---

    def get_status_bytes(self, silent: bool = False) -> bytes:
        """
        Requests and returns the 64-byte status data from the device.
        args:
            silent (bool): If True, all the .info messages are replaced with .debug.
        Returns:
            The 64-byte status data payload.
        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK.
        """
        silent_log = self.logger.debug if silent else self.logger.info
        silent_log("[Amptek MCA] Requesting status...")
        self._send_request(REQ_STATUS[0], REQ_STATUS[1])
        pid1, pid2, data = self._read_response()

        if (pid1, pid2) != RESP_STATUS:
            # Could be Mini-X status or an unexpected response
            if (pid1, pid2) == RESP_MINIX_STATUS:
                self.logger.error("[Amptek MCA] Received Mini-X Status. This device is not supported.")
                raise AmptekMCAError("Received Mini-X Status. This device is not supported.")
            else:
                 raise AmptekMCAError(f"Unexpected response packet received for Get Status: PID1={pid1}, PID2={pid2}")

        if data is None or len(data) != 64:
             raise AmptekMCAError(f"Invalid status data received: Length={len(data) if data else 'None'}")

        silent_log("[Amptek MCA] Status received successfully.")
        return data

    def get_status(self, silent: bool = False) -> Dict[str, Any]:
        """
        Requests the standard status packet and parses it into a dictionary.

        It calls get_status_bytes internally and then interprets the fields.

        Args:
            silent (bool): If True, all the .info messages are replaced with .debug.

        Returns:
            A dictionary containing the parsed status information.

        Raises:
            AmptekMCAError: If connection or communication fails, or if the
                            received data cannot be parsed correctly.
            AmptekMCAAckError: If the device returns an error ACK.
        """
        status_bytes = self.get_status_bytes(silent=silent)
        self.logger.debug(f"[Amptek MCA] Parsing status bytes...")
        status_dict: Dict[str, Any] = {}

        try:
            # Parse counters and timers (assuming little-endian based on SN example)
            status_dict['fast_count'] = struct.unpack('<I', status_bytes[0:4])[0]
            status_dict['slow_count'] = struct.unpack('<I', status_bytes[4:8])[0]
            status_dict['gp_counter'] = struct.unpack('<I', status_bytes[8:12])[0]
            # Parse Acq Time: byte 12 (1ms/count) + bytes 13-15 (100ms/count)
            status_dict['acquisition_time_sec'] = (struct.unpack('<I', bytes([status_bytes[13], status_bytes[14], status_bytes[15], 0]))[0] * 0.1) + (status_bytes[12] * 0.001)
            # Real Time (1ms/count)
            status_dict['real_time_sec'] = struct.unpack('<I', status_bytes[20:24])[0] * 0.001

            # Parse versions and serial number
            fw_major = (status_bytes[24] >> 4) & 0x0F
            fw_minor = status_bytes[24] & 0x0F
            fw_build = status_bytes[37] & 0x0F
            status_dict['firmware_version'] = f"{fw_major}.{fw_minor:02d}.{fw_build:02d}"
            fpga_major = (status_bytes[25] >> 4) & 0x0F
            fpga_minor = status_bytes[25] & 0x0F
            status_dict['fpga_version'] = f"{fpga_major}.{fpga_minor:02d}"
            status_dict['serial_number'] = struct.unpack('<I', status_bytes[26:30])[0]

            # HV (signed short, 0.5V/count, byte 30=MSB, byte 31=LSB -> Big Endian)
            status_dict['hv'] = struct.unpack('>h', status_bytes[30:32])[0] * 0.5
            # Parse Detector Temp (Bytes 32-33): 12-bit value = (byte32 & 0x0F) << 8 | byte33, scale 0.1K/count
            detector_temp_value_12bit = ((status_bytes[32] & 0x0F) << 8) | status_bytes[33]
            status_dict['detector_temp_k'] = detector_temp_value_12bit * 0.1
            # Board Temp (signed byte, 1C/count)
            status_dict['board_temp_c'] = struct.unpack('<b', bytes([status_bytes[34]]))[0]

            # Parse device ID
            status_dict['device_id'] = self.DEVICE_ID_MAP.get(status_bytes[39], f"Unknown ({status_bytes[39]})")

            # Parse status flags from bytes 35, 36, and 38 into a single dictionary
            byte35 = status_bytes[35]
            byte36 = status_bytes[36]
            byte38 = status_bytes[38]
            # Get device type string determined earlier
            device_id = status_dict.get('device_id', 'Unknown') # Default if not found

            # Start with flags from byte 36 and common flags from byte 35
            flags = {
                # Byte 36 Flags
                'auto_input_offset_locked': not bool(byte36 & 0x80), # 0 means locked, 1 means searching
                'mcs_finished': bool(byte36 & 0x40),
                'is_first_packet_since_reboot': bool(byte36 & 0x20),
                'fpga_clock_80mhz': bool(byte36 & 0x02),
                'fpga_clock_auto_selected': bool(byte36 & 0x01),
                # Byte 35 Common Flags (excluding bit 6 for now)
                'preset_real_time_reached': bool(byte35 & 0x80),
                'mca_enabled': bool(byte35 & 0x20),
                'preset_counts_reached': bool(byte35 & 0x10),
                'gate_active': bool(byte35 & 0x08), # Note: Active low means stopping events
                'scope_data_ready': bool(byte35 & 0x04),
                'unit_configured': bool(byte35 & 0x02),
                # D0 TBD (Byte 35)
            }

            # Add flags from byte 38 based on device type
            # Bit 7: PC5/Jumper Status/Device OK
            if device_id in ["DP5", "DP5G", "TB5"]:
                flags['pc5_detected'] = bool(byte38 & 0x80)
            elif device_id == "PX5":
                flags['hv_jumper_ok'] = bool(byte38 & 0x80) # 0=Error, 1=Normal

            # Bit 6: HV Polarity (Not applicable for DP5G/TB5/MCA8000D)
            if device_id in ["DP5", "PX5", "DP5-X"]:
                flags['hv_polarity_positive'] = bool(byte38 & 0x40) # False=Negative

            # Bit 5: Preamp Supply Voltage (Not applicable for DP5G/TB5/DP5X/MCA8000D)
            if device_id in ["DP5", "PX5"]:
                flags['preamp_supply_8_5v'] = bool(byte38 & 0x20) # False means 5V

            # Handle byte 35, bit 6 (0x40) based on device type
            bit6_value = bool(byte35 & 0x40)
            if device_id == "MCA8000D":
                flags['preset_livetime_reached'] = bit6_value
            else:
                # Default interpretation for non-MCA8000D devices
                flags['auto_fast_thresh_locked'] = bit6_value

            # Assign the completed flags dictionary
            status_dict['status_flags'] = flags

            # Parse Bootloader version (Byte 48)
            bl_ver_map = {0xFF: "Original", 0x80: "7.00.00", 0x7F: "7.00.01"}
            status_dict['bootloader_version'] = bl_ver_map.get(status_bytes[48], f"Unknown ({status_bytes[48]:#04x})")

            self.logger.debug(f"[Amptek MCA] Status parsed successfully.")

        except struct.error as e:
            self.logger.error(f"[Amptek MCA] Failed to unpack status bytes: {e}")
            raise AmptekMCAError(f"Failed to parse status packet structure: {e}")
        except IndexError:
            # This shouldn't happen if get_status_bytes validated length, but good practice
            self.logger.error(f"[Amptek MCA] Status packet too short for parsing.")
            raise AmptekMCAError("Status packet too short for parsing.")
        except Exception as e:
            # Catch any other unexpected errors during parsing
            self.logger.error(f"[Amptek MCA] Unexpected error parsing status: {e}")
            raise AmptekMCAError(f"Unexpected error parsing status: {e}")

        self.last_status = status_dict # Save the status for later use
        if self.model is None:
            self.model = status_dict.get('device_id', 'Unknown')  # Set model if not already set

        return status_dict

    def get_last_status(self) -> Dict[str, Any]:
        """
        Returns the last status dictionary received from the device.
        If no status has been received yet, it will call get_status() to fetch the latest status.

        Returns:
            A dictionary containing the last status information.
        """
        if not self.last_status:
            self.get_status()  # Ensure we have the latest status
        return self.last_status
    
    def get_model(self) -> str:
        """
        Returns the model of the connected device.
        If no model has been set yet, returns 'Unknown'.

        Returns:
            The model string of the connected device.
        """
        return self.model if self.model else 'Unknown'

    def get_spectrum_bytes(self) -> bytes:
        """
        Requests and returns the raw spectrum data from the MCA device.

        Note: The number of bytes returned depends on the number of channels configured
        on the device (e.g., 256ch * 3 bytes/ch = 768 bytes).

        Returns:
            The raw spectrum data payload as bytes.

        Raises:
            AmptekMCAError: If connection or communication fails, or if an
                            unexpected response packet is received.
            AmptekMCAAckError: If the device returns an error ACK.
        """
        self.logger.info(f"[Amptek MCA] Requesting spectrum bytes...")
        # Send the Request Spectrum command (PID1=2, PID2=1)
        self._send_request(REQ_SPECTRUM[0], REQ_SPECTRUM[1])
        # Read the response, which should be a spectrum packet (PID1=0x81)
        # Use a longer timeout as spectrum reads can be large/slow
        pid1, pid2, data = self._read_response(timeout=self.LONG_TIMEOUT)

        # Check if the response is a spectrum packet (PID1=0x81)
        # PID2 indicates channel count for spectrum-only responses:
        # 1=256, 3=512, 5=1024, 7=2048, 9=4096, 0x0B(11)=8192
        expected_pid2_values = {1, 3, 5, 7, 9, 11}
        if pid1 != 0x81 or pid2 not in expected_pid2_values:
             raise AmptekMCAError(f"Unexpected response packet received for Get Spectrum: PID1={pid1:#04x}, PID2={pid2:#04x}. Expected PID1=0x81, PID2 in {expected_pid2_values}.")

        if data is None:
             # This shouldn't happen for a valid spectrum response, but check anyway
             raise AmptekMCAError("Received spectrum response packet but data payload is missing.")

        self.logger.info(f"[Amptek MCA] Spectrum bytes received successfully ({len(data)} bytes).")
        return data
    
    def get_spectrum(self) -> List[int]:
        """
        Requests spectrum data and parses it into a list of counts per channel.

        Calls get_spectrum_bytes internally and processes the raw byte data.
        Each channel count is represented by 3 bytes (24-bit unsigned integer),
        interpreted as little-endian.

        Returns:
            A list of integers, where each integer represents the counts in a channel,
            starting from channel 0.

        Raises:
            AmptekMCAError: If connection or communication fails, or if the
                            received spectrum data is invalid.
            AmptekMCAAckError: If the device returns an error ACK.
        """
        spectrum_bytes = self.get_spectrum_bytes()
        self.logger.debug("[Amptek MCA] Parsing spectrum bytes...")

        if len(spectrum_bytes) % 3 != 0:
            self.logger.error(f"[Amptek MCA] Invalid spectrum data length ({len(spectrum_bytes)} bytes), not divisible by 3.")
            raise AmptekMCAError("Invalid spectrum data length received.")

        num_channels = len(spectrum_bytes) // 3
        spectrum_counts = []

        try:
            # Iterate through the bytes, 3 at a time
            for i in range(0, len(spectrum_bytes), 3):
                # Extract 3 bytes for the channel count (LSB, Mid, MSB)
                chunk = spectrum_bytes[i:i+3]
                # Pad with a zero byte to unpack as a 4-byte unsigned integer (<I)
                count_bytes = bytes([chunk[0], chunk[1], chunk[2], 0])
                # Unpack as little-endian unsigned 32-bit int (value is 24-bit)
                count = struct.unpack('<I', count_bytes)[0]
                spectrum_counts.append(count)

            self.logger.debug(f"[Amptek MCA] Spectrum parsed successfully ({num_channels} channels).")

        except struct.error as e:
            self.logger.error(f"[Amptek MCA] Failed to unpack spectrum bytes: {e}")
            raise AmptekMCAError(f"Failed to parse spectrum data structure: {e}")
        except Exception as e:
            self.logger.error(f"[Amptek MCA] Unexpected error parsing spectrum: {e}")
            raise AmptekMCAError(f"Unexpected error parsing spectrum: {e}")

        return spectrum_counts

    def send_configuration(self, config_dict: Dict[str, Any], save_to_flash: bool = False) -> None:
        """
        Formats a configuration dictionary into ASCII command strings, splits them
        into multiple packets if necessary (max 512 data bytes per packet),
        and sends them sequentially to the device.

        Intermediate packets are sent using a non-saving command. The final packet
        uses a saving or non-saving command based on the save_to_flash flag.

        Args:
            config_dict: A dictionary where keys are the 4-character command mnemonics
                         (case-insensitive, will be converted to uppercase) and values
                         are the corresponding parameters (will be converted to string).
                         Example: {'RESC': 'Y', 'TPEA': 10.0, 'CLCK': 'AUTO'}
                         Note: Order might matter for some commands; use collections.OrderedDict
                         if specific command order is required and Python version < 3.7.
            save_to_flash: If True, the configuration is saved to non-volatile
                           memory after the final packet is acknowledged. If False,
                           the configuration is applied but not saved.
                            Default is False (non-saving command) to avoid memory degradation.

        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK during any packet transmission.
            ValueError: If any single formatted command exceeds 512 bytes or
                        if dictionary values cannot be converted to string.
        """
        self.logger.info(f"[Amptek MCA] Formatting and Sending Configuration (Save={save_to_flash})...")

        # 1. Format all commands into individual byte strings (CMD=VAL;)
        encoded_parts: List[bytes] = []
        reset_command_bytes: Optional[bytes] = None
        temp_config_dict = config_dict.copy() # Avoid modifying original dict

        # Check for RESC command case-insensitively and separate it
        resc_key = None
        for k in temp_config_dict:
            if str(k).upper() == 'RESC':
                resc_key = k
                break

        if resc_key:
            resc_val_str = str(temp_config_dict[resc_key]).upper()
            if resc_val_str in ['Y', 'YES', 'TRUE', '1']:
                try:
                    reset_command_bytes = f"RESC={resc_val_str};".encode('ascii')
                    if len(reset_command_bytes) > 512:
                         raise ValueError("RESC=Y; command format exceeds 512 bytes (unexpected).")
                    del temp_config_dict[resc_key] # Remove from main processing list
                    self.logger.debug("[Amptek MCA] RESC=Y command identified.")
                except UnicodeEncodeError:
                     raise ValueError("RESC command value contains non-ASCII characters.")

        # Format remaining commands
        for key, value in temp_config_dict.items():
            try:
                part_str = f"{str(key).upper()}={str(value)};"
                part_bytes = part_str.encode('ascii')
                if len(part_bytes) > 512:
                    raise ValueError(f"Single configuration command '{part_str}' is longer than 512 bytes.")
                encoded_parts.append(part_bytes)
            except UnicodeEncodeError:
                raise ValueError(f"Configuration key '{key}' or value '{value}' contains non-ASCII characters.")
            except Exception as e:
                 raise ValueError(f"Could not format configuration parameter for key '{key}': {e}")

        # 2. Build the list of packet data payloads (chunks <= 512 bytes)
        packet_payloads: List[bytes] = []
        current_payload = b''

        # Add RESC first if it exists
        if reset_command_bytes:
            current_payload += reset_command_bytes

        # Accumulate other parts
        for part_bytes in encoded_parts:
            # Check if adding this part would exceed the limit
            if len(current_payload) + len(part_bytes) <= 512:
                current_payload += part_bytes
            else:
                # Add the completed payload to the list (if it's not empty)
                if current_payload:
                    packet_payloads.append(current_payload)
                # Start the new payload with the current part
                current_payload = part_bytes

        # Add the last payload if it contains any data
        if current_payload:
            packet_payloads.append(current_payload)

        # 3. Send the list of payloads
        if not packet_payloads:
            self.logger.info("[Amptek MCA] No configuration packets to send.")
            return

        self.logger.info(f"[Amptek MCA] Sending {len(packet_payloads)} configuration packet(s)...")
        num_packets = len(packet_payloads)

        for i, payload in enumerate(packet_payloads):
            is_last_packet = (i == num_packets - 1)

            # Determine PID: Use NO_SAVE for intermediate, final based on flag
            if is_last_packet:
                pid1, pid2 = REQ_TEXT_CONFIG if save_to_flash else REQ_TEXT_CONFIG_NO_SAVE
                log_save_status = f"(Save={save_to_flash})"
            else:
                pid1, pid2 = REQ_TEXT_CONFIG_NO_SAVE
                log_save_status = "(Save=False - Intermediate)"

            self.logger.debug(f"[Amptek MCA] Sending packet #{i+1}/{num_packets} ({len(payload)} bytes) {log_save_status}...")
            self._send_request(pid1, pid2, payload)
            # Wait for ACK for each packet
            self._read_response(timeout=self.LONG_TIMEOUT) # Raises error on failure
            if save_to_flash:
                time.sleep(0.2) # Small delay to allow device to process

        self.logger.info(f"[Amptek MCA] Configuration sent successfully in {num_packets} packet(s).")

    def read_configuration_bytes(self, commands_to_read: List[str]) -> bytes:
        """
        Requests a readback of specified configuration commands from the device.

        Formats the list of commands into a template string, sends the
        readback request, and returns the raw ASCII response bytes from the device.

        Args:
            commands_to_read: A list of strings, where each string is a
                              4-character command mnemonic (e.g., ['TPEA', 'GAIN'])
                              or includes parameters if required (e.g., ['SCAI=1', 'SCAL']).
                              Case-insensitive, will be converted to uppercase.

        Returns:
            The raw response data payload as bytes (ASCII encoded string like "CMD1=VAL1;CMD2=VAL2;").

        Raises:
            AmptekMCAError: If connection or communication fails, or if an
                            unexpected response packet is received.
            AmptekMCAAckError: If the device returns an error ACK.
            ValueError: If the formatted template string exceeds 512 bytes or
                        contains non-ASCII characters.
        """
        if not commands_to_read:
            self.logger.warning("[Amptek MCA] No commands provided for configuration readback.")
            return b''

        self.logger.info("[Amptek MCA] Requesting text configuration readback...")

        # Format the list into the template string: CMD1;CMD2;SCAI=1;SCAL;
        # Commands are converted to uppercase. Add trailing semicolon.
        template_string = ";".join([cmd.upper() for cmd in commands_to_read]) + ";"
        self.logger.debug(f"[Amptek MCA] Readback template string: {template_string}")

        try:
            template_bytes = template_string.encode('ascii')
        except UnicodeEncodeError:
             raise ValueError("Command list contains non-ASCII characters.")

        if len(template_bytes) > 512:
             raise ValueError(f"Formatted readback template string exceeds maximum length of 512 bytes ({len(template_bytes)} bytes).")

        # Send the Readback Request
        pid1_req, pid2_req = REQ_TEXT_CONFIG_READBACK
        self._send_request(pid1_req, pid2_req, template_bytes)

        # Read the response (PID1=0x82, PID2=7 expected)
        pid1_resp, pid2_resp, data = self._read_response(timeout=self.DEFAULT_TIMEOUT)

        # Check response PID
        if pid1_resp != 0x82 or pid2_resp != 7:
            raise AmptekMCAError(f"Unexpected response packet received for Text Config Readback: PID1={pid1_resp:#04x}, PID2={pid2_resp:#04x}. Expected PID1=0x82, PID2=0x07.")

        if data is None:
             # Should not happen for this response type, but check
             raise AmptekMCAError("Received configuration readback response packet but data payload is missing.")

        self.logger.info(f"[Amptek MCA] Raw configuration readback bytes received successfully ({len(data)} bytes).")
        return data

    def read_configuration(self, commands_to_read: List[str]) -> Dict[str, str]:
        """
        Requests and parses the readback of specified configuration commands.

        Calls read_configuration_bytes internally and parses the resulting
        ASCII string ("CMD1=VAL1;CMD2=VAL2;") into a dictionary.

        Args:
            commands_to_read: A list of strings, where each string is a
                              4-character command mnemonic (e.g., ['TPEA', 'GAIN'])
                              or includes parameters if required (e.g., ['SCAI=1', 'SCAL']).

        Returns:
            A dictionary mapping command mnemonics (uppercase) to their current
            values as strings reported by the device (e.g., {'TPEA': '10.000', 'GAIN': '50.0'}).
            Unrecognized commands might return '??' or similar as the value.

        Raises:
            AmptekMCAError: If connection or communication fails, or if the
                            received data cannot be parsed correctly.
            AmptekMCAAckError: If the device returns an error ACK.
            ValueError: If the input command list leads to an invalid request.
        """
        response_bytes = self.read_configuration_bytes(commands_to_read)
        self.logger.debug("[Amptek MCA] Parsing configuration readback response...")

        readback_dict: Dict[str, str] = {}
        try:
            # Decode the response bytes (should be ASCII)
            response_string = response_bytes.decode('ascii')
            self.logger.debug(f"[Amptek MCA] Decoded response string: {response_string}")

            # Split the response string by semicolon and parse each part
            parts = response_string.split(';')
            for part in parts:
                part = part.strip() # Remove leading/trailing whitespace if any
                if not part:
                    continue # Skip empty parts resulting from split

                # Split CMD=VAL, handle cases where '=' might be missing or value is empty
                command_value = part.split('=', 1)
                command = command_value[0] # Command is always the first part
                value = command_value[1] if len(command_value) > 1 else "" # Value is the rest, or empty string

                readback_dict[command] = value # Store value as string

            self.logger.debug("[Amptek MCA] Configuration readback parsed successfully.")

        except UnicodeDecodeError:
            self.logger.error("[Amptek MCA] Failed to decode readback response as ASCII.")
            raise AmptekMCAError("Failed to decode configuration readback response.")
        except Exception as e:
            self.logger.error(f"[Amptek MCA] Unexpected error parsing readback response: {e}")
            raise AmptekMCAError(f"Unexpected error parsing configuration readback response: {e}")

        return readback_dict


    def clear_spectrum(self) -> None:
        """
        Sends the command to clear the MCA spectrum buffer and associated status values
        (like counters and timers). Also resets the List-mode FIFO.

        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK instead of ACK OK.
        """
        self.logger.info("[Amptek MCA] Sending Clear Spectrum command...")
        # Send the Clear Spectrum command (PID 0xF0, 0x01)
        self._send_request(REQ_CLEAR_SPECTRUM[0], REQ_CLEAR_SPECTRUM[1])

        # Wait for the ACK OK response
        # _read_response will raise AmptekMCAAckError for error ACKs
        # or AmptekMCAError for communication issues.
        pid1, pid2, _ = self._read_response(timeout=self.DEFAULT_TIMEOUT)

        # Verify it was specifically ACK_OK, although _read_response handles errors
        if not (pid1 == 0xFF and pid2 == ACK_OK):
             # This case should ideally not be reached if _read_response works correctly
             raise AmptekMCAError(f"Unexpected non-error response received for Clear Spectrum: PID1={pid1:#04x}, PID2={pid2:#04x}")

        self.logger.info("[Amptek MCA] Clear Spectrum command acknowledged.")

    def enable_mca(self) -> None:
        """
        Sends the command to enable MCA/MCS data acquisition.
        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK.
        """
        self.logger.info("[Amptek MCA] Sending Enable MCA command...")
        self._send_request(REQ_ENABLE_MCA[0], REQ_ENABLE_MCA[1])
        pid1, pid2, _ = self._read_response() # Expecting ACK

        if pid1 != 0xFF or pid2 != ACK_OK:
            raise AmptekMCAError(f"Unexpected response received for Enable MCA: PID1={pid1}, PID2={pid2}")

        self.logger.info("[Amptek MCA] Enable MCA command acknowledged.")

    def disable_mca(self) -> None:
        """
        Sends the command to disable MCA/MCS data acquisition.
        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK.
        """
        self.logger.info("[Amptek MCA] Sending Disable MCA command...")
        self._send_request(REQ_DISABLE_MCA[0], REQ_DISABLE_MCA[1])
        pid1, pid2, _ = self._read_response() # Expecting ACK

        if pid1 != 0xFF or pid2 != ACK_OK:
            raise AmptekMCAError(f"Unexpected response received for Disable MCA: PID1={pid1}, PID2={pid2}")

        self.logger.info("[Amptek MCA] Disable MCA command acknowledged.")

    def start_autoset_input_offset(self) -> None:
        """
        Sends the command to start the automatic input offset adjustment process.

        NOTE: This command returns ACK OK immediately and does NOT wait for the
        autoset process to complete. The status must be polled separately
        using the get_status() method and checking the 'auto_input_offset_locked'
        flag in the 'status_flags' dictionary (False means locked, True means searching).

        Supported Devices: DP5, PX5. Not supported on DP5G. Check device
        compatibility before calling.

        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK instead of ACK OK.
        """
        self.logger.info("[Amptek MCA] Sending Autoset Input Offset command...")
        # Send the Autoset Input Offset command (PID 0xF0, 0x05)
        self._send_request(REQ_AUTOSET_OFFSET[0], REQ_AUTOSET_OFFSET[1])

        # Wait for the ACK OK response
        # _read_response will raise AmptekMCAAckError for error ACKs
        # or AmptekMCAError for communication issues.
        pid1, pid2, _ = self._read_response(timeout=self.DEFAULT_TIMEOUT)

        # Verify it was specifically ACK_OK
        if not (pid1 == 0xFF and pid2 == ACK_OK):
             raise AmptekMCAError(f"Unexpected non-error response received for Autoset Input Offset: PID1={pid1:#04x}, PID2={pid2:#04x}")

        self.logger.info("[Amptek MCA] Autoset Input Offset command acknowledged (process started).")

    def start_autoset_fast_threshold(self) -> None:
        """
        Sends the command to start the automatic fast threshold adjustment process.

        NOTE: This command returns ACK OK immediately and does NOT wait for the
        autoset process to complete. The status must be polled separately
        using the get_status() method and checking the 'auto_fast_thresh_locked'
        flag in the 'status_flags' dictionary (True means locked).
        (Exception: For MCA8000D, this status bit indicates Preset Livetime).

        The resulting threshold value can be read using read_configuration(['THFA']).

        IMPORTANT: This function requires no input counts (other than noise) to
        work correctly, according to the documentation.

        Raises:
            AmptekMCAError: If connection or communication fails.
            AmptekMCAAckError: If the device returns an error ACK instead of ACK OK.
        """
        self.logger.info("[Amptek MCA] Sending Autoset Fast Threshold command...")
        # Send the Autoset Fast Threshold command (PID 0xF0, 0x06)
        self._send_request(REQ_AUTOSET_FAST_THRESH[0], REQ_AUTOSET_FAST_THRESH[1])

        # Wait for the ACK OK response
        # _read_response will raise AmptekMCAAckError for error ACKs
        # or AmptekMCAError for communication issues.
        pid1, pid2, _ = self._read_response(timeout=self.DEFAULT_TIMEOUT)

        # Verify it was specifically ACK_OK
        if not (pid1 == 0xFF and pid2 == ACK_OK):
             raise AmptekMCAError(f"Unexpected non-error response received for Autoset Fast Threshold: PID1={pid1:#04x}, PID2={pid2:#04x}")

        self.logger.info("[Amptek MCA] Autoset Fast Threshold command acknowledged (process started).")

    def echo_test_bytes(self, data_to_echo: bytes) -> bytes:
        """
        Sends raw byte data to the device's echo command and returns the raw echoed bytes.

        Args:
            data_to_echo: The bytes to send. Max length 512.

        Returns:
            The raw byte data echoed back by the device.

        Raises:
            AmptekMCAError: If connection or communication fails, or response mismatch.
            AmptekMCAAckError: If the device returns an error ACK.
            ValueError: If data_to_echo exceeds 512 bytes.
        """
        if len(data_to_echo) > 512:
             raise ValueError("Echo data cannot exceed 512 bytes.")

        self.logger.info(f"[Amptek MCA] Sending Echo Test with {len(data_to_echo)} bytes...")
        # Send Echo command (PID 0xF1, 0x7F)
        self._send_request(REQ_COMM_TEST_ECHO[0], REQ_COMM_TEST_ECHO[1], data_to_echo)
        # Read Echo response (PID 0x8F, 0x7F)
        pid1, pid2, data = self._read_response()

        # Validate response PID
        if (pid1, pid2) != RESP_COMM_TEST_ECHO:
             raise AmptekMCAError(f"Unexpected response packet received for Echo Test: PID1={pid1:#04x}, PID2={pid2:#04x}")

        # Check if received data matches sent data
        if data != data_to_echo:
             self.logger.error(f"[Amptek MCA] Echo mismatch! Sent: {data_to_echo.hex()}, Received: {data.hex() if data else 'None'}")
             raise AmptekMCAError("Echo data mismatch.")

        # Ensure data is not None before returning
        returned_data = data if data is not None else b''
        self.logger.info(f"[Amptek MCA] Echo Test successful ({len(returned_data)} bytes).")
        return returned_data

    def echo_test(self, text_to_echo: str, encoding: str = 'ascii') -> str:
        """
        Sends a string to the device's echo command and returns the echoed string.

        Handles encoding the string before sending and decoding the response.

        Args:
            text_to_echo: The string to send.
            encoding: The encoding to use (default: 'ascii').

        Returns:
            The string echoed back by the device.

        Raises:
            AmptekMCAError: If connection or communication fails, or response mismatch.
            AmptekMCAAckError: If the device returns an error ACK.
            ValueError: If the encoded string exceeds 512 bytes or encoding/decoding fails.
        """
        self.logger.info(f"[Amptek MCA] Sending Echo Test with string: '{text_to_echo}' using {encoding} encoding...")
        try:
            encoded_data = text_to_echo.encode(encoding)
        except UnicodeEncodeError as e:
            self.logger.error(f"[Amptek MCA] Failed to encode string using {encoding}: {e}")
            raise ValueError(f"Failed to encode string using {encoding}: {e}")

        # Use the byte-level method to handle communication and validation
        received_bytes = self.echo_test_bytes(encoded_data)

        try:
            decoded_string = received_bytes.decode(encoding)
            self.logger.info("[Amptek MCA] Echo string received and decoded successfully.")
            return decoded_string
        except UnicodeDecodeError as e:
            self.logger.error(f"[Amptek MCA] Failed to decode echoed bytes using {encoding}: {e}")
            raise AmptekMCAError(f"Failed to decode echoed response using {encoding}: {e}")

    def get_parameters_info(self,
                           param_names: Union[str, List[str]],
                           required_params: Optional[Dict[str, Any]] = None
                           ) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves metadata for specified configuration parameters.

        Args:
            param_names: A single parameter name (string) or a list of parameter
                         names (e.g., "GAIN", ["TPEA", "MCAC", "VOLU"]). Case-insensitive.
            required_params: Some parameters require additional parameters to be set, such as
                            "MCSL" and "MCSH" which depend on the current MCAC value.
                            Use this argument to specify any required parameter values.
                            If not provided, the required parameters will be fetched from the device.

        Returns:
            A dictionary where keys are the requested parameter names (uppercase)
            and values are dictionaries containing metadata:
            {
                "PARAM1": {
                    "type": str, # Python type name ('str', 'int', 'float', 'bool', 'tuple')
                    "doc": str,  # Description
                    "range": tuple | list | str | None, # Min/Max, list, or descriptive string
                    "allowed_values": list[str] | None, # For string parameters
                    "supported": bool, # Is the parameter supported by this device model?
                    "error": str | None # Error message if info retrieval failed
                },
                "PARAM2": {...}
            }
        """
        param_info_result: Dict[str, Dict[str, Any]] = {}
        internal_status: Optional[Dict[str, Any]] = None # To store status 
        mcac_val_str: Optional[str] = None # To store MCAC value, needed for MCSL/MCSH range

        # 1. Determine Device ID
        device_id_str = self.get_model() # Get model from internal state

        # 2. Normalize input parameter names to a list of uppercase strings
        if isinstance(param_names, str):
            param_names_list = [param_names.upper()]
        else:
            param_names_list = [name.upper() for name in param_names]

        # 3. Fetch MCAC value *if* MCSL or MCSH range calculation is needed
        mcac_val_str = required_params.get('MCAC', None) if required_params else None
        if not mcac_val_str and ('MCSL' in param_names_list or 'MCSH' in param_names_list):
            self.logger.debug("[Amptek MCA] MCSL/MCSH requested, fetching current MCAC value for range calculation...")
            try:
                mcac_config = self.read_configuration(['MCAC'])
                mcac_val_str = mcac_config.get('MCAC')
                if not mcac_val_str:
                    self.logger.warning("[Amptek MCA] Failed to read back MCAC value for MCSL/MCSH range.")
            except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
                self.logger.warning(f"[Amptek MCA] Error reading MCAC value for MCSL/MCSH range: {e}. Range might be inaccurate.")
                # Proceed without MCAC value, range logic will use default
        if not mcac_val_str:
            mcac_val_str = "1024"

        # 4. Process each requested parameter using internal logic
        for param_name in param_names_list:
            info: Dict[str, Any] = {
                "type": "unknown",
                "doc": "",
                "range": None,
                "allowed_values": None,
                "supported": False, # Default to not supported
                "error": None
            }

            # --- Internal Logic per Parameter ---
            if param_name == "GAIN":
                info["type"] = "float"
                info["doc"] = "Sets the total gain (analog * fine)."
                if device_id_str == "DP5": info["range"], info["supported"] = (0.75, 150.0), True
                elif device_id_str == "PX5": info["range"], info["supported"] = (0.75, 500.0), True
                elif device_id_str in ["DP5G", "TB5"]: info["range"], info["supported"] = (1.0, 10.0), True
                elif device_id_str == "MCA8000D": info["allowed_values"], info["type"], info["supported"] = [1.0, 10.0], "float", True
                elif device_id_str == "DP5-X": info["range"], info["supported"] = (2.67, 150.0), True
                else: info["error"] = "Range unknown for this device."

            elif param_name == "HVSE":
                info["type"] = "(float, str)"
                info["doc"] = "Sets High Voltage supply value (Volts)."
                info["allowed_values"] = ["OFF"]
                info["supported"] = False
                hv_range = None

                if internal_status is None: # Fetch status if not already done
                    self.logger.debug(f"[Amptek MCA] Fetching status to determine HV polarity for {param_name} range...")
                    try:
                        internal_status = self.get_last_status()
                    except (AmptekMCAError, AmptekMCAAckError) as e:
                         self.logger.warning(f"[Amptek MCA] Could not fetch status for HV polarity check: {e}")
                         internal_status = {}

                is_positive = internal_status.get('status_flags', {}).get('hv_polarity_positive')

                if device_id_str in ["DP5", "TB5"]:
                    if is_positive is True: hv_range = (0.0, 1500.0)
                    elif is_positive is False: hv_range = (-1500.0, 0.0)
                    else: info["error"] = "Could not determine PC5 polarity from status."

                elif device_id_str == "PX5":
                    is_hpge = internal_status.get('px5_options_42', {}).get('option_code') == 1
                    max_v = 5000.0 if is_hpge else 1500.0
                    if is_positive is True: hv_range = (0.0, max_v)
                    elif is_positive is False: hv_range = (-max_v, 0.0)
                    else: info["error"] = "Could not determine PX5 HV polarity from status."

                elif device_id_str == "DP5-X":
                    max_v = 300.0
                    if is_positive is True: hv_range = (0.0, max_v)
                    elif is_positive is False: hv_range = (-max_v, 0.0)
                    else: info["error"] = "Could not determine DP5-X HV polarity from status."

                info["range"] = hv_range

            elif param_name == "MCAC":
                info["type"] = "int"
                info["doc"] = "Select Number of MCA Channels."
                info["allowed_values"] = [256, 512, 1024, 2048, 4096, 8192]

            elif param_name in ["MCSL", "MCSH"]:
                info["type"] = "int"
                info["doc"] = f"Sets {'Low' if param_name == 'MCSL' else 'High'} Threshold for MCS."
                mcac_val = int(mcac_val_str)
                max_ch = mcac_val - 1
                info["range"] = (0, max_ch)

            elif param_name == "MCST":
                 info["type"] = "float"
                 info["doc"] = "Sets the MCS Timebase in seconds (10ms precision)."
                 info["range"] = (0.01, 655.35)

            elif param_name == "PAPS":
                 info["type"] = "(str, float)"
                 info["doc"] = "Controls Preamp Power Supplies."
                 if device_id_str == "DP5": info["allowed_values"] = ["8.5", "5", "OFF", "ON"]
                 elif device_id_str == "PX5": info["allowed_values"] = ["8.5", "5", "OFF"]

            elif param_name == "PREC":
                 info["type"] = "(int, str)"
                 info["doc"] = "Preset Counts (0 to 2^32-1 or OFF)."
                 info["allowed_values"] = ["OFF"]
                 info["range"] = (0, 4294967295)

            elif param_name == "PRER":
                 info["type"] = "(float, str)"
                 info["doc"] = "Preset Real Time in seconds (precision 0.01s or 0.001s)."
                 info["allowed_values"] = ["OFF"]
                 info["range"] = (0.0, 4294967.29)

            elif param_name == "PRET":
                 info["type"] = "(float, str)"
                 info["doc"] = "Preset Acquisition Time in seconds (precision 0.1s)."
                 info["allowed_values"] = ["OFF"]
                 info["range"] = (0.0, 99999999.9)

            elif param_name == "TECS":
                 info["type"] = "(int, str)"
                 info["doc"] = "Sets Thermoelectric Cooler temperature setpoint in Kelvin."
                 info["allowed_values"] = ["OFF"]
                 info["range"] = (0, 299)

            elif param_name == "VOLU":
                 info["type"] = "str"
                 info["doc"] = "Controls the speaker volume (PX5 only)."
                 info["allowed_values"] = ["ON", "OFF"]

            else:
                info["error"] = "Parameter metadata logic not fully implemented in get_parameter_info."

            # Support
            info["supported"] = self.parameter_is_supported(param_name, device_id_str)
            if not info["supported"]:
                info["range"] = None
                info["allowed_values"] = None
                msg = "Parameter not supported by this device."
                if info["error"] is None:
                    info["error"] = msg
                else:
                    info["error"] += f" {msg}"

            param_info_result[param_name] = info
            # --- End Internal Logic ---

        return param_info_result

    def get_parameter_info(self,
                        param_name: str,
                        required_params: Optional[Dict[str, Any]] = None
                ) -> Dict[str, Dict[str, Any]]:
        """
        Retrieves metadata for the specified parameter.

        Args:
            param_name: A single parameter name (string)
                        (e.g., "GAIN", ["TPEA", "MCAC", "VOLU"]). Case-insensitive.
            required_params: Some parameters require additional parameters to be set, such as
                            "MCSL" and "MCSH" which depend on the current MCAC value.
                            Use this argument to specify any required parameter values.
                            If not provided, the required parameters will be fetched from the device.

        Returns:
            A dictionary containing metadata for the requested parameter:
            {
                "type": str, # Python type name ('str', 'int', 'float', 'bool', 'tuple')
                "doc": str,  # Description
                "range": tuple | list | str | None, # Min/Max, list, or descriptive string
                "allowed_values": list[str] | None, # For string parameters
                "supported": bool, # Is the parameter supported by this device model?
                "error": str | None # Error message if info retrieval failed
            }
        """
        return self.get_parameters_info([param_name], required_params)[param_name]

    def get_unsupported_devices_per_parameter(self) -> List[str]:
        """
        Returns a dict of parameter names and the devices that do not support them.
        If some parameter is not present in the dictionary, it is assumed to be supported by all devices.

        Returns:
            A dictionary where keys are parameter names and values are lists of device IDs
            that do not support the parameter.
        """
        return {
            'AINP': ['MCA8000D'],
            'AU34': ['DP5', 'DP5X', 'MCA8000D', 'Mini-X2'],
            'BLRD': ['MCA8000D'],
            'BLRM': ['MCA8000D'],
            'BLRU': ['MCA8000D'],
            'BOOT': ['PX5', 'DP5G', 'MCA8000D', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'CON1': ['DP5', 'DP5X', 'MCA8000D', 'Mini-X2'],
            'CON2': ['DP5', 'DP5X', 'MCA8000D', 'Mini-X2'],
            'CLCK': ['MCA8000D', 'Mini-X2'],
            'CLKL': ['MCA8000D', 'Mini-X2'],
            'CUSP': ['MCA8000D', 'Mini-X2'],
            'DACF': ['MCA8000D', 'Mini-X2'],
            'DACO': ['MCA8000D', 'Mini-X2'],
            'GAIF': ['MCA8000D', 'Mini-X2'],
            'GATE': ['PX5', 'DP5G', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'HVSE': ['MCA8000D'],
            'INOF': ['DP5G', 'MCA8000D', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'INOG': ['DP5', 'DP5X', 'DP5G', 'MCA8000D', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'PAPZ': ['DP5G', 'MCA8000D', 'DP5X', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'PDMD': ['DP5X', 'Mini-X2'],
            'PREL': ['DP5', 'PX5', 'DP5X', 'DP5G', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'PURE': ['Mini-X2'],
            'PURS': ['PX5', 'DP5G', 'MCA8000D', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'RESL': ['MCA8000D', 'Mini-X2'],
            'RTDD': ['MCA8000D', 'Mini-X2'],
            'RTDE': ['MCA8000D', 'Mini-X2'],
            'RTDS': ['MCA8000D', 'Mini-X2'],
            'RTDT': ['MCA8000D', 'Mini-X2'],
            'RTDW': ['MCA8000D', 'Mini-X2'],
            'SCTC': ['DP5', 'PX5', 'DP5X', 'MCA8000D', 'Mini-X2'],
            'SYNC': ['MCA8000D', 'Mini-X2'],
            'TECS': ['DP5G', 'MCA8000D', 'DP5X', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'TFLA': ['MCA8000D', 'Mini-X2'],
            'THFA': ['MCA8000D', 'Mini-X2'],
            'TPFA': ['MCA8000D', 'Mini-X2'],
            'TPMO': ['MCA8000D', 'DP5X', 'Mini-X2'],
            'VOLU': ['DP5', 'DP5G', 'MCA8000D', 'DP5X', 'Mini-X2', 'TB-5', 'Gamma-Rad5'],
            'AUO2=STREAM': ['DP5X']
        }
    
    def parameter_is_supported(self, parameter: str, device_id: str = None) -> bool:
        """
        Checks if a given parameter is supported by the device.

        Args:
            parameter: The parameter name (string) to check.
            device_id: The device ID string (e.g., "PX5", "DP5G"). If None, uses the
                       current device ID.
            verbose: If True, logs a warning if the parameter is not supported.
                       If False, suppresses the warning.
        Returns:
            True if the parameter is supported, False otherwise.
        """
        if device_id is None:
            device_id = self.get_model()
        if device_id == "Unknown":
            self.logger.warning("[Amptek MCA] Device ID is unknown, cannot check parameter support.")
            return False
        unsupported_devices = self.get_unsupported_devices_per_parameter()
        if parameter in unsupported_devices:
            if device_id in unsupported_devices[parameter]:
                # Only log if the parameter is not supported by the device
                self.logger.debug(f"[Amptek MCA] Parameter '{parameter}' is NOT supported by device '{device_id}'.")
                return False
            else:
                return True
        else:
            return True


    def set_HVSE(self, target_voltage: Union[float, int, str], step: float = 50.0, delay_sec: float = 0.5, save_to_flash: bool = False) -> None:
        """
        Sets the High Voltage supply, ramping the voltage in steps if necessary.

        It reads the current HV setting, calculates intermediate steps, and sends
        HVSE commands sequentially with a delay between them. Intermediate steps
        are sent without saving to flash, only the final target voltage command
        triggers a save.

        Args:
            target_voltage: The desired final voltage (float or int) or "OFF" (string).
            step: The approximate voltage step size for ramping (default: 50.0 V).
                  Must be positive.
            delay_sec: The delay in seconds between sending each voltage step command
                       (default: 0.5 s).
            save_to_flash: If True, the final target voltage will be saved to flash.
                          If False, it will not be saved (default: False).

        Raises:
            AmptekMCAError: If connection or communication fails, device doesn't support HVSE,
                            target voltage polarity is incompatible, or readback fails.
            AmptekMCAAckError: If the device returns an error ACK during command transmission.
            ValueError: If target_voltage format is invalid or step is non-positive.
        """
        self.logger.info(f"[Amptek MCA] Setting HVSE to '{target_voltage}' with ramp (step={step}V, delay={delay_sec}s)...")

        if not isinstance(target_voltage, (float, int, str)):
             raise ValueError("target_voltage must be a number or the string 'OFF'")
        if isinstance(target_voltage, str) and target_voltage.upper() != 'OFF':
             raise ValueError("If target_voltage is a string, it must be 'OFF'")
        if step <= 0:
             raise ValueError("Step must be positive.")

        final_command_dict = {}
        is_turning_off = False
        target_v_numeric = 0.0 # Default target if turning off

        if isinstance(target_voltage, str): # Must be "OFF"
            is_turning_off = True
            final_command_dict = {'HVSE': 'OFF'}
            # Ramp down to 0 before turning off
            target_v_numeric = 0.0
        else:
            target_v_numeric = float(target_voltage)
            # Final command sets the precise target voltage
            final_command_dict = {'HVSE': int(round(target_v_numeric))}


        # --- Get Current Status for Polarity and Current HV ---
        try:
            current_status = self.get_last_status() # Get status dictionary
            # Check device support first (simplistic check, assumes HVSE support if not DP5G/MCA)
            device_id = current_status.get('device_id', 'Unknown')
            if device_id in ["DP5G", "MCA8000D"]:
                 raise AmptekMCAError(f"HVSE command is not supported by device {device_id}")

            # Check polarity compatibility
            is_positive_capable = current_status.get('status_flags', {}).get('hv_polarity_positive')
            if is_positive_capable is None and device_id not in ["Unknown"]:
                 self.logger.warning(f"[Amptek MCA] Could not determine HV polarity capability for {device_id}. Proceeding with caution.")
            elif target_v_numeric > 0 and is_positive_capable is False:
                 raise AmptekMCAError(f"Device {device_id} is configured for negative HV polarity, cannot set positive target {target_v_numeric}V.")
            elif target_v_numeric < 0 and is_positive_capable is True:
                 raise AmptekMCAError(f"Device {device_id} is configured for positive HV polarity, cannot set negative target {target_v_numeric}V.")

            # Get current HV value from configuration readback
            current_config = self.read_configuration(['HVSE'])
            current_hv_str = current_config.get('HVSE', 'OFF') # Default to OFF if not found
            current_v = 0.0
            if current_hv_str.upper() != 'OFF':
                try:
                    current_v = float(current_hv_str)
                except ValueError:
                    self.logger.warning(f"[Amptek MCA] Could not parse current HV value '{current_hv_str}', assuming 0V.")
                    current_v = 0.0

        except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
             self.logger.error(f"[Amptek MCA] Failed to get initial status or current HV: {e}")
             raise AmptekMCAError(f"Failed to get initial status/HV before ramping: {e}")

        self.logger.info(f"[Amptek MCA] Current HV: {current_v:.1f}V, Target HV: {target_v_numeric:.1f}V{' (then OFF)' if is_turning_off else ''}")

        # --- Perform Ramping ---
        ramp_steps = []
        # Use math.isclose for floating point comparison
        if not math.isclose(current_v, target_v_numeric):
            # Determine direction and generate steps
            actual_step = step if target_v_numeric > current_v else -step
            next_v = current_v + actual_step

            if actual_step > 0: # Ramping up
                 while next_v < target_v_numeric:
                     ramp_steps.append(int(round(next_v)))
                     next_v += actual_step
            else: # Ramping down
                 while next_v > target_v_numeric:
                     ramp_steps.append(int(round(next_v)))
                     next_v += actual_step
            # Ensure the final numeric target is in the list if not already hit exactly
            if not math.isclose(ramp_steps[-1] if ramp_steps else current_v, target_v_numeric):
                 ramp_steps.append(int(round(target_v_numeric)))

        # Send intermediate steps without saving
        if ramp_steps:
             self.logger.info(f"[Amptek MCA] Ramping HV via {len(ramp_steps)} steps...")
             for i, step_v in enumerate(ramp_steps):
                 # Send all steps except the very last one without saving
                 is_final_numeric_step = (i == len(ramp_steps) - 1)
                 # Save only if it's the final numeric step AND we are not turning off
                 # afterwards and if save_to_flash is True
                 should_save_this_step = is_final_numeric_step and save_to_flash

                 step_config = {'HVSE': step_v}
                 self.logger.debug(f"[Amptek MCA] Sending ramp step {i+1}/{len(ramp_steps)}: HVSE={step_v} (Save={should_save_this_step})")
                 try:
                      # Use internal method with save_to_flash control
                      self.send_configuration(step_config, save_to_flash=should_save_this_step)
                      time.sleep(delay_sec)
                 except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
                      self.logger.error(f"[Amptek MCA] Error during HV ramp at step HVSE={step_v}: {e}")
                      raise AmptekMCAError(f"Error during HV ramp at step HVSE={step_v}: {e}") # Abort ramp on error

        # Send the final "OFF" command if requested, saving state
        if is_turning_off:
             self.logger.info("[Amptek MCA] Sending final HVSE=OFF command...")
             try:
                  self.send_configuration(final_command_dict, save_to_flash=True)
             except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
                  self.logger.error(f"[Amptek MCA] Error sending final HVSE=OFF command: {e}")
                  raise AmptekMCAError(f"Error sending final HVSE=OFF command: {e}")
        elif not ramp_steps and not math.isclose(current_v, target_v_numeric):
             # If no ramp steps were needed but target isn't current, send final target now
             self.logger.info(f"[Amptek MCA] Setting final HVSE target: {final_command_dict}")
             try:
                  self.send_configuration(final_command_dict, save_to_flash=True)
             except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
                  self.logger.error(f"[Amptek MCA] Error setting final HVSE target: {e}")
                  raise AmptekMCAError(f"Error setting final HVSE target: {e}")
        elif not is_turning_off and ramp_steps:
             self.logger.info(f"[Amptek MCA] HV ramp completed. Final target HVSE={final_command_dict['HVSE']} set and saved.")
        else:
             self.logger.info("[Amptek MCA] HV already at target voltage.")

    def get_available_default_configurations_with_content(self) -> Dict[str, Dict[str, OrderedDictType[str, str]]]:
        """
        Scans the 'default' directory in the library path for available default configuration files.

        These files come from the original Amptek SDK (https://www.amptek.com/software/dp5-digital-pulse-processor-software),
        particularly from the folder: DP5 Microsoft SDK > VC++ > vcDP5 > Release > DET_CFG

        Structure expected:
        - <library_directory>/
          - default/
            - DP5/
              - config1.txt
              - config2.txt
            - PX5/
              - standard.txt
            - ... (other device type folders)

        Each .txt file should contain configuration parameters in the format
        "KEY1=VALUE1;KEY2=VALUE2;...". Lines starting with '[' and ending
        with ']' (section headers) are ignored.

        Returns:
            A nested dictionary where the first key is the device type (subfolder name),
            the second key is the configuration name (filename without .txt),
            and the value is an OrderedDict containing the parsed key-value pairs
            from the configuration file.
            Returns an empty dictionary if the 'default' folder is not found or
            if no valid configurations are found.

            Example:
            {
                "DP5": {
                    "config1": OrderedDict([('RESC', 'Y'), ('TPEA', '2.400'), ...]),
                    "config2": OrderedDict([...])
                },
                "PX5": {
                    "standard": OrderedDict([...])
                }
            }
        """
        self.logger.info("[Amptek MCA] Searching for default configurations...")
        try:
            script_dir = Path(__file__).parent
        except NameError:
             # Fallback to current working directory if __file__ is not defined
             script_dir = Path.cwd()
             self.logger.warning(f"[Amptek MCA] __file__ not defined, using current working directory {script_dir} as base.")

        default_dir = script_dir / 'default'

        available_configs: Dict[str, Dict[str, OrderedDictType[str, str]]] = {}

        if not default_dir.is_dir():
            self.logger.warning(f"[Amptek MCA] 'default' directory not found at {default_dir}")
            return available_configs

        # Iterate through subdirectories in the 'default' folder (Device Types)
        for device_dir in default_dir.iterdir():
            if device_dir.is_dir():
                device_type = device_dir.name # e.g., "DP5", "PX5"
                self.logger.debug(f"[Amptek MCA] Found device type folder: {device_type}")
                device_configs: Dict[str, OrderedDictType[str, str]] = {}

                # Iterate through .txt files in the device type subfolder
                for config_file in device_dir.glob('*.txt'):
                    if config_file.is_file():
                        config_name = config_file.stem # Filename without extension
                        parsed_config = OrderedDict()
                        try:
                            with config_file.open('r', encoding='utf-8') as f:
                                for line_num, line in enumerate(f, 1):
                                    line = line.strip()
                                    # Skip empty lines or section headers like [Header]
                                    if not line or (line.startswith('[') and line.endswith(']')):
                                        continue

                                    # Process potentially multiple commands on one line
                                    parts = line.split(';')
                                    for part in parts:
                                        part = part.strip()
                                        if not part: # Skip empty parts
                                            continue

                                        # Split only on the first '=', allow '=' in value
                                        key_value = part.split('=', 1)
                                        if len(key_value) == 2:
                                            key = key_value[0].strip().upper() # Use uppercase keys
                                            value = key_value[1].strip() # Get value
                                            if key and value: # Ensure key and value are not empty
                                                parsed_config[key] = value
                                            elif not key:
                                                self.logger.debug(f"[Amptek MCA] Skipping empty key in part '{part}' in file {config_file.name}, line {line_num}")
                                            elif not value:
                                                self.logger.debug(f"[Amptek MCA] Skipping empty value in part '{part}' in file {config_file.name}, line {line_num}")
                                        else:
                                            # Handle parts without '=' if necessary
                                            self.logger.warning(f"[Amptek MCA] Skipping malformed part '{part}' (no '=') in file {config_file.name}, line {line_num}")

                            if parsed_config:
                                # Remove unsupported parameters
                                self.logger.debug(f"[Amptek MCA] Removing unsupported parameters from config '{config_name}' for device '{device_type}'...")
                                parsed_config = {k: v for k, v in parsed_config.items() if self.parameter_is_supported(k, device_type)}
                                # Fix RTDS (0 was an invalid value, according to the programmer's guide the correct minimum value is 2)
                                if 'RTDS' in parsed_config and parsed_config['RTDS'] == '0':
                                    self.logger.debug(f"[Amptek MCA] Fixing RTDS value in config '{config_name}' for device '{device_type}' from 0 to 2.")
                                    parsed_config['RTDS'] = '2'
                                # Save
                                device_configs[config_name] = parsed_config
                            else:
                                self.logger.warning(f"[Amptek MCA] No valid configuration pairs found in {config_file.name}")

                        except IOError as e:
                             self.logger.error(f"[Amptek MCA] Error reading config file {config_file.name}: {e}")
                        except Exception as e:
                             self.logger.error(f"[Amptek MCA] Error parsing config file {config_file.name}: {e}")

                if device_configs:
                    available_configs[device_type] = device_configs

        if not available_configs:
             self.logger.info("[Amptek MCA] No default configurations found in 'default' directory structure.")
        else:
             self.logger.info(f"[Amptek MCA] Found default configurations for devices: {list(available_configs.keys())}")

        return available_configs

    def get_available_default_configurations(self) -> Dict[str, List[str]]:
        """
        Scans the 'default' directory in the library path for available default configuration files.

        These files come from the original Amptek SDK (https://www.amptek.com/software/dp5-digital-pulse-processor-software),
        particularly from the folder: DP5 Microsoft SDK > VC++ > vcDP5 > Release > DET_CFG

        This method calls get_available_default_configurations_with_content()
        internally to read all configurations and then extracts only the names.

        Returns:
            A dictionary where keys are the device type names (subfolder names)
            and values are lists of available configuration names (filenames
            without .txt extension).
            Returns an empty dictionary if the 'default' folder is not found or
            if no valid configurations are found by the underlying method.

            Example:
            {
                "DP5": ["config1", "config2"],
                "PX5": ["standard"]
            }
        """
        simplified_configs: Dict[str, List[str]] = {}
        try:
            # Call the method that reads the content
            all_configs_with_content = self.get_available_default_configurations_with_content()

            # Simplify the result: extract only keys (device types and config names)
            for device_type, device_configs in all_configs_with_content.items():
                config_names = sorted(list(device_configs.keys())) # Get config names and sort
                if config_names:
                    simplified_configs[device_type] = config_names

        except Exception as e:
            # Catch errors from the underlying method call
            self.logger.error(f"[Amptek MCA] Error retrieving configurations with content: {e}")
            # Return empty dict in case of error during retrieval/parsing
            return {}

        return simplified_configs

    def get_default_configuration(self, device_type: str, config_name: str) -> Optional[OrderedDictType[str, Any]]:
        """
        Retrieves a specific default configuration dictionary for a given device type
        and configuration name by reading it from the 'default' subdirectory.

        This method calls get_available_default_configurations_with_content() internally to load
        all available default configurations first.

        Args:
            device_type: The name of the device type (e.g., "DP5", "PX5"),
                         corresponding to a subfolder name in the 'default' directory.
                         Case-sensitive (must match folder name).
            config_name: The name of the configuration (e.g., "standard", "config1"),
                         corresponding to a .txt filename (without extension) within
                         the device type subfolder. Case-sensitive (must match filename).

        Returns:
            An OrderedDict containing the key-value pairs for the requested configuration,
            with values potentially converted to int/float where possible.
            Returns None if the device type folder or the configuration file is not found,
            or if the file could not be parsed correctly by the underlying method.
        """
        self.logger.info(f"[Amptek MCA] Getting default configuration '{config_name}' for device '{device_type}'...")

        # Get all available default configurations
        all_configs = self.get_available_default_configurations_with_content()

        # Look up the specific device type
        device_configs = all_configs.get(device_type)
        if device_configs is None:
            self.logger.warning(f"[Amptek MCA] No default configurations found for device type '{device_type}'.")
            return None

        # Look up the specific configuration name
        specific_config = device_configs.get(config_name)
        if specific_config is None:
            self.logger.warning(f"[Amptek MCA] Default configuration '{config_name}' not found for device type '{device_type}'.")
            return None

        self.logger.info(f"[Amptek MCA] Default configuration '{config_name}' for '{device_type}' retrieved.")
        return specific_config

    def apply_default_configuration(self, device_type: str, config_name: str, save_to_flash = False) -> None:
        """
        Applies a specific default configuration to the device.

        Retrieves the specified default configuration, sends all parameters
        except for HVSE, and then calls set_HVSE to apply the high voltage
        with ramping. The main configuration part is always saved to flash.

        Args:
            device_type: The name of the device type (e.g., "DP5", "PX5").
            config_name: The name of the configuration file (e.g., "standard").
            save_to_flash: If True, the configuration will be saved to flash.
                            If False, it will not be saved (default: False).

        Raises:
            AmptekMCAError: If connection or communication fails, if the default
                            configuration cannot be found/retrieved, or during
                            command execution.
            AmptekMCAAckError: If the device returns an error ACK.
            ValueError: If configuration values are invalid.
        """
        self.logger.info(f"[Amptek MCA] Applying default configuration '{config_name}' for device '{device_type}'...")

        # 1. Get the target default configuration dictionary
        config_to_apply = self.get_default_configuration(device_type, config_name)

        if config_to_apply is None:
            # Error already logged by get_default_configuration
            raise AmptekMCAError(f"Could not retrieve default configuration '{config_name}' for device '{device_type}'.")

        # 2. Separate HVSE command if present
        # Use pop with default None. Assumes keys are uppercase from get_default_configuration
        target_hv_value = config_to_apply.pop('HVSE', None)

        # 3. Send the rest of the configuration
        if config_to_apply: # Check if there are other parameters left
            self.logger.info(f"[Amptek MCA] Sending main configuration parameters ({len(config_to_apply)} items)...")
            try:
                self.send_configuration(config_to_apply, save_to_flash=save_to_flash) # Do not save in flash, avoid memory degradation
            except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
                self.logger.error(f"[Amptek MCA] Error sending main configuration part: {e}")
                raise # Re-raise the exception
        else:
            self.logger.info("[Amptek MCA] No main configuration parameters to send (only HVSE was present or config empty).")

        # 4. Apply HVSE using the ramping method (if it was present)
        if target_hv_value is not None:
            self.logger.info(f"[Amptek MCA] Applying HVSE setting separately: {target_hv_value}")
            try:
                # Convert potential numeric strings back if needed, though get_default should type them
                hv_to_set: Union[float, int, str]
                if isinstance(target_hv_value, str) and target_hv_value.upper() == 'OFF':
                    hv_to_set = 'OFF'
                else:
                    # Attempt conversion to float, handle potential errors if value wasn't typed correctly
                    try:
                        hv_to_set = float(target_hv_value)
                    except (ValueError, TypeError):
                         raise ValueError(f"Invalid HVSE value found in configuration: '{target_hv_value}'")

                # Call the ramping method
                self.set_HVSE(hv_to_set, save_to_flash=save_to_flash) # Uses default step/delay
            except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
                 self.logger.error(f"[Amptek MCA] Error applying HVSE setting '{target_hv_value}': {e}")
                 raise # Re-raise the exception
        else:
             self.logger.info("[Amptek MCA] No HVSE parameter found in the configuration to apply separately.")

        self.logger.info(f"[Amptek MCA] Default configuration '{config_name}' applied successfully for '{device_type}'.")

    def wait_until_mca_is_closed(self, time_between_checks: float = 1) -> None:
        """
        Waits until the MCA is closed.

        Checks relevant preset configurations (PRET, PRER, PREC, PREL) first.
        If no preset condition is active that would stop the MCA, it logs a
        warning and returns immediately to prevent an infinite wait.
        User can interrupt the wait with Ctrl+C.

        Args:
            time_between_checks: The time interval in seconds between status checks.
                               Defaults to 1 seconds.

        Raises:
            AmptekMCAError: If connection or communication fails during status checks
                            or configuration readback.
            AmptekMCAAckError: If the device returns an error ACK during checks.
            ValueError: If time_between_checks is not positive or zero.
        """
        if time_between_checks <= 0:
            raise ValueError("time_between_checks must be positive and non-zero.")

        self.logger.info(f"[Amptek MCA] Waiting for MCA to close (polling every {time_between_checks}s)...")

        # First, check if MCA is already closed
        try:
            initial_status = self.get_status(silent=True)
            if not initial_status['status_flags']['mca_enabled']:
                self.logger.info("[Amptek MCA] MCA is already closed.")
                return
        except (AmptekMCAError, AmptekMCAAckError) as e:
            self.logger.exception("[Amptek MCA] Failed to get initial MCA status")
            raise # Re-raise the error

        # Check preset conditions
        device_model = self.get_model()
        presets_to_check = ['PRET', 'PRER', 'PREC']
        if device_model == 'MCA8000D':
            presets_to_check.append('PREL')

        try:
            preset_config = self.read_configuration(presets_to_check)
        except (AmptekMCAError, AmptekMCAAckError, ValueError) as e:
            self.logger.exception("[Amptek MCA] Failed to read preset configuration")
            raise AmptekMCAError("Failed to read preset configuration before waiting")

        any_preset_active = False
        for preset_cmd in presets_to_check:
            value_str = preset_config.get(preset_cmd, 'OFF') # Default to OFF if not found
            if value_str.upper() != 'OFF':
                try:
                    # Try converting to float, check if non-zero
                    if float(value_str) != 0.0:
                        any_preset_active = True
                        break # Found an active preset, no need to check others
                except ValueError:
                    # If it's not 'OFF' and not convertible to float (or is non-zero int), consider it active
                    # This should not happen
                    self.logger.warning(f"[Amptek MCA] Preset {preset_cmd} has non-numeric value '{value_str}', assuming it's active.")
                    any_preset_active = True
                    break

        # If MCA is currently enabled but no presets are active, warn and return
        if not any_preset_active:
            self.logger.warning("[Amptek MCA] MCA is enabled, but no active preset condition (PRET/PRER/PREC/PREL) found.")
            self.logger.warning("[Amptek MCA] wait_until_mca_is_closed() will return immediately to avoid potential infinite loop.")
            return

        # Start polling loop only if at least one preset is active
        self.logger.debug("[Amptek MCA] At least one preset condition is active. Starting polling loop...")
        while True:
            try:
                current_status = self.get_status(silent=True)
                if not current_status['status_flags']['mca_enabled']:
                    self.logger.info("[Amptek MCA] MCA is now closed.")
                    break # Exit the loop

                self.logger.debug("[Amptek MCA] MCA still enabled, waiting...")
                time.sleep(time_between_checks)

            except (AmptekMCAError, AmptekMCAAckError) as e:
                self.logger.exception("[Amptek MCA] Error polling MCA status during wait")
                raise # Re-raise the error, interrupting the wait

            except KeyboardInterrupt:
                self.logger.warning("[Amptek MCA] Wait interrupted by user.")
                raise # Allow interruption to propagate

    # --- Static Methods ---
    @staticmethod
    def install_libusb() -> None:
        """
        Install the libusb backend for pyusb.
        """
        UsbUtils.install_libusb() # Assumes this function exists and works

    @staticmethod
    def add_udev_rule() -> None:
        """
        Add udev rules for the Amptek MCA device.
        """
        UsbUtils.add_udev_rule(AmptekMCA.VENDOR_ID_STR, AmptekMCA.PRODUCT_ID_STR) # Assumes this exists


# Example Usage
if __name__ == "__main__":
    amptek_mca = AmptekMCA()
    print(amptek_mca.get_default_configuration("PX5", "CdTe Default PX5"))