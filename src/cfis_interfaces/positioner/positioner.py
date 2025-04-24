# Standard imports
import time
import threading
from typing import Callable, Optional
import logging
# Third-party imports
import serial
# CFIS libraries
from cfis_utils import LoggerUtils, TimeUtils


# Default time in seconds to wait after sending a move command
DEFAULT_WAIT_TIME_S = 5.0
# Default time in seconds to wait after a homing command
DEFAULT_HOMING_WAIT_TIME_S = 30.0
# Short wait time after setting position or sending some commands
SHORT_WAIT = 0.2
# Time to wait for controller initialization after connection
CONTROLLER_INIT_WAIT_S = 2.0
# Timeout for serial read operations
SERIAL_READ_TIMEOUT_S = 0.1
# Short sleep duration to avoid busy-waiting in reader thread
READER_THREAD_SLEEP_S = 0.1
# Timeout for joining the reader thread
READER_THREAD_JOIN_TIMEOUT_S = 0.5

class Positioner:
    """
    Handles communication and control of a G-code based positioning system.
    Assumes the controller does NOT provide reliable acknowledgments ('ok').
    Uses fixed time delays to approximate move completion.
    Includes a background thread to read any output from the controller.
    """
    def __init__(self,
                port: str,
                baudrate: int = 9600,
                default_wait_time: float = DEFAULT_WAIT_TIME_S,
                on_data_callback: Optional[Callable[[str], None]] = None,
                logger: Optional[logging.Logger] = None,
                logger_name: str = "Positioner",
                logger_level: int = logging.INFO
            ):
        """
        Initializes the connection parameters for the G-code controller.

        Args:
            port (str): The serial port name (e.g., 'COM3' or '/dev/ttyACM0').
            baudrate (int): The communication speed (bits per second).
            default_wait_time (float): Default time in seconds to wait after sending
                                       a move command, assuming it completed.
                                       Defaults to DEFAULT_WAIT_TIME_S.
            on_data_callback (Optional[Callable[[str], None]]): An optional function
                                       to call when data is received from the serial port.
                                       It should accept a single string argument. Defaults to None.
            logger (Optional[logging.Logger]): An optional logger instance. If None,
                                       a new logger will be created with the provided name and level.
            logger_name (str): The name of the new logger. Defaults to "Positioner".
            logger_level (int): The logging level for the new logger. Defaults to logging.INFO.
        """
        self.port = port
        self.baudrate = baudrate
        self.default_wait_time = default_wait_time
        self.on_data_callback = on_data_callback
        self.connection = None
        self.is_connected = False
        self._reader_thread = None
        self._reading_active = False
        self.logger = logger if logger else LoggerUtils.get_logger(logger_name, level=logger_level)
        self.logger.info(f"[POSITIONER] Initializing Positioner on port {self.port} at {self.baudrate} baud.")
        self.logger.debug(f"[POSITIONER] Default wait time set to: {TimeUtils.format_time(self.default_wait_time)}")

    def _background_reader(self):
        """
        Internal method to run in a thread to read controller output.
        """
        self.logger.info("[POSITIONER] Background reader thread started.")
        try:
            while self._reading_active:
                if not (self.connection and self.connection.is_open):
                    self.logger.warning("[POSITIONER] Connection lost or closed. Stopping reader.")
                    break
                try:
                    if self.connection.in_waiting > 0:
                        line = self.connection.readline().decode('utf-8', errors='ignore').strip()
                        if line:
                            self.logger.debug(f"[POSITIONER] Received from controller: {line}")
                            if self.on_data_callback:
                                self.on_data_callback(line)
                    else:
                        time.sleep(READER_THREAD_SLEEP_S)
                except serial.SerialException as e:
                    self.logger.exception(f"[POSITIONER] Serial error in background reader: {e}. Stopping reader.")
                    self._reading_active = False # Signal thread to stop on serial error
                    self.is_connected = False # Assume connection is lost
                    break
                except Exception as e:
                    # Catch unexpected errors within the loop
                    self.logger.exception(f"[POSITIONER] Unexpected error in background reader loop: {e}")
                    time.sleep(READER_THREAD_SLEEP_S) # Wait briefly before retrying

        except Exception as e:
             # Catch errors occurring outside the main loop (e.g., during initial checks)
             self.logger.exception(f"[POSITIONER] Fatal error in background reader thread: {e}")
        finally:
            self._reading_active = False # Ensure flag is false on exit
            self.logger.info("[POSITIONER] Background reader thread finished.")


    def connect(self) -> bool:
        """
        Establishes the serial connection and starts the background reader.

        Returns:
            bool: True if connection is successful, False otherwise.

        Raises:
            PositionerError: If serial connection fails or initial command send fails.
        """
        if self.is_connected:
            self.logger.warning("[POSITIONER] Positioner already connected.")
            return True

        self.logger.info(f"[POSITIONER] Attempting to connect to {self.port}...")
        try:
            self.connection = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=SERIAL_READ_TIMEOUT_S 
            )
            # Wait for controller to initialize
            time.sleep(CONTROLLER_INIT_WAIT_S)
            self.connection.flushInput()
            self.connection.flushOutput()
            self.is_connected = True
            self.logger.info("[POSITIONER] Serial port opened successfully.")

            # Start background reader
            self._reading_active = True
            self._reader_thread = threading.Thread(target=self._background_reader, daemon=True)
            self._reader_thread.start()

            # Send initial configuration (units to mm)
            if not self.send_command("G21"):
                # If sending fails immediately, connection is likely problematic
                self.logger.error("Failed to send initial G21 command after connection.")
                self._cleanup_connection()
                return False
            
            # Small delay to allow command processing before potential next commands
            time.sleep(SHORT_WAIT)
            self.logger.info("[POSITIONER] Positioner connected and initialized (G21: units=mm).")
            return True

        except serial.SerialException as e:
            self.logger.exception(f"[POSITIONER] Serial connection error: {e}")
            self._cleanup_connection()
        except Exception as e:
            self.logger.exception(f"[POSITIONER] Unexpected error during connection: {e}")
            self._cleanup_connection()
        return False

    def _cleanup_connection(self) -> None:
        """Internal helper to close connection and stop reader thread."""
        self._reading_active = False
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=READER_THREAD_JOIN_TIMEOUT_S)
        self._reader_thread = None

        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
            except Exception as e:
                self.logger.exception(f"[POSITIONER] Error closing serial port during cleanup: {e}")
        self.connection = None
        self.is_connected = False


    def disconnect(self) -> None:
        """
        Stops the background reader thread and closes the serial connection.
        """
        self.logger.info("[POSITIONER] Disconnecting Positioner...")
        self._cleanup_connection()
        self.logger.info("[POSITIONER] Positioner disconnected.")


    def send_command(self, command: str) -> bool:
        """
        Sends a G-code command to the controller.

        Args:
            command (str): The G-code command to send (newline is added automatically).

        Returns:
            bool: True if the command was sent successfully, False otherwise.
        """
        if not self.is_connected or not self.connection:
            self.logger.error("[POSITIONER] Cannot send command: Positioner not connected.")
            return False

        clean_command = command.strip()
        self.logger.debug(f"[POSITIONER] Sending G-code: {clean_command}")

        try:
            # Ensure command ends with a newline and encode it
            command_bytes = (clean_command + '\n').encode('utf-8')
            self.connection.write(command_bytes)
            self.connection.flush() # Ensure data is sent
            return True

        except serial.SerialException as e:
            self.logger.error(f"[POSITIONER] Serial communication error sending '{clean_command}': {e}")
            # Assume connection is lost
            self._cleanup_connection()
            return False
        except Exception as e:
            self.logger.error(f"[POSITIONER] Unexpected error sending '{clean_command}': {e}")
            return False

    def _wait_approximate(self, duration: float):
        """
        Waits for a fixed duration as an approximation for command completion.

        Args:
            duration (float): Time to wait in seconds.
            reason (str): Description of why the wait is happening.
        """
        if duration <= 0:
            return
        self.logger.info(f"[POSITIONER] Waiting {TimeUtils.format_time(duration)}")
        time.sleep(duration)


    def _send_move_command(self, mode_command: str, move_command: str, wait_duration: float):
        """Internal helper to send mode, move command and wait."""
        if not self.is_connected or not self.connection:
             self.logger.error(f"[POSITIONER] Cannot move: Positioner not connected.")
             return False

        # Set positioning mode (Absolute G90 or Relative G91)
        if not self.send_command(mode_command):
             self.logger.error(f"[POSITIONER] Failed to send mode command ({mode_command}) before move.")
             return False

        # Send the actual move command
        if not self.send_command(move_command):
             return False # Error already logged by send_command

        # Wait the approximate time for the move to complete
        self._wait_approximate(wait_duration)

        return True # Commands sent, but completion is not guaranteed


    def move_absolute(self, x: float, y: float, z: float, speed: Optional[float] = None, wait_time: Optional[float] = None):
        """
        Moves to absolute coordinates (X, Y, Z) using G90.

        Args:
            x (float): Target absolute X coordinate (mm).
            y (float): Target absolute Y coordinate (mm).
            z (float): Target absolute Z coordinate (mm).
            speed (float, optional): Feed rate (mm/minute). G0 (rapid) if None.
            wait_time (float, optional): Override default wait time (seconds) if provided.

        Returns:
            bool: True if commands were sent, False on error. Completion not guaranteed.
        """
        command_prefix = f"G1 F{speed:.2f}" if speed else "G0"
        move_cmd = f"{command_prefix} X{x:.4f} Y{y:.4f} Z{z:.4f}"
        wait_time = wait_time if wait_time is not None else self.default_wait_time
        self.logger.info(f"[POSITIONER] Absolute movement to X={x} Y={y} Z={z} (mm) with speed {speed} (mm/min) using G90...")
        return self._send_move_command("G90", move_cmd, wait_time)


    def move_relative(self, dx: float, dy: float, dz: float, speed: float = None, wait_time: Optional[float] = None):
        """
        Moves by relative distances (dX, dY, dZ) using G91.

        Args:
            dx (float): Relative X distance (mm).
            dy (float): Relative Y distance (mm).
            dz (float): Relative Z distance (mm).
            speed (float, optional): Feed rate (mm/minute). G0 (rapid) if None.
            wait_time (float, optional): Override default wait time (seconds) if provided.

        Returns:
            bool: True if commands were sent, False on error. Completion not guaranteed.
        """
        command_prefix = f"G1 F{speed:.2f}" if speed else "G0"
        move_cmd = f"{command_prefix} X{dx:.4f} Y{dy:.4f} Z{dz:.4f}"
        wait_time = wait_time if wait_time is not None else self.default_wait_time
        self.logger.info(f"[POSITIONER] Relative movement to X={dx} Y={dy} Z={dz} (mm) with speed {speed} (mm/min) using G91...")
        return self._send_move_command("G91", move_cmd, wait_time)


    def set_home(self, x: float = 0.0, y: float = 0.0, z: float = 0.0) -> bool:
        """
        Sets the current position as the given coordinates using G92 (requires G90 first).

        Args:
            x (float): X coordinate to assign (mm). Default 0.
            y (float): Y coordinate to assign (mm). Default 0.
            z (float): Z coordinate to assign (mm). Default 0.

        Returns:
            bool: True if commands were sent successfully, False otherwise.
        """
        if not self.is_connected or not self.connection:
             self.logger.error("[POSITIONER] Cannot set home: Positioner not connected.")
             return False

        # G92 usually works in the current mode, but setting G90 explicitly is safer
        if not self.send_command("G90"):
             self.logger.error("[POSITIONER] Failed to send absolute mode command (G90) before set_home.")
             return False

        command = f"G92 X{x:.4f} Y{y:.4f} Z{z:.4f}"
        self.logger.info(f"[POSITIONER] Setting current position to X={x} Y={y} Z={z} (mm) using G92...")
        sent = self.send_command(command)
        self._wait_approximate(SHORT_WAIT, "G92 set position")
        return sent


    def go_home(self, wait_time: Optional[float] = None) -> bool:
        """
        Executes the machine's homing sequence (G28).

        Args:
            wait_time (float, optional): Time (seconds) to wait, overriding default if provided.

        Returns:
            bool: True if G28 command was sent, False otherwise. Completion not guaranteed.
        """
        if not self.is_connected or not self.connection:
             self.logger.error("[POSITIONER] Cannot go home: Positioner not connected.")
             return False

        self.logger.info("[POSITIONER] Starting homing sequence (G28)...")
        if not self.send_command("G28"):
            return False

        wait_time = wait_time if wait_time is not None else DEFAULT_HOMING_WAIT_TIME_S
        self._wait_approximate(wait_time)

        return True
    
if __name__ == "__main__":
    # Example usage
    positioner = Positioner(port="/dev/ttyUSB0")
