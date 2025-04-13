# Positioner

This library allows controlling a positioner via g-code commands.

It is designed for controllers that may not provide reliable command acknowledgments. To accommodate this, the library utilizes a user-configurable timeout to allow sufficient time for command execution.

# Getting Started

1.  Install the `cfis_interfaces` package:
    ```bash
    pip install git+https://github.com/CFIS-UFRO/cfis-interfaces.git
    ```
2.  Import the library in your Python script:
    ```python
    from cfis_interfaces import Positioner
    ```
3.  Instantiate the `Positioner` class, specifying the serial port and optionally the baudrate and default wait time:
    ```python
    positioner = Positioner(port='/dev/ttyUSB0', baudrate=115200, default_wait_time=5.0) 
    ```
    * `port`: The serial port name (e.g., '/dev/ttyUSB0' on Linux, 'COM3' on Windows).
    * `baudrate`: Communication speed (default is 115200).
    * `default_wait_time`: Default time in seconds to wait after move commands (default is 5.0 seconds).
    * `on_data_callback`: An optional function to call when data is received from the controller.
4.  Use the `connect` method to establish a connection with the positioner, and the `disconnect` method to close it when finished:
    ```python
    if positioner.connect():
        print("Connected successfully!")
        # ... control the positioner ...
        positioner.disconnect()
        print("Disconnected.")
    else:
        print("Failed to connect.")
    ```
5.  Use the following methods to control the positioner after connecting:

    * `move_absolute(x, y, z, speed=None, wait_time=None)`: Moves the positioner to the specified absolute coordinates (X, Y, Z) in millimeters.
        * `x`, `y`, `z`: Target absolute coordinates (float).
        * `speed`: Optional movement speed in mm/minute (float). If `None`, uses rapid movement (G0).
        * `wait_time`: Optional specific time in seconds to wait after this command, overriding the default (float).
    * `move_relative(dx, dy, dz, speed=None, wait_time=None)`: Moves the positioner by the specified relative distances (dX, dY, dZ) in millimeters from its current position.
        * `dx`, `dy`, `dz`: Relative distances to move (float).
        * `speed`: Optional movement speed in mm/minute (float). If `None`, uses rapid movement (G0).
        * `wait_time`: Optional specific time in seconds to wait after this command, overriding the default (float).
    * `go_home(wait_time=None)`: Executes the machine's homing sequence (G28).
        * `wait_time`: Optional time in seconds to wait for homing to complete, overriding the default homing wait time (float).
    * `set_home(x=0.0, y=0.0, z=0.0)`: Defines the current physical position as the specified coordinates (X, Y, Z) using G92, without moving the machine. This effectively sets the origin of the coordinate system.
        * `x`, `y`, `z`: Coordinates to assign to the current position (float).
    * `send_command(command)`: Sends a raw G-code `command` string directly to the controller. This is useful for custom commands not covered by other methods. Returns `True` if the command was sent, `False` otherwise.
        * `command`: The G-code string to send (str). A newline character `\n` is added automatically.

**Important Note:** Since this library relies on time delays rather than acknowledgments, the `wait_time` parameters (either default or specific) are crucial for ensuring one command finishes before the next one begins. Adjust these based on your specific hardware's movement speeds and requirements. 