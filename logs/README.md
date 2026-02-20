## Empty Demo Doc

This demo validates end-to-end IoT data capture using ESP32-S3.

The device scans nearby Bluetooth devices, captures signal strength,
and associates each detection with live GPS coordinates.

A Python application reads this data in real time, hashes device MAC IDs
for privacy, detects unique vs repeated devices, and displays results
on a local dashboard.

Once approved, the same architecture can forward data securely
to client systems without modification.
