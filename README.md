# [InfluxDB](http://influxdb.org/) Output for [Mozilla Heka](http://hekad.readthedocs.org/)

Example config:

```TOML
[InfluxDBOutput]
message_matcher = 'TRUE'

server = 'http://localhost:8086'
database = 'db'
username = 'user'
password = 'pass'

# InfluxDB connection timeout, in seconds
response_header_timeout = 30

# The default if series in the field_map is missing
series = 'history'

# Use the Heka message Timestamp as the InfluxDB time; on by default
use_heka_timestamp = true

# Flush points to InfluxDB at least this often, in milliseconds
flush_interval = 1000

# Buffer no more than this many points before flushing to InfluxDB
flush_count = 10

[InfluxDBOutput.field_map]
# Map InfluxDB field names to Heka message field names
payload = 'Payload'
custom_field = 'CustomField'

# Special case for dynamic series name - falls back to series above
series = 'Type'
```
