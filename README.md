# InfluxDB Helpers

> All scripts require python3

## influxdb_cleaner
This script is made specifically to work with Home Assisant InfluxDB database. It's goal is to scan, count, and remove unwanted measurements.

The script takes the following parameters:

| option       | required | default        | description                                                                                  |
|--------------|----------|----------------|----------------------------------------------------------------------------------------------|
| --host, -h   | yes      | none           | InfluxDB hostname                                                                            |
| --port, -p   | no       | 8086           | InfluxDB port                                                                                |
| --user, -U   | no       | none           | InfluxDB username if authentication is enabled                                               |
| --pass, -P   | no       | none           | InfluxDB password if authentication is enabled                                               |
| --db         | no       | home_assistant | InfluxDB target database                                                                     |
| --count      | no       | none           | Enable to count number of items in a measurements containing a value or a state.             |
| --action, -a | no       | dryrun         | dryrun or remove. Remove requires an additional input file containing measurements to remove |
| --sleep, -s  | no       | 120            | Seconds to wait between remove operations. See warning below                                 |
| --abandoned  | no       | 180            | Identify measurement as abandoned if last entry is older than specified number of days       |
| --file, -f   | no       | none           | File containing measurements to remove. Each line of a file should be a measurement          |

> Warnings/Info
> * --count - using this option will generate a csv output file in the same directory where the script is located
> * --count - Higher number wins: ie, if there are 100 records with a state and 200 records with a value, value will be reported. There is hard-coded 5 second delay betrween count operation to allow InfluxDB to fetch the data.
> * --sleep - This is needed to ensure InfluxDB has enough time to compact shards after each measurement is removed. The value required will depend on many factors, however general rule should be to use a higher value to allow InfluxDB ample time to compact all required shards before a new remove operation is requested