"""InfluxDB-CQ-Maker"""
import click
import influxdb
import logging
import requests
from influxdb import InfluxDBClient
from time import sleep

logging.basicConfig(level=logging.INFO)


class influx_scan:
    """Influx Scan class"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        pwd: str,
        database: str,
        count: bool,
        action: str,
        sleep: int,
        file: str,
    ):
        """Intialize class"""
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.db = database
        self.count = count
        self.action = action
        self.sleep = sleep
        self.file = file
        self.input_data = []

        try:
            self.client = InfluxDBClient(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.pwd,
                timeout=10,
            )
            self.all_dbs = self.client.get_list_database()
        except requests.exceptions.ConnectTimeout:
            logging.error("Connection to InfludDB timed out")
            exit()
        except influxdb.exceptions.InfluxDBClientError as error:
            logging.error(
                f"Unable to establish connection - {error.code}: {error.content}"
            )
            exit()

        if self.db in [value for elem in self.all_dbs for value in elem.values()]:
            logging.debug(f"Database exists: {self.db}")
            self.client.switch_database(self.db)
        else:
            logging.error(f"Database does not exist: {self.db}")
            logging.debug(
                f"Found DBs: {[val for elem in self.all_dbs for val in elem.values()]}"
            )
            exit()

        if self.action == "remove":
            self.input_data = self.load_file()

    def main(self):
        """Main"""
        measurement_count = 1
        records_total = 0
        records_count = 0
        remove_records_total = 0
        remove_error = 0

        for measurement in self.get_measurements():
            print(f'**** {measurement["name"]}')

            if self.count:
                measurement_total = self.get_measurement_total(measurement["name"])

                if measurement_total != -1:
                    if isinstance(measurement_total, int):
                        records_total += measurement_total
                        if measurement["name"] in self.input_data:
                            remove_records_total += measurement_total

                    print(measurement_total)
                    print(records_total)
                    print(remove_records_total)
                else:
                    print("Unable to get total for measurement")
                sleep(5)

            if self.action == "remove":
                if measurement["name"] in self.input_data:
                    print("Removing")
                    remove = self.remove_measurement(measurement["name"])
                    records_count += 1

                    if remove is False:
                        remove_error += 1

                    # if records_count % 5 == 0:
                    #    print(f"Pause: {(self.sleep * 1.5)} sec")
                    #    sleep((self.sleep * 1.5))
                    # else:
                    sleep(self.sleep)

            measurement_count += 1

        print(f"Measurements: {measurement_count}")
        print(f"Measurements Remove: {records_count}")

        if self.count:
            print(f"Measurement Records: {records_total}")
            print(f"Measurement Records Remove: {remove_records_total}")
        if self.action == "remove":
            print(f"Remove execution warnings: {remove_error}")
        self.client.close()

    def get_measurements(self) -> list:
        """Get available measurements"""
        try:
            _measurements = self.client.get_list_measurements()
            return _measurements
        except influxdb.exceptions.InfluxDBClientError as error:
            logging.error(
                f"Unable to retrieve measurements: {error.code}: {error.content}"
            )
            exit()

    def get_measurement_total(self, measurement) -> int:
        """Get total count of measurement records"""

        try:
            _query = f'SELECT count(value), count(state) FROM "{measurement}"'
            _count = self.client.query(_query)

            for item in _count.get_points():
                all_values = list(item.values())
                all_values.pop(0)
                integers = [x for x in all_values if isinstance(x, int)]

                return max(integers)
        except influxdb.exceptions.InfluxDBClientError as error:
            logging.error(
                f"Unable to retrieve measurements: {error.code}: {error.content}"
            )
            return -1

    def remove_measurement(self, measurement_name) -> bool:
        """Remove measurement"""

        try:
            self.client.drop_measurement(measurement_name)
        except influxdb.exceptions.InfluxDBClientError as error:
            print(f"Warning: {error.content}")
            return False
        except requests.exceptions.ConnectionError as error:
            print(f"Warning: {error}")
            return False

        return True

    def load_file(self) -> list:
        """Load measurements file"""

        _output = []
        try:
            with open(self.file) as _file:
                while _line := _file.readline().rstrip():
                    _output.append(_line)
        except Exception as e:
            print(e)
            exit()

        return _output


@click.command()
@click.option("--host", "-h", type=str, required=True, help="InfluxDB host")
@click.option(
    "--port", "-p", type=int, default=8086, help="InfluxDB port. Default: 8086"
)
@click.option("--user", "-U", type=str, help="InfluxDB username")
@click.option(
    "--pwd",
    "-P",
    type=str,
    help="InfluxDB username password",
)
@click.option(
    "--db",
    type=str,
    required=True,
    help="Target database. Default: home_assistant",
    default="home_assistant",
)
@click.option("--count", is_flag=True, help="Count measurement totals")
@click.option(
    "--action",
    "-a",
    type=click.Choice(["dryrun", "remove"]),
    default="dryrun",
    help="Remove or dryrun. Default: dryrun",
)
@click.option(
    "--sleep",
    "-s",
    type=int,
    default=120,
    help="Wait time (seconds) between remove queries. Default: 120",
)
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True, file_okay=True, readable=True),
    help="File containing measurements to remove",
)
def main(
    host: str = None,
    port: int = None,
    user: str = None,
    pwd: str = None,
    db: str = None,
    count: bool = False,
    action: str = None,
    sleep: int = None,
    file: str = None,
):
    """Mass remove measurements from influxdb

    Args: \n
        host (str): InfluxDB Host \n
        user (str): Username \n
        pwd (str): Password \n
        db (str): Database Name \n
        count (bool): Count measurement totals \n
        action (str): Action remove or dryrun \n
        sleep (int): \n
        file (path): File containing measurements to remove \n
    """

    if action == "remove" and file is None:
        print("Action remove requires an input file")
        exit()
    influx_scan(host, port, user, pwd, db, count, action, sleep, file).main()


if __name__ == "__main__":
    main()
