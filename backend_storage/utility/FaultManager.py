import logging


class FaultManager:

    def __init__(self, raid_manager):
        self.raid_manager = raid_manager

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s"
        )

    def check_system(self):
        health = self.raid_manager.check_health()

        failed = [
            replica
            for replica, healthy in health.items()
            if not healthy
        ]

        if failed:
            logging.warning(
                "Failed storage replicas: %s",
                failed
            )
        else:
            logging.info("All storage replicas healthy.")

        return health

    def recover_file(self, relative_path):
        try:
            recovered = self.raid_manager.recover(relative_path)

            logging.info(
                "Recovery completed: %s",
                recovered
            )

            return recovered

        except FileNotFoundError as error:
            logging.error("Recovery failed: %s", error)
            return []
