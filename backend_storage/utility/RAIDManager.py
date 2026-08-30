from pathlib import Path


class RAIDManager:

    def __init__(self, base_dir, replica_count=2):
        self.base_dir = Path(base_dir)
        self.replica_count = replica_count

        self.replica_dirs = [
            self.base_dir / f"replica_{i}"
            for i in range(replica_count)
        ]

        for directory in self.replica_dirs:
            directory.mkdir(parents=True, exist_ok=True)

    def write(self, relative_path, data):
        written = []

        for replica_dir in self.replica_dirs:
            output_path = replica_dir / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)

            output_path.write_bytes(data)
            written.append(output_path)

        return written

    def check_health(self):
        return {
            str(directory): directory.exists()
            for directory in self.replica_dirs
        }

    def recover(self, relative_path):
        source = None

        for replica_dir in self.replica_dirs:
            candidate = replica_dir / relative_path

            if candidate.exists():
                source = candidate
                break

        if source is None:
            raise FileNotFoundError(
                f"No healthy replica found for {relative_path}"
            )

        data = source.read_bytes()
        recovered = []

        for replica_dir in self.replica_dirs:
            target = replica_dir / relative_path

            if not target.exists():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                recovered.append(target)

        return recovered
