from journalabbrev.db import *

from .common import *


def handle_upgrade_events(db: JournalDB) -> None:
	@db.on("upgrade_started")
	def upgrade_started(old_version: Version, new_version: Version, num_journals: int) -> None:
		info(f"upgrading DB from v{old_version} to v{new_version} ({num_journals:,} journals)...")

	@db.on("upgrade_progress")
	def upgrade_finished(num_journals_processed: int, num_journals_updated: int) -> None:
		pass

	@db.on("upgrade_finished")
	def upgrade_finished(new_version: Version, num_journals: int) -> None:
		info(f"finished upgrading DB to v{new_version} ({num_journals:,} journals).")
