from journalabbrev.db import *

from .common import *


def handle_upgrade_events(db: JournalDB):
	@db.on("upgrade_started")
	def upgrade_started(old_version, new_version, num_journals):
		info(f"upgrading DB from v{old_version} to v{new_version} ({num_journals:,} journals)...")

	@db.on("upgrade_progress")
	def upgrade_finished(num_journals_processed, num_journals_updated):
		pass

	@db.on("upgrade_finished")
	def upgrade_finished(new_version, num_journals):
		info(f"finished upgrading DB to v{new_version} ({num_journals:,} journals).")
