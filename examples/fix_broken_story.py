#!/usr/bin/env python
# encoding: utf-8
"""
Unfortunately, source dataset scraped by jorfl contains garbage entries.
They're broken... in a way. All the metadata is correct, but the text itself
is a mess of html/js instead of the actual story.

This example shows how to update the underlying dataset if you'd like to use the broken stories, too.
Keep in mind that it's auto-downloaded from a different repo and ALL your modifications aren't applied to it
(mostly because dataset in the repo is archived since it's more that 1Gb unpacked).

If you decide to fix the stories, it's done manually, one-by-one.
"""

import typing as _t

from itertools import chain

from literotica import DataSetDB, Story, DataSetLoader


def main():
	db = DataSetDB.load()

	category = 'Gay Male'

	db = db.with_categories(category)  # The dataset files are stored separately, one for each category.

	# You SHOULD NOT (!!!) perform any filtering/sorting on the dataset, other than selecting a single category.

	# And DO NOT remember the DB with broken stories extracted from the main pool.
	# Get only broken stories themselves:
	broken_stories = db.filter_out_broken_stories().broken_stories

	# At this stage you might want to see the names and URLs of such stories. In my case, the following ones were.
	# At each fix iteration, I manually:
	# - opened a story URL in browser and copied the text
	# - pasted it it into `fix.txt` file, UTF-8. Line endings don't matter, they're converted to '\n' anyway.
	# - removed double-newlines by search&replace feature in Notepad++
	# - saved it
	# - ONLY THEN uncommented THE RIGHT line below.
	# Be cautious here ^ since you can accidentally import text to the wrong story.

	story_id = 'a-late-night-stranger-at-the-door'
	# story_id = 'renovation-1'
	# story_id = 'awakened-guardian'
	# story_id = 'first-18'
	# story_id = 'my-first-time-236'
	# story_id = 'too-many-questions-1'
	# story_id = 'must-have-cock'
	# story_id = 'club-night-pregame'
	# story_id = 'unsexy-1'
	# story_id = 'discovery-24'

	# Load the text from a file within ghe same dir as :
	text = DataSetDB.load_single_story_text_from_file('fix.txt')

	# Update the story in question with the new text:
	fixed_story: Story = broken_stories[story_id]
	fixed_story.text = text

	# Just to make sure, print the text - to display newlines as... well, newlines:
	print(
		f"\n{story_id}\n"
		f"{fixed_story.id} <- should be the same ^ (if not, the source dataset itself is broken)\n"
		f"{fixed_story.title}\n\n"
		f">>>\n"
		f"{text}"
	)

	input()  # in IDE, you should put a breakpoint here. But to be safe, here's a hard-coded user confirmation, too

	# When you've checked that everything is good, update the source json file:
	DataSetLoader().dump_stories_to_category_json(db.categories[category], db.stories)
	print("fixed!")


if __name__ == '__main__':
	try:
		main()
	except Exception as e:
		print(e)
	input()
