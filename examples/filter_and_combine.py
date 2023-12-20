#!/usr/bin/env python
# encoding: utf-8
"""
An example how to build your own dataset slice, filtered in steps.

Yeah, I'm aware that here I share my kinks. But I don't care: I'm a REALLY open guy about this stuff IRL.

If you're new to python stuff, don't forget to do the following command in the repo root folder:
python -m pip install -U -r requirements.txt
"""

import typing as _t

from itertools import chain

from literotica import DataSetDB, Category, Story, DataSetLoader as _DataSetLoader, PathLike as _PathLike


def main():
	db = DataSetDB.load()
	db = db.with_categories('Gay Male')  # Initially, filter by desired category

	# Second, perform a rough "white list" filter.
	# It's a really broad one, using ALL the keywords from ALL the categories as a "green pass":
	db = db.with_keywords_from_categories(
		'Group Sex', 'Incest/Taboo', 'Interracial Love', 'NonConsent/Reluctance', 'Sci-Fi & Fantasy'
	)

	# Keywords discovery. It's better done by checking kept keywords from a single category ot once.
	# category = 'Sci-Fi & Fantasy'
	# db = db.with_keywords_from_categories(category)
	# print(db.keyword_hits)  # if you're familiar with IDE, debugging it interactively is a MUCH better alternative

	# Third, blacklist by undesired keywords. You need to explore those yourself, by performing a few initial filters
	# and looking into `.keyword_hits` property.
	db = db.not_keywords(
		# 'bisexual', 'bisexual male', 'bi', 'bisexual husband', 'bi-sexual', 'bisex', 'bi male',

		# Clearly, a woman is involved:
		'two men one woman', 'mmmf', 'mmf', 'mfm',
		'milf', 'mother', 'mom', 'sexy_mama_09', 'aunt', 'mother son',
		'loving wives', 'older woman', 'wife', 'wife sharing', 'cheating wife', 'loving wife', 'wife watching',
		'femdom', 'girlfriend', 'girl', 'younger woman', 'sister', 'schoolgirl', 'sissy',
		'asian woman', 'lesbian',
		'strapon', 'strap on', 'strap-on', 'bikini', 'high heels',
		'cunnilingus', 'pussy', 'eating pussy', 'pussy eating', 'pussy licking', 'shaved pussy', 'female masturbation',
		'breasts', 'tits', 'big tits',
		'pregnant',

		# Old men:
		'old man', 'older man', 'grandpa', 'grandson', 'grandfather',
		'daddy', 'father', 'dad', 'father son', 'father sex', 'mature', 'hairy',

		# Physically dirty:
		'pissing', 'piss enema',

		# Tied-up roleplay.
		# Usually if it's there, that's where BDSM stuff begins and ends, with activity itself being very vanilla:
		'bondage',
	)

	db_pre_filters = db  # remember it if we want to re-include stories by specific keywords we want REALLY bad

	# Now, the main part. After we've discovered our desired keywords, let's combine them into groups of synonyms
	# and assign weights to each, defining how significant that specific group is to us:
	desired_kw_groups = {
		(  # a medium-size group activity
			'threesome', 'first threesome', 'first time threesome', 'threesomes', '3some', 'threeway', 'three way',
			'foursome', '4some',
			'swingers', 'swinging', 'sharing', 'swinger', 'swing', 'cuckold', 'cuck',
		): 1,
		(  # a group activity with many guys
			'orgy', 'gangbang', 'gang bang',
			'group sex', 'group',
			'bukkake',
		): 1.5,
		(  # same, but might mean a place, so smaller weight... though, 'orgy' + 'party' would way even more - but I like that
			'party', 'the party', 'sex party', 'bachelor party', 'sex club',
		): 0.5,
		(  # who doesn't like twinks?
			'younger man', 'young', 'teen', '18-year-old', '18 year old', '19-year-old', 'college', 'student',
			'high school',
			'gay twink',
		): 0.5,
		(  # Yeah. My thing and I'm not ashamed of it. Stick your puritanism deep into your... decide yourself, where.
			'twincest', 'gay twins', 'gay twin brothers', 'twin brothers',
			'brother', 'brothers', 'brother sex',
			'brother-in-law', 'brother in law',
		): 5,  # OMFG, I DREAM OF IT IRL 😍
		(  # Less desired, but still cool:
			'cousin', 'cousins', 'uncle', 'nephew',
		): 2.5,
		(  # A ore general keywords, so they might mean a boring `daddy/son`, which wasn't excluded by blacklist. So, lower weight:
			'family', 'incest', 'incest romance', 'gay incest', 'anal incest', 'taboo',
			'stepdad', 'father-in-law', 'stepfather', 'son in law', 'stepson',
		): 1.5,
		(
			'size difference',
		): 3,  # Thanks to 'daddies' excluded by blacklist, this should keep only same-age guys.
		(  # O-o-oh yeah. Really love that, in any position.
			'forced', 'force', 'forced sex', 'forced orgasm', 'power', 'coercion', 'coerced', 'cnc', 'restrained',
			'reluctance', 'reluctant', 'nonconsent', 'non consent', 'non-consensual', 'nonconsensual', 'non-consent',
			'unwilling', 'dubious consent',
			'kidnapped', 'taken',
			'revenge', 'used',
		): 2.5,
		(  # Vanilla lovers use these words too often in a completely wrong meaning, lower weight:
			'rough', 'rough sex', 'hardcore',
		): 1.5,
		(
			'pain',
		): 1,
		(  # These words are "poisoned" by leather fetish community and it's not my thing, so lower weight, too:
			'domination', 'dominant', 'dominance', 'punishment', 'sex slave',
		): 0.25,
		(  # Even if already intersects with other 'hardcore' keywords, no problem boosting the story even higher:
			'double penetration', 'dp', 'fisting',
		): 1.5,
		(
			'triple penetration',
		): 3,
		(
			'gagging', 'gag', 'gagged', 'choking',
		): 2.5,
		(  # Might mean just a "vanilla" oral, with just ATTEMPTS to try it - so lower
			'deepthroat', 'deep throat',
		): 1,
		(
			'big cock', 'big dick', 'huge cock', 'huge', 'bbc', 'big black cock', 'black dick', 'black cock', 'cock',
			'size', 'hung',
		): 0.75,
		(
			'interracial', 'interacial', 'interracial threesome', 'interracial sex', 'interracial romance',
			'black on white',
			'black', 'black man', 'black men', 'black male', 'black guy', 'black lover', 'african', 'ebony',
			'asian', 'asian man', 'japanese', 'japanese man', 'chinese', 'korean',
		): 1,
		(
			'latino', 'latina', 'hispanic', 'jamaica',
		): 1.25,
		(
			'military', 'soldier',
		): 0.75,
		(
			'outdoors', 'outdoor sex', 'outside', 'public', 'public sex', 'pool', 'beach',
		): 0.5,
		(
			'sleep', 'sleeping', 'asleep',
		): 0.75,
		(  # I initially started this dataset selection to train a model for Mass Effect setting, so explicitly prefer these:
			'science fiction', 'scifi', 'sci fi', 'sci-fi',
		): 3,
		(  # ... and something like DmC and/or more cliche supernatural won't hurt, too:
			'paranormal', 'supernatural', 'vampire', 'demons', 'demon', 'angel', 'angels', 'gods',
		): 1,
	}

	# Study not yet treated keywords to see if you'd like to use any of them:
	# already_used_keywords = set(chain(*desired_kw_groups.keys()))
	# keyword_hits = db.keyword_hits
	# keywords_intersect = {
	# 	kw: hit for kw, hit in keyword_hits.items()
	# 	if kw in db.categories[category].keywords and kw not in already_used_keywords
	# }

	# With our keywords and their weights selection, let's filter out any story which has no such keywords
	# OR their combined weight is less then 1:
	db = db.keyword_hits_min(
		# This function expects only groups and the hit count.
		# For more reasonable but also aggressive filtering, you might want at least two keyword-groups match,
		# but I'm OK with just one:
		1, *desired_kw_groups.keys()
	).keyword_weights_min(
		1, desired_kw_groups  # This time, `1` means the weight the keyword-group selection must be the whole dict, too
	)

	# Finally, let's force-restore any stories with too hot tags, which might've been filtered out
	# (not my case, but it might be for you if you had 2+ hits requirement):
	hottest_tags = list(chain(*(
		kws for kws, weight in desired_kw_groups.items()
		if weight > 2.95
	)))
	# No fancy magic here. Stories is just a dict, and any filtering creates
	# just a new instance of the DB with another dict in it.
	# So it's completely enough to update the desired dict with the items from another DB instance...
	# or any filters performed manually on the dict itself:
	db.stories.update(
		db_pre_filters.with_keywords(*hottest_tags).stories
	)

	# After all the filtering is done, let's also exclude stories with garbage content:
	db = db.filter_out_broken_stories()

	# ... and sort the stories dict (works only on recent versions of Python) to see the most desired stories first:
	# db = db.sorted_by_max_keyword_hits(*desired_kw_groups.keys())
	db = db.sorted_by_max_keywords_weight(desired_kw_groups)

	# When everything is done, all that's left is to save the work into
	db.dump_to_output_txt_file()  # saved to 'combined.txt'
	# db.dump_to_output_txt_file(file_path='MySuperDuperMegaBest_TrainingSet.txt')  # full path can also be provided

	print('YAAAY! Done! The combined-stories file is saved.')


if __name__ == '__main__':
	try:
		main()
	except Exception as e:
		print(e)
	input()
