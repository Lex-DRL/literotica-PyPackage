# encoding: utf-8
"""
"""

import typing as _t

from attrs import define, field
from itertools import chain, islice
from os import getcwd
from os.path import isabs
from pathlib import Path

from ._data_objects import Category, Story
from ._dataset_loader import DataSetLoader as _DataSetLoader, PathLike as _PathLike

_default_out_file = 'combined.txt'


def _get_full_file_path(file_name: _t.Optional[_PathLike] = None, default_filename='file', file_print_name='File') -> Path:
	"""
	Fallback:
	- to current working directory if path isn't specified
	- to default filename if it's not provided at all.
	"""
	if not(file_name and isabs(file_name)):
		root_dir = Path(getcwd())
		print(f"Directory for {file_print_name} is:\n{root_dir}")
		if not file_name:
			file_name = default_filename
		file_path = root_dir / file_name
	else:
		file_path = Path(file_name)
	assert isinstance(file_path, Path)
	return file_path.absolute()


@define
class DataSetDB:
	"""
	The main class to perform custom filtering/sorting on the dataset.
	"""

	categories: _t.Dict[str, Category]
	stories: _t.Dict[str, Story]
	broken_stories: _t.Dict[str, Story] = field(factory=dict)

	@staticmethod
	def load(**dataset_loader_kwargs):
		"""
		Constructor method.
		Builds an instance with `categories` and `stories` field contents loaded from the underlying dataset.
		The dataset itself well be auto-downloaded if necessary.

		`broken_stories` field is intentionally not populated. Such stories should be manually extracted from the main pool
		at the very end, with explicit call to `filter_out_broken_stories()` method.
		"""
		categories, stories = _DataSetLoader(**dataset_loader_kwargs).load_all()
		return DataSetDB(categories=categories, stories=stories)

	def category_keywords(self, category: str):
		return self.categories[category].keywords

	@property
	def keyword_hits(self) -> _t.Dict[str, int]:
		"""
		Get ALL the keywords min the pool (including unpopular ones) and count how many times each keyword gets referenced.
		"""
		all_keywords: _t.Dict[str, int] = dict()
		for story in self.stories.values():
			for kw in story.keywords:
				all_keywords[kw] = all_keywords.get(kw, 0) + 1
		return dict(sorted(
			all_keywords.items(), key=lambda k_v: k_v[1], reverse=True
		))

	def __filtered(self, ok_f: _t.Callable[[Story], bool]) -> 'DataSetDB':
		"""
		Base method to build a filtered version of DB.
		The only thing that's changed is the `.stories` dict.
		For bug-prevention, other fields have a shallow (not deep) copy: the dict itself is a copy, it's members are references.
		"""
		stories = {
			k: v for k, v in self.stories.items()
			if ok_f(v)
		}
		return DataSetDB(dict(self.categories), stories, broken_stories=dict(self.broken_stories))

	def with_authors(self, *authors: str):
		"""A filtered version of the DB: only with stories from the given author(s)."""
		authors = set(authors)
		def ok_filter(story: Story):
			return story.author in authors
		return self.__filtered(ok_filter)

	def not_authors(self, *authors: str):
		"""A filtered version of the DB, which no longer contains any stories from the given author(s)."""
		authors = set(authors)
		def ok_filter(story: Story):
			return story.author not in authors
		return self.__filtered(ok_filter)

	def with_categories(self, *categories: str):
		"""A filtered version of the DB: only with stories from the given categories."""
		categories = set(categories)
		def ok_filter(story: Story):
			return story.category in categories
		return self.__filtered(ok_filter)

	def not_categories(self, *categories: str):
		"""A filtered version of the DB, which no longer contains any stories the given categories."""
		categories = set(categories)
		def ok_filter(story: Story):
			return story.category not in categories
		return self.__filtered(ok_filter)

	def with_keywords_from_categories(self, *categories: str):
		"""
		A filtered version of the DB: only with stories which are marked with keywords associated with the given categories.
		The association relationship is taken from keys of the `.categories[*].keywords` dict.
		"""
		categories = set(categories)
		keywords_from_categories = set(chain(
			*(self.categories[cat_id].keywords for cat_id in categories)
		))
		def ok_filter(story: Story):
			return any(kw in keywords_from_categories for kw in story.keywords)
		return self.__filtered(ok_filter)

	def not_keywords_from_categories(self, *categories: str):
		"""
		A filtered version of the DB, which no longer contains any stories
		which are marked with keywords associated with the given categories.
		The association relationship is taken from keys of the `.categories[*].keywords` dict.
		"""
		categories = set(categories)
		keywords_from_categories = set(chain(
			*(self.categories[cat_id].keywords for cat_id in categories)
		))
		def ok_filter(story: Story):
			return not any(kw in keywords_from_categories for kw in story.keywords)
		return self.__filtered(ok_filter)

	def with_keywords(self, *keywords: str):
		"""A filtered version of the DB: only with stories marked with the given keywords."""
		def ok_filter(story: Story):
			story_keywords = story.keywords
			return all(kw in story_keywords for kw in keywords)
		return self.__filtered(ok_filter)

	def not_keywords(self, *keywords: str):
		"""A filtered version of the DB, which no longer contains any stories marked with the given keywords."""
		def ok_filter(story: Story):
			story_keywords = story.keywords
			return not any(kw in story_keywords for kw in keywords)
		return self.__filtered(ok_filter)

	@staticmethod
	def __keyword_group_hits_sorting_key_func(keyword_synonym_groups: _t.Tuple[_t.Iterable[str], ...]):
		"""Factory. Generates a function to produce sorting hit-weight for the given keyword groups."""
		group_name_by_keyword: _t.Dict[str, str] = dict()  # kw -> kw_group
		for kw_group_iter in keyword_synonym_groups:
			if isinstance(kw_group_iter, str):
				kw_group_iter = [kw_group_iter]
			kw_group_iter = list(kw_group_iter)
			group_name = kw_group_iter[0]
			for kw in kw_group_iter:
				group_name_by_keyword[kw] = group_name

		def n_group_hits_f(story: Story):
			group_hits: _t.Set[str] = set()
			for kw in story.keywords:
				if kw in group_name_by_keyword:
					gr_name = group_name_by_keyword[kw]
					group_hits.add(gr_name)
			return len(group_hits)

		return n_group_hits_f

	@staticmethod
	def __keyword_group_weighted_hits_sorting_key_func(
		wights_by_keyword_synonym_groups: _t.Dict[_t.Iterable[str], _t.Union[int, float]]
	):
		"""Factory. Similarly to `__keyword_group_hits_sorting_key_func()`, detects not just hits but WEIGHTED hits."""
		group_name_by_keyword: _t.Dict[str, str] = dict()  # kw -> kw_group
		group_weights: _t.Dict[str, int] = dict()  # kw_group -> weight
		for kw_group_iter, weight in wights_by_keyword_synonym_groups.items():
			if isinstance(kw_group_iter, str):
				kw_group_iter = [kw_group_iter]
			kw_group_iter = tuple(kw_group_iter)
			group_name = kw_group_iter[0]
			group_weights[group_name] = weight
			for kw in kw_group_iter:
				group_name_by_keyword[kw] = group_name

		def keywords_weight_f(story: Story):
			group_hits: _t.Set[str] = set()
			for kw in story.keywords:
				if kw in group_name_by_keyword:
					gr_name = group_name_by_keyword[kw]
					group_hits.add(gr_name)
			return sum(group_weights[gr_name] for gr_name in group_hits)

		return keywords_weight_f

	def keyword_hits_min(self, n: int, *keyword_synonym_groups: _t.Iterable[str]):
		"""
		A filtered version of the DB, with... a bit fancy, but very powerful filtering method.

		What you provide is not just keywords, but GROUPS of synonymous keywords (each group as an iterable of strings).
		Each story in the set is counted on how many times it hits any of the groups, but hits only once per each.
		This way, you won't get multiple hits in stories which have multiple variations of the same thing.

		Then, the only stories kept in a filtered DB are the ones with AT LEAST the given number of hits.
		"""
		n_group_hits_f = self.__keyword_group_hits_sorting_key_func(keyword_synonym_groups)
		def ok_filter(story: Story):
			return n_group_hits_f(story) >= n
		return self.__filtered(ok_filter)

	def keyword_hits_max(self, n: int, *keyword_synonym_groups: _t.Iterable[str]):
		"""
		A filtered version of the DB, with... a bit fancy, but very powerful filtering method.

		What you provide is not just keywords, but GROUPS of synonymous keywords (each group as an iterable of strings).
		Each story in the set is counted on how many times it hits any of the groups, but hits only once per each.
		This way, you won't get multiple hits in stories which have multiple variations of the same thing.

		Then, the only stories kept in a filtered DB are the ones with AT MOST the given number of hits.
		"""
		n_group_hits_f = self.__keyword_group_hits_sorting_key_func(keyword_synonym_groups)
		def ok_filter(story: Story):
			return n_group_hits_f(story) <= n
		return self.__filtered(ok_filter)

	def keyword_hits_range(self, min: int, max: int, *keyword_synonym_groups: _t.Iterable[str]):
		"""
		A convenience method, combining `.keyword_hits_min().keyword_hits_max()` into one call
		(which should also be slightly faster).
		"""
		n_group_hits_f = self.__keyword_group_hits_sorting_key_func(keyword_synonym_groups)
		def ok_filter(story: Story):
			return min <= n_group_hits_f(story) <= max
		return self.__filtered(ok_filter)

	def keyword_weights_min(
		self, weight: _t.Union[float, int], wights_by_keyword_synonym_groups: _t.Dict[_t.Iterable[str], _t.Union[int, float]]
	):
		"""
		Similar to `keyword_hits_min()`, but here it expects not just groups of keywords, but also weights for each group
		(in a form of dict where key is the group and value is the weight).

		A total weight calculated for every story in the DB (hitting any group gives it's weight only once),
		and then only the story with AT LEAST the given weight are kept in the filtered DB.
		"""
		keywords_weight_f = self.__keyword_group_weighted_hits_sorting_key_func(wights_by_keyword_synonym_groups)
		def ok_filter(story: Story):
			return keywords_weight_f(story) >= weight
		return self.__filtered(ok_filter)

	def keyword_weights_max(
		self, weight: _t.Union[float, int], wights_by_keyword_synonym_groups: _t.Dict[_t.Iterable[str], _t.Union[int, float]]
	):
		"""
		Similar to `keyword_weights_min()`, but filtering out any stories with the weight ABOVE the given threshold.
		Useful when you want to split the DB into subsets of high- and low-relevance, treat them individually
		and combine afterwards.
		"""
		keywords_weight_f = self.__keyword_group_weighted_hits_sorting_key_func(wights_by_keyword_synonym_groups)
		def ok_filter(story: Story):
			return keywords_weight_f(story) <= weight
		return self.__filtered(ok_filter)

	def keyword_weights_range(
		self, min: _t.Union[float, int], max: _t.Union[float, int],
		wights_by_keyword_synonym_groups: _t.Dict[_t.Iterable[str], _t.Union[int, float]]
	):
		"""
		A convenience method, combining `.keyword_weights_min().keyword_weights_max()` into one call
		(which should also be slightly faster).
		"""
		keywords_weight_f = self.__keyword_group_weighted_hits_sorting_key_func(wights_by_keyword_synonym_groups)
		def ok_filter(story: Story):
			return min <= keywords_weight_f(story) <= max
		return self.__filtered(ok_filter)

	def rating_min(self, rating: _t.Union[float, int]):
		"""A filtered version of the DB, with the stories of AT LEAST the given rating."""
		def ok_filter(story: Story):
			return story.rating >= rating
		return self.__filtered(ok_filter)

	def rating_max(self, rating: _t.Union[float, int]):
		"""A filtered version of the DB, with the stories of AT MOST the given rating."""
		def ok_filter(story: Story):
			return story.rating <= rating
		return self.__filtered(ok_filter)

	def rating_range(self, min: _t.Union[float, int], max: _t.Union[float, int]):
		"""A filtered version of the DB, with the stories which have their rating in the specified range."""
		def ok_filter(story: Story):
			return min <= story.rating <= max
		return self.__filtered(ok_filter)

	def pages_min(self, n: int):
		"""A filtered version of the DB, with the stories of AT LEAST the given number of pages."""
		def ok_filter(story: Story):
			return story.page_count >= n
		return self.__filtered(ok_filter)

	def pages_max(self, n: int):
		"""A filtered version of the DB, with the stories of AT MOST the given number of pages."""
		def ok_filter(story: Story):
			return story.page_count <= n
		return self.__filtered(ok_filter)

	def pages_range(self, min: int, max: int):
		"""A filtered version of the DB, with the stories which have their page count in the specified range."""
		def ok_filter(story: Story):
			return min <= story.page_count <= max
		return self.__filtered(ok_filter)

	def words_min(self, n: int):
		"""A filtered version of the DB, with the stories of AT LEAST the given number of words."""
		def ok_filter(story: Story):
			return story.word_count >= n
		return self.__filtered(ok_filter)

	def words_max(self, n: int):
		"""A filtered version of the DB, with the stories of AT MOST the given number of words."""
		def ok_filter(story: Story):
			return story.word_count <= n
		return self.__filtered(ok_filter)

	def words_range(self, min: int, max: int):
		"""A filtered version of the DB, with the stories which have their word count in the specified range."""
		def ok_filter(story: Story):
			return min <= story.word_count <= max
		return self.__filtered(ok_filter)

	def __sorted(self, key: _t.Callable[[Story], _t.Any], reverse=False) -> 'DataSetDB':
		"""
		Base method to build a sorted version of DB. Relies on the order-preserving built-in dicts in the recent python versions.
		The only thing that's changed is the `.stories` dict.
		For bug-prevention, other fields have a shallow (not deep) copy: the dict itself is a copy, it's members are references.
		"""
		sorted_stories = sorted(self.stories.values(), key=key, reverse=reverse)
		stories = {story.id: story for story in sorted_stories}
		return DataSetDB(dict(self.categories), stories, broken_stories=dict(self.broken_stories))

	def sorted_by_max_keyword_hits(self, *keyword_synonym_groups: _t.Iterable[str], descending=True):
		"""
		Similar to `keyword_hits_min()`, but instead of filtering sorts the stories dict according to
		the number of keyword-group-hits per story.
		"""
		n_group_hits_f = self.__keyword_group_hits_sorting_key_func(keyword_synonym_groups)
		return self.__sorted(n_group_hits_f, reverse=descending)

	def sorted_by_max_keywords_weight(
		self, wights_by_keyword_synonym_groups: _t.Dict[_t.Iterable[str], _t.Union[int, float]], descending=True
	):
		"""
		Similar to `keyword_weights_min()`, but instead of filtering sorts the stories dict according to
		the overall weight per story.
		"""
		keywords_weight_f = self.__keyword_group_weighted_hits_sorting_key_func(wights_by_keyword_synonym_groups)
		return self.__sorted(keywords_weight_f, reverse=descending)

	def sorted_by_rating(self, step: _t.Union[int, float] = None, descending=True):
		"""
		A version of the DB, with stories sorted by their rating.
		If optional `step` argument provided, treats rating within the given step as equal.
		"""
		def key_no_round(story: Story):
			return story.rating

		round_multiplier = 1 if step is None or step <= 0 else 1.0 / step

		def key_rounded(story: Story):
			return int(story.rating * round_multiplier)

		return self.__sorted(key_no_round if step is None or step <= 0 else key_rounded, reverse=descending)

	def dumped_as_output_text(self, max_stories=-1) -> _t.List[str]:
		"""
		Export the entire story pool as a joined output.
		Titles and keywords are formatted according to the template from:
		https://tapwavezodiac.github.io/novelaiUKB/The-Rabbit-Hole.html

		The recommended 4-dashes separator is used to further emphasise the story beginning.
		"""
		stories_dict = self.stories
		if max_stories is not None and max_stories < 0:
			max_stories = max(0, len(stories_dict) + max_stories)

		all_story_texts = (
			story.dumped_as_output_text() for story in stories_dict.values()
		)
		all_story_texts = iter(all_story_texts)
		try:
			first_story = next(all_story_texts)
		except StopIteration:
			return []

		all_story_texts = chain(
			[first_story],
			(f"\n\n----\n\n{txt}" for txt in all_story_texts)
		)
		return list(islice(all_story_texts, max_stories))

	def dump_to_output_txt_file(self, file_name: _t.Optional[_PathLike] = None, max_stories=-1):
		file_path = _get_full_file_path(file_name, _default_out_file, 'output txt file')
		with open(file_path, 'w', encoding='utf-8', newline='\n') as file_handle:
			file_handle.writelines(self.dumped_as_output_text(max_stories))

	def filter_out_broken_stories(self):
		"""
		Unfortunately, there's a garbage within dataset.
		Fortunately, it for some reason has a COVID-19 warning at the end, so it's easy to detect.
		To fix it permanently, one should update the dataset itself.

		Use `DataSetLoader().dump_stories_to_category_json()` - but treat the DB CAREFULLY).
		You should NOT perform any filtering or sorting prior to updating the source dataset.
		"""
		stories = dict(self.stories)
		buggy = dict(self.broken_stories)
		for story_id, story in list(stories.items()):
			if story.text.rstrip().endswith("COVID-19 RESOURCES"):
				stories.pop(story_id)
				buggy[story_id] = story
		return DataSetDB(dict(self.categories), stories, broken_stories=buggy)

	@staticmethod
	def load_single_story_text_from_file(file_name: _PathLike, **dataset_loader_kwargs) -> str:
		"""
		Utility function used to manually fix (replace text for) broken stories in dataset.
		Look into `filter_out_broken_stories()` method description.
		"""
		file_path = _get_full_file_path(file_name, 'fix.txt', 'txt file with story fix')
		with open(file_path, 'r', encoding='utf-8', newline='\n') as file:
			lines = (l.rstrip() for l in file.readlines())
			return '\n'.join(lines)
