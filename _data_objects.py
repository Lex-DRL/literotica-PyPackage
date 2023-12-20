# encoding: utf-8
"""
"""

import typing as _t

from attrs import define, field, validators as v


@define
class Category:
	category: str = field(
		eq=str.lower,
		validator=[v.instance_of(str), v.min_len(1)]
	)
	description: str = field()
	url: str = field()
	stories: _t.Set[str] = field(factory=set)
	stories_by_keyword: _t.Dict[str, _t.Set[str]] = field(factory=dict)
	page_links: _t.List[str] = field(factory=list)

	@property
	def keywords(self):
		return list(self.stories_by_keyword.keys())

	@property
	def _json_basename(self) -> str:
		return self.category.replace("/", " & ")

	@property
	def json_keywords_filename(self) -> str:
		return f"{self._json_basename}_keywords_top.json"

	@property
	def json_stories_filename(self) -> str:
		return f"{self._json_basename}_stories.json"

	@staticmethod
	def deserialize_json_dict(**kwargs):
		if 'stories' in kwargs:
			kwargs['stories'] = set(kwargs['stories'])
		if 'stories_by_keyword' in kwargs:
			kwargs['stories_by_keyword'] = {
				k: set(v) for k, v in kwargs['stories_by_keyword'].items()
			}
		if 'page_links' in kwargs:
			kwargs['page_links'] = list(sorted(kwargs['page_links']))
		return Category(**kwargs)

	def serialize_to_dict(self):
		# DRL's note: I know there should be some built-in way in `attrs` to do serialization,
		# but I'm not as familiar with the module.
		return dict(
			category=self.category,
			description=self.description,
			url=self.url,
			stories=list(sorted(self.stories)),
			stories_by_keyword={
				k: list(sorted(v)) for k, v in self.stories_by_keyword.items()
			},
			page_links=self.page_links,
		)


@define
class ShortStoryMeta:
	id: str = field(
		eq=str.lower,
		validator=[v.instance_of(str), v.min_len(1)]
	)
	title: str = field()
	url: str = field()
	category: str = field()
	rating: float = field()


@define
class Story:
	id: str = field(
		eq=str.lower,
		validator=[v.instance_of(str), v.min_len(1)]
	)
	title: str = field()
	url: str = field()
	category: str = field()
	rating: float = field()
	description: str = field()
	keywords: _t.Set[str] = field(factory=set)
	text: str = field(default='')
	page_count: int = field(
		default=1,
		validator=[v.instance_of(int), v.gt(0)]
	)
	word_count: int = field(
		default=1,
		validator=[v.instance_of(int), v.gt(0)]
	)
	author: str = field(default='')
	date_approved: str = field(default='')

	@staticmethod
	def deserialize_json_dict(**kwargs):
		if 'keywords' in kwargs:
			kwargs['keywords'] = set(kwargs['keywords'])
		return Story(**kwargs)

	def serialize_to_dict(self):
		return dict(
			id=self.id,
			title=self.title,
			url=self.url,
			category=self.category,
			rating=self.rating,
			description=self.description,
			keywords=list(sorted(self.keywords)),
			text=self.text,
			page_count=self.page_count,
			word_count=self.word_count,
			author=self.author,
			date_approved=self.date_approved
		)

	def dumped_as_output_text(self) -> str:
		# Template from: https://tapwavezodiac.github.io/novelaiUKB/The-Rabbit-Hole.html
		tags = ', '.join(self.keywords)
		header = f"[ Title: {self.title.strip()};\nTags: {tags} ]"
		return f"{header}\n***\n{self.text.strip()}"
