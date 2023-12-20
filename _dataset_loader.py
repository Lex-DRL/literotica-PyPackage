#!/usr/bin/env python
# encoding: utf-8
"""
Wrapper module responsible for loading the dataset (and, if necessary, DOWNloading it first).
"""

import typing as _t

from attrs import define, field, setters as attrs_setters

import json
from pathlib import Path
from shutil import rmtree

from git import Repo, PathLike, NoSuchPathError, InvalidGitRepositoryError, RemoteProgress
import multivolumefile
from py7zr import SevenZipFile
from tqdm import tqdm

from ._data_objects import Category, ShortStoryMeta, Story


class _SimpleGitProgress(RemoteProgress):
	def __init__(self):
		super().__init__()
		self.pbar = tqdm()

	def update(self, op_code, cur_count, max_count=None, message=''):
		self.pbar.total = max_count
		self.pbar.n = cur_count
		self.pbar.refresh()


_default_dataset_repo_subdir = 'dataset_repo'

_categories_file = 'categories.json'
_story_ids_by_keyword_file = 'keywords_top_overall.json'
_story_metas_file = 'story_list.json'
_story_ids_by_category_file = 'story_list_by_category.json'

_json_encoding = 'utf-8'


def field_readonly(default, **kwargs):
	return field(default=default, on_setattr=attrs_setters.frozen, **kwargs)


@define
class DataSetLoader:
	"""
	A low-level class responsible for loading the dataset. Normally, you shouldn't use it directly,
	letting `DataSetDB` do the communication.

	The only exception is `dump_stories_to_category_json()` method.
	You might use it to manually fix broken stories within dataset.
	"""

	# We do a bunch of explicit filed declarations - to keep the entire class un-frozen,
	# while having an internally-mutable cache fields:

	root_dir: _t.Optional[PathLike] = field_readonly(None)
	repo_subdir: _t.Optional[PathLike] = field_readonly(None)

	archive_file: str = field_readonly('LitEroticaV2JSON.7z')
	unpack_subdir: str = field_readonly('LitEroticaV2JSON')

	repo_url: str = field_readonly('https://github.com/Lex-DRL/LitErotica-v2-JSON.git')

	__root_dir_path_cached: Path = None
	__unpacked_dir_path_cached: Path = None

	@property
	def root_package_dir_path(self) -> Path:
		"""Path to the package's dir."""
		cached_path = self.__root_dir_path_cached
		if cached_path is None:
			root_dir = self.root_dir
			if root_dir is None:
				root_dir = Path(__file__).parent
			elif not isinstance(root_dir, Path):
				root_dir = Path(root_dir)
			self.__root_dir_path_cached = cached_path = root_dir.absolute()
		isinstance(cached_path, Path)
		return cached_path

	def _repo_subdir_path(self) -> Path:
		root_dir = self.root_package_dir_path
		repo_subdir = self.repo_subdir
		if repo_subdir is None:
			repo_subdir = _default_dataset_repo_subdir
		return (root_dir / repo_subdir).absolute() if repo_subdir else root_dir

	@property
	def _unpacked_dir_path(self) -> Path:
		if self.__unpacked_dir_path_cached is None:
			repo_dir = self._repo_subdir_path()
			self.__unpacked_dir_path_cached = (repo_dir / self.unpack_subdir).absolute()
		return self.__unpacked_dir_path_cached

	def download_and_unpack(self) -> Repo:
		"""
		Unconditionally (force-) download the dataset from GitHub and extract it.
		You might need to manually remove the repo dir if it's already downloaded and yet broken.
		The method doesn't do that to stay away from accidental removal of dataset customizations you'd like to keep.
		"""
		repo_dir = self._repo_subdir_path()

		try:
			# noinspection PyTypeChecker
			repo = Repo(repo_dir)
			print(f"Pulling updates for:\n{repo_dir}")
			repo.remotes.origin.pull(progress=_SimpleGitProgress())
		except (NoSuchPathError, InvalidGitRepositoryError):
			print(f"Cloning <LitErotica dataset> repository...\n{self.repo_url}\n{repo_dir}")
			# noinspection PyTypeChecker
			repo = Repo.clone_from(self.repo_url, repo_dir, branch='main', progress=_SimpleGitProgress())

		unpacked_dir_path = self._unpacked_dir_path
		print(f"\nUnpacking dataset from archive to:\n{unpacked_dir_path}")
		if unpacked_dir_path.exists() and unpacked_dir_path.is_dir():
			print("Removing old dir...")
			rmtree(unpacked_dir_path)

		print("Unpacking (please wait)...")
		with multivolumefile.open(repo_dir / self.archive_file, mode='rb') as joined_archive_file:
			with SevenZipFile(joined_archive_file, mode='r') as archive:
				archive.extractall(path=unpacked_dir_path)
		print("Done!\n")
		return repo

	def dataset_dir(self) -> Path:
		"""The path to the folder containing JSON files. If necessary, the dataset will be auto-downloaded."""
		unpacked_dir_path = self._unpacked_dir_path
		if not(
			unpacked_dir_path.exists() and unpacked_dir_path.is_dir() and list(unpacked_dir_path.glob('*.json'))
		):
			print("Dataset isn't downloaded yet. Downloading it...")
			self.download_and_unpack()
		return unpacked_dir_path

	def _load_json_file(self, file_name: PathLike):
		file_path = (self.dataset_dir() / file_name).absolute()
		# noinspection PyTypeChecker
		with open(file_path, 'r', encoding=_json_encoding) as file_handle:
			return json.load(file_handle)

	def _load_story_ids_by_category(self) -> _t.Dict[str, _t.List[str]]:
		return self._load_json_file(_story_ids_by_category_file)

	def load_shortened_story_id_lists_by_keyword(self) -> _t.Dict[str, _t.List[str]]:
		return self._load_json_file(_story_ids_by_keyword_file)

	def _load_story_ids_by_keyword_for_category(self, category: Category) -> _t.Dict[str, _t.List[str]]:
		return self._load_json_file(category.json_keywords_filename)

	def _load_stories_for_category(self, category: Category):
		return self._load_json_file(category.json_stories_filename)

	def load_short_story_metas(self) -> _t.Dict[str, ShortStoryMeta]:
		raw_json_data: dict = self._load_json_file(_story_metas_file)
		return {
			x_nm: ShortStoryMeta(**x_dict)
			for x_nm, x_dict in raw_json_data.items()
		}

	def load_categories(self) -> _t.Dict[str, Category]:
		raw_json_data: dict = self._load_json_file(_categories_file)
		categories: _t.Dict[str, Category] = {
			x_nm: Category.deserialize_json_dict(**x_dict)
			for x_nm, x_dict in raw_json_data.items()
		}
		story_ids_by_category = self._load_story_ids_by_category()
		for cat_id, story_ids in story_ids_by_category.items():
			cat = categories[cat_id]
			cat.stories = set(story_ids)

		for cat in categories.values():
			cat.stories_by_keyword = {
				k: set(v) for k, v in self._load_story_ids_by_keyword_for_category(cat).items()
			}

		return categories

	def load_all_stories(self, categories: _t.Dict[str, Category]) -> _t.Dict[str, Story]:
		raw_story_dicts_by_id_by_cat: _t.Dict[str, _t.Dict[str, dict]] = {
			cat_id: self._load_stories_for_category(cat) for cat_id, cat in categories.items()
		}
		all_story_dicts_by_id: _t.Dict[str, dict] = dict()
		for cat_stories_dict in raw_story_dicts_by_id_by_cat.values():
			for story_id, story_data_dict in cat_stories_dict.items():
				if story_id not in all_story_dicts_by_id:
					all_story_dicts_by_id[story_id] = story_data_dict
					continue
				if all_story_dicts_by_id[story_id] != story_data_dict:
					raise ValueError(
						f"Same story appears twice with different data:\n"
						f"{story_id}\n{all_story_dicts_by_id[story_id]}\n{story_data_dict}"
					)

		# We've flattened the dict of dicts of dicts.
		# Now, all the stories are in the same pool... but they're still raw json dicts themselves.
		# Converting to the actual data objects:
		return {
			x_id: Story.deserialize_json_dict(**x_dict)
			for x_id, x_dict in all_story_dicts_by_id.items()
		}

	def load_all(self) -> _t.Tuple[_t.Dict[str, Category], _t.Dict[str, Story]]:
		categories = self.load_categories()
		stories = self.load_all_stories(categories)
		return categories, stories

	def dump_stories_to_category_json(self, category: Category, stories: _t.Dict[str, 'Story']):
		file_path = (self.dataset_dir() / category.json_stories_filename).absolute()
		stories_data_dict = {
			k: story.serialize_to_dict() for k, story in stories.items()
		}
		# noinspection PyTypeChecker
		with open(file_path, "w", encoding=_json_encoding) as json_file:
			json.dump(stories_data_dict, json_file)


if __name__ == '__main__':
	# It's troublesome to make this module work from the actual command line,
	# vut when used within IDE, it can be called to ensure everything is downloaded.
	try:
		print(DataSetLoader().dataset_dir())
	except Exception as e:
		print(e)
	input()
