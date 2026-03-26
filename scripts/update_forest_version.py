from importlib.metadata import metadata

from database.models import ForestVersion
from libs.utils.forest_utils import get_forest_git_hash


def main():
    forest_version = ForestVersion.singleton()
    md = metadata("beiwe-forest")
    forest_version.package_version = md.get("version")
    forest_version.git_commit = get_forest_git_hash()
    forest_version.save()
