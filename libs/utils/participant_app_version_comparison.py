from operator import ge as gte, gt, le as lte, lt

from constants.message_strings import (ERR_ANDROID_REFERENCE_VERSION_CODE_DIGITS,
    ERR_ANDROID_TARGET_VERSION_DIGITS, ERR_IOS_REFERENCE_VERSION_NAME_FORMAT,
    ERR_IOS_REFERENCE_VERSION_NULL, ERR_IOS_TARGET_VERSION_FORMAT,
    ERR_IOS_VERSION_COMPONENTS_DIGITS, ERR_TARGET_VERSION_CANNOT_BE_MISSING,
    ERR_TARGET_VERSION_MUST_BE_STRING, ERR_UNKNOWN_OS_TYPE, ERR_UNKNOWN_TARGET_VERSION)
from constants.user_constants import ANDROID_API, IOS_API


"""
This code needs to be perfect because it is used in survey resend logic. 

These all work, but their language was backwards from what we ended up needing in the app version
check in the resend logic.  We are going to keep them because there might be a use for them, and
tests already exist and was SUPER hard to get right.

These functions can all throw VersionError and you just have to catch that sorry.

TODO: get rid of this entirely and swap to the app reports its ability and we set a flag based on it
    on the participant.
"""

class VersionError(ValueError): pass

#
## is target version X than reference version
#

def is_this_version_gt_participants(
    os_type: str, target_version: str, participant_version_code: str, participant_version_name: str
) -> bool:
    return _is_this_version_op_than_participants(
        gt, os_type, target_version, participant_version_code, participant_version_name
    )


def is_this_version_lt_participants(
    os_type: str, target_version: str, participant_version_code: str, participant_version_name: str
) -> bool:
    return _is_this_version_op_than_participants(
        lt, os_type, target_version, participant_version_code, participant_version_name
    )


def is_this_version_gte_participants(
    os_type: str, target_version: str, participant_version_code: str, participant_version_name: str
) -> bool:
    return _is_this_version_op_than_participants(
        gte, os_type, target_version, participant_version_code, participant_version_name
    )


def is_this_version_lte_participants(
    os_type: str, target_version: str, participant_version_code: str, participant_version_name: str
) -> bool:
    return _is_this_version_op_than_participants(
        lte, os_type, target_version, participant_version_code, participant_version_name
    )

#
## is participant's version OPERATOR than target version
#

def is_participants_version_gt_target(
    os_type: str, participant_version_code: str, participant_version_name: str, target_version: str
) -> bool:
    return _is_participants_version_op_than_target(
        gt, os_type, participant_version_code, participant_version_name, target_version
    )


def is_participants_version_lt_target(
    os_type: str, participant_version_code: str, participant_version_name: str, target_version: str
) -> bool:
    return _is_participants_version_op_than_target(
        lt, os_type, participant_version_code, participant_version_name, target_version
    )


def is_participants_version_gte_target(
    os_type: str, participant_version_code: str, participant_version_name: str, target_version: str
) -> bool:
    return _is_participants_version_op_than_target(
        gte, os_type, participant_version_code, participant_version_name, target_version
    )


def is_participants_version_lte_target(
    os_type: str, participant_version_code: str, participant_version_name: str, target_version: str
) -> bool:
    return _is_participants_version_op_than_target(
        lte, os_type, participant_version_code, participant_version_name, target_version
    )

#
## OS wranglers
#


def _is_participants_version_op_than_target(
    op: callable, os_type: str, participant_version_code: str, participant_version_name: str,
    target_version: str
) -> bool:
    _validate_target_and_os(os_type, target_version)
    if os_type == IOS_API:
        return _ios_is_this_version_op_than(op, participant_version_name, target_version)
    
    if os_type == ANDROID_API:
        return _android_is_version_op_than(op, participant_version_code, target_version)
    
    raise VersionError(ERR_UNKNOWN_OS_TYPE(os_type))


def _is_this_version_op_than_participants(
    op: callable, os_type: str, target_version: str, participant_version_code: str,
    participant_version_name: str
) -> bool:
    _validate_target_and_os(os_type, target_version)
    if os_type == IOS_API:
        return _ios_is_this_version_op_than(op, target_version, participant_version_name)
    
    if os_type == ANDROID_API:
        return _android_is_version_op_than(op, target_version, participant_version_code)
    
    raise VersionError(ERR_UNKNOWN_OS_TYPE(os_type))


def _validate_target_and_os(os_type: str, target_version: str) -> None:
    if not isinstance(target_version, str):
        raise VersionError(ERR_TARGET_VERSION_MUST_BE_STRING(type(target_version)))
    
    if target_version == "missing":
        raise VersionError(ERR_TARGET_VERSION_CANNOT_BE_MISSING)
    
    if os_type not in (IOS_API, ANDROID_API):
        raise VersionError(ERR_UNKNOWN_OS_TYPE(os_type))


def _ios_is_this_version_op_than(
    op: callable, target_version: str, participant_version_name: str
) -> bool:
    # version_code for ios looks like 2.x, 2.x.y, or 2.x.yz, or is None
    # version_name for ios looks like 2024.21, or is None, or a commit-hash-like string.
    # version_name CANNOT be 2024.21.1 (it is not a semantic version)
    
    if participant_version_name is None:
        raise VersionError(ERR_IOS_REFERENCE_VERSION_NULL)
    
    if not isinstance(target_version, str):
        raise VersionError(ERR_UNKNOWN_TARGET_VERSION(target_version))
    
    if target_version.count(".") != 1:
        raise VersionError(ERR_IOS_TARGET_VERSION_FORMAT(target_version))
    if participant_version_name.count(".") != 1:
        raise VersionError(ERR_IOS_REFERENCE_VERSION_NAME_FORMAT(participant_version_name))
    
    target_year, target_build = target_version.split(".")
    reference_year, reference_build = participant_version_name.split(".")
    
    if (not target_year.isdigit() or not reference_year.isdigit()
        or not target_build.isdigit() or not reference_build.isdigit()):
        raise VersionError(ERR_IOS_VERSION_COMPONENTS_DIGITS(target_version, participant_version_name))
    
    # to intify and beyond
    target_year, target_build = int(target_year), int(target_build)
    reference_year, reference_build = int(reference_year), int(reference_build)
    
    # FINALLY some logic...
    if target_year == reference_year:
        return op(target_build, reference_build)
    return op(target_year, reference_year)

def _android_is_version_op_than(
    op: callable, target_version: str, participant_version_code: str
) -> bool:
    # android is easy, we just compare the version code, which must be digits-only.
    if participant_version_code is None:
        raise VersionError(ERR_ANDROID_participant_VERSION_CODE_NULL)
    
    if not target_version.isdigit():
        raise VersionError(ERR_ANDROID_TARGET_VERSION_DIGITS(target_version))
    if not participant_version_code.isdigit():
        raise VersionError(ERR_ANDROID_REFERENCE_VERSION_CODE_DIGITS(participant_version_code))
    
    return op(int(target_version), int(participant_version_code))
