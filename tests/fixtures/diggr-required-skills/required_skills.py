"""Synthetic test policy for disposable diggr-main profile only.

This repository fixture does not represent the private production role policy.
"""


def required_skill_ids(profile):
    if profile == 'diggr-main':
        return ('diggr-coding-loop-onboarding',)
    return ()
