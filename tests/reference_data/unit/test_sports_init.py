"""Smoke test — verify sports sub-package imports."""

from instruments_service.reference_data.adapters.sports import (
    ApiFootballAdapter,
    BaseSportsReferenceAdapter,
    CompetitionPhase,
    OddsApiAdapter,
    classify_competition_phase,
    create_sports_reference_adapter,
)


def test_factory_is_callable() -> None:
    assert callable(create_sports_reference_adapter)


def test_classify_is_callable() -> None:
    assert callable(classify_competition_phase)


def test_base_adapter_is_abstract() -> None:
    import inspect

    assert inspect.isabstract(BaseSportsReferenceAdapter)


def test_adapter_classes_exist() -> None:
    assert ApiFootballAdapter is not None
    assert OddsApiAdapter is not None


def test_competition_phase_enum() -> None:
    assert CompetitionPhase.NORMAL_LEAGUE == "NORMAL_LEAGUE"
    assert CompetitionPhase.TOURNAMENT == "TOURNAMENT"


def test_top_level_re_export() -> None:
    """create_sports_reference_adapter is available from URDI top-level."""
    from instruments_service.reference_data import (
        create_sports_reference_adapter as top_level,
    )

    assert top_level is create_sports_reference_adapter
