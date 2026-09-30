from spark_jobs.build_gold_dataset import GOLD_COLUMNS
from taxi_features.features import FEATURE_COLUMNS

NON_FEATURE_COLUMNS = {"unique_key", "trip_start_ts", "split", "label"}


def test_gold_schema_matches_feature_columns():
    """GOLD_COLUMNS must list the same features, in the same order, as common/."""
    features_in_schema = [name for name, _ in GOLD_COLUMNS if name not in NON_FEATURE_COLUMNS]
    assert features_in_schema == FEATURE_COLUMNS


def test_gold_columns_have_no_duplicates():
    names = [name for name, _ in GOLD_COLUMNS]
    assert len(names) == len(set(names))
