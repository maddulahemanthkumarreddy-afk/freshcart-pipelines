
import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def export_metrics(df: pd.DataFrame, **kwargs) -> str:
    """
    Export model evaluation metrics to a CSV file.
    """
    output_file = 'iris_metrics.csv'

    df.to_csv(output_file, index=False)

    print(f"Metrics saved to: {os.path.abspath(output_file)}")
    print(f"Total metric rows: {len(df)}")

    return os.path.abspath(output_file)


@test
def test_output(output, *args) -> None:
    assert output is not None, 'Output file path is missing'
    assert os.path.exists(output), 'Metrics CSV was not created'

    saved_df = pd.read_csv(output)

    assert len(saved_df) >= 5, 'Expected at least 5 metric rows'
    assert 'class' in saved_df.columns, 'Class column missing'
    assert 'precision' in saved_df.columns, 'Precision column missing'
    assert 'recall' in saved_df.columns, 'Recall column missing'
    assert 'f1-score' in saved_df.columns, 'F1-score column missing'
