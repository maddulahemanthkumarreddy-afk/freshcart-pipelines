import os
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def save_results(df: pd.DataFrame, **kwargs) -> str:
    """
    Save Iris model predictions to a CSV file.
    """

    output_file = 'iris_predictions.csv'

    df.to_csv(output_file, index=False)

    print(f"Predictions saved to {os.path.abspath(output_file)}")
    print(f"Total predictions: {len(df)}")

    return os.path.abspath(output_file)


@test
def test_output(output, *args) -> None:
    """
    Verify that the prediction CSV file was created.
    """

    assert output is not None, 'Output file path is missing'
    assert os.path.exists(output), 'iris_predictions.csv was not created'

    saved_df = pd.read_csv(output)

    assert len(saved_df) == 30, 'Expected 30 predictions'
    assert 'actual' in saved_df.columns
    assert 'predicted' in saved_df.columns
    assert 'test_accuracy' in saved_df.columns